"""
Building Routes — HiMap v3.0

/buildings/{dataset}    — query buildings by bbox at zoom >= 14
/buildings/{dataset}/stats — feature counts and occupancy breakdown

Zoom gate contract:
    Buildings are ONLY queryable at zoom >= MIN_BUILDING_ZOOM (14).
    Below this zoom, the endpoint returns HTTP 400 with a clear message.
    This is enforced at the API boundary — it cannot be bypassed.

    Semantic mapping:
        zoom 0–13:  continental → city scale — buildings not rendered
        zoom 14–15: neighborhood scale — building footprints first appear
        zoom 16+:   street scale — full building detail

Query strategy:
    Reads from the persistent views BuildingViewGenerator builds — no
    CREATE VIEW happens per-request anymore. Two genuinely different
    sources are involved, selected by `config.h3_enriched`:

    Pre-H3 (h3_enriched=False):
        Reads {dataset}_buildings_view — pass-through over the RAW
        OpenBuildingMap parquet. Filters:
            1. Bbox filter using the bbox STRUCT columns (cheap, no
               geometry parse)
            2. Quadkey prefix filter as secondary guard
        Geometry returned is a Point approximated from the bbox center.

    Post-H3 (h3_enriched=True, set once partition_data.py has run for
    this dataset and BuildingViewGenerator.build() has been re-run —
    see BuildingRegistry.py / buildingViewGenerator.py):
        Reads {dataset}_buildings_partitioned — pass-through over the
        ENRICHED, H3-partitioned lake output. This is the view that
        actually has h3_9/area_m2/height_m/etc — the raw source view
        never did, which was a bug in an earlier version of this file.
        Filter: WHERE h3_9 IN (<cells covering the viewport>). Geometry
        returned is the true polygon, parsed from the enriched WKB.
"""


import logging
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

try:
    import h3
    _H3_V4 = hasattr(h3, "polygon_to_cells")
except ImportError:
    h3 = None
    _H3_V4 = False

from shapely import wkb as shapely_wkb
from shapely.geometry import mapping

from ..Ingestion.BuildingRegistry import building_registry, MIN_BUILDING_ZOOM
from ..Generator.Buildingviewgenerator import BuildingViewGenerator
from ..Services.DuckLakeService import ducklake_service
from ..Utils.Zoom import zoom_levels
from ..Models.responses import FeaturesResponse, Feature


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/buildings", tags=["buildings"])
building_view_generator = BuildingViewGenerator(ducklake_service)

H3_QUERY_RESOLUTION = 9  # must match PARTITION_RES used at enrichment time


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_building_dataset(key: str) -> None:
    if key not in building_registry:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown building dataset: '{key}'. "
                f"Registered: {building_registry.list()}"
            ),
        )


def _resolve_view(dataset: str, config) -> str:
    """
    Which persistent view a query should read from — the raw source view
    (pre-H3) or the partitioned view (post-H3). Does NOT create the view;
    BuildingViewGenerator.build_all() must have already run for it to
    exist. Raises a clear 500 rather than DuckDB's binder error if it's
    missing, since "view doesn't exist" almost always means build() was
    never called after h3_enriched flipped to True.
    """
    view = (
        config.partitioned_view_name(dataset)
        if config.h3_enriched
        else config.view_name(dataset)
    )
    active = building_view_generator.list_views()
    if view not in active:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Expected view '{view}' does not exist. "
                f"Call BuildingViewGenerator.build('{dataset}') "
                f"(re-run after partition_data.py or after flipping "
                f"h3_enriched)."
            ),
        )
    return view


def _enforce_zoom_gate(zoom: int, config) -> None:
    """
    Enforce the minimum zoom level for building queries.

    Below MIN_BUILDING_ZOOM, buildings are not visible on the map and
    querying them is wasteful. Returns a clear error with the semantic
    context so the client can adapt its behavior.
    """
    if zoom < config.zoom_gate:
        semantic = zoom_levels.semantic_level(zoom)
        gate_semantic = zoom_levels.semantic_level(config.zoom_gate)
        raise HTTPException(
            status_code=400,
            detail={
                "error":           "zoom_below_building_threshold",
                "zoom":            zoom,
                "zoom_semantic":   semantic,
                "min_zoom":        config.zoom_gate,
                "min_zoom_semantic": gate_semantic,
                "message": (
                    f"Buildings are not visible at zoom {zoom} ({semantic}). "
                    f"Zoom in to at least zoom {config.zoom_gate} ({gate_semantic}) "
                    f"to query building footprints."
                ),
            },
        )


def _parse_height(height_str: Optional[str]) -> Optional[float]:
    """
    Parse the height field into meters (pre-H3 path only — the
    partitioned view already has height_m/height_floors computed).

    Encodings:
        'HBET:1-2'  → average of range × 3m per floor ≈ 4.5m
        '12.5'      → 12.5 meters
        None        → None
    """
    if height_str is None:
        return None
    if height_str.startswith("HBET:"):
        try:
            parts = height_str.replace("HBET:", "").split("-")
            lo, hi = float(parts[0]), float(parts[1])
            return ((lo + hi) / 2) * 3.0   # floors × approx 3m
        except Exception:
            return None
    try:
        return float(height_str)
    except (ValueError, TypeError):
        return None


def _bbox_to_h3_cells(sw_lng: float, sw_lat: float, ne_lng: float, ne_lat: float,
                       res: int = H3_QUERY_RESOLUTION) -> List[str]:
    """
    Return the H3 cells (at `res`) covering a viewport bbox.

    Requires h3-py v4 (`polygon_to_cells` / `LatLngPoly`). Falls back to
    raising a clear error if an older h3 package is installed, rather
    than silently returning an empty/wrong cell set.
    """
    if h3 is None or not _H3_V4:
        raise RuntimeError(
            "H3 query path requires h3-py>=4 (polygon_to_cells). "
            "Install with: pip install 'h3>=4'"
        )

    ring = [
        (sw_lat, sw_lng),
        (sw_lat, ne_lng),
        (ne_lat, ne_lng),
        (ne_lat, sw_lng),
    ]
    poly = h3.LatLngPoly(ring)
    return list(h3.polygon_to_cells(poly, res))


def _rows_to_building_features_bbox(rows: List[Dict]) -> List[Feature]:
    """
    Pre-H3 path: convert rows to Point features approximated from bbox
    center. No WKB parsing.
    """
    features = []
    for row in rows:
        bbox = row.get("bbox") or {}

        xmin = bbox.get("xmin", 0)
        xmax = bbox.get("xmax", 0)
        ymin = bbox.get("ymin", 0)
        ymax = bbox.get("ymax", 0)
        center_lng = (xmin + xmax) / 2
        center_lat = (ymin + ymax) / 2

        width_m  = abs(xmax - xmin) * 111_320 * math.cos(math.radians(center_lat))
        height_m_bbox = abs(ymax - ymin) * 110_574
        area_m2  = width_m * height_m_bbox

        height_raw = row.get("height")
        height_m_val = _parse_height(height_raw)

        geometry = {"type": "Point", "coordinates": [center_lng, center_lat]}

        properties = {
            "id":           row.get("id"),
            "occupancy":    row.get("occupancy"),
            "height_raw":   height_raw,
            "height_m":     height_m_val,
            "floorspace":   row.get("floorspace"),
            "relation_id":  row.get("relation_id"),
            "quadkey":      row.get("quadkey"),
            "last_update":  str(row.get("last_update")) if row.get("last_update") else None,
            "source":       row.get("source"),
            "bbox_area_m2": round(area_m2, 1),
        }

        features.append(Feature(type="Feature", properties=properties, geometry=geometry))

    return features


def _rows_to_building_features_h3(rows: List[Dict]) -> List[Feature]:
    """
    Post-H3 path: true polygon geometry from the enriched WKB column,
    plus the enrichment fields (centroid, area_m2, height_m/floors, h3_9).
    """
    features = []
    for row in rows:
        geom_wkb = row.get("geometry")
        try:
            geometry = mapping(shapely_wkb.loads(bytes(geom_wkb))) if geom_wkb else None
        except Exception:
            geometry = None

        properties = {
            "id":            row.get("feature_id"),
            "occupancy":     row.get("occupancy"),
            "height_raw":    row.get("height_raw"),
            "height_m":      row.get("height_m"),
            "height_floors": row.get("height_floors"),
            "floorspace":    row.get("floorspace"),
            "relation_id":   row.get("relation_id"),
            "quadkey":       row.get("quadkey"),
            "source":        row.get("source_provider"),
            "area_m2":       row.get("area_m2"),
            "perimeter_m":   row.get("perimeter_m"),
            "h3_9":          row.get("h3_9"),
        }

        features.append(Feature(type="Feature", properties=properties, geometry=geometry))

    return features


def _build_bbox_query(config, view: str, sw_lng: float, sw_lat: float, ne_lng: float, ne_lat: float) -> str:
    """
    Pre-H3 query against {dataset}_buildings_view: bbox STRUCT
    intersection + quadkey region guard. No geometry operations at all.
    """
    quadkey_filter = config.quadkey_filter_sql()

    return f"""
        SELECT
            id, floorspace, occupancy, relation_id, quadkey,
            last_update, height, bbox, source
        FROM {view}
        WHERE
            bbox.xmin <= ?
            AND bbox.xmax >= ?
            AND bbox.ymin <= ?
            AND bbox.ymax >= ?
            AND {quadkey_filter}
        ORDER BY
            (bbox.xmax - bbox.xmin) * (bbox.ymax - bbox.ymin) DESC
        LIMIT ?
    """


def _build_h3_query(view: str, cells: List[str]) -> str:
    """
    Post-H3 query against {dataset}_buildings_partitioned: filter by
    h3_9. This view has no `bbox` STRUCT column, so there's no cheap
    secondary refinement the way the pre-H3 path has one — at resolution
    9 (~0.1 km² per cell) that's an acceptable trade for now, but flag it
    if false positives at cell edges turn out to matter for your case
    (the fix would be adding a bbox_geom-based ST_Intersects filter using
    the *_buildings_enriched view instead of *_buildings_partitioned).
    """
    cell_list = ", ".join(f"'{c}'" for c in cells)

    return f"""
        SELECT
            feature_id, floorspace, occupancy, relation_id,
            height_raw, height_m, height_floors,
            geometry, quadkey, source_provider, area_m2, perimeter_m, h3_9
        FROM {view}
        WHERE
            h3_9 IN ({cell_list})
        ORDER BY area_m2 DESC
        LIMIT ?
    """


# ---------------------------------------------------------------------------
# GET /buildings/{dataset}
# ---------------------------------------------------------------------------

@router.get(
    "/{dataset}",
    response_model=FeaturesResponse,
    summary="Query building footprints",
    responses={
        400: {"description": "Zoom below threshold, unknown dataset, or invalid bbox"},
        500: {"description": "Query execution error, or expected view missing"},
    },
)
async def query_buildings(
    dataset:  str,
    zoom:     int   = Query(..., ge=0,   le=20,    description="Current map zoom level. Must be >= 14."),
    sw_lng:   float = Query(..., ge=-180, le=180,  description="Southwest longitude"),
    sw_lat:   float = Query(..., ge=-90,  le=90,   description="Southwest latitude"),
    ne_lng:   float = Query(..., ge=-180, le=180,  description="Northeast longitude"),
    ne_lat:   float = Query(..., ge=-90,  le=90,   description="Northeast latitude"),
    limit:    int   = Query(500,  ge=1,  le=2000,  description="Max buildings returned"),
    occupancy: Optional[str] = Query(None, description="Filter by occupancy code: UNK, RES, COM, IND, CIV"),
):
    """
    Return building footprints within a bounding box.

    **Zoom gate**: zoom must be >= 14.

    **Query path**: reads `{dataset}_buildings_partitioned` once the
    dataset's `h3_enriched` flag is set; otherwise reads
    `{dataset}_buildings_view` (the pre-H3 bbox/quadkey path). Both are
    persistent views built by BuildingViewGenerator — nothing is created
    per-request.

    **Feature ordering**: largest buildings first, so prominent
    landmarks surface before residential buildings when the limit hits.
    """
    _validate_building_dataset(dataset)
    config = building_registry.get(dataset)

    _enforce_zoom_gate(zoom, config)

    if sw_lng >= ne_lng or sw_lat >= ne_lat:
        raise HTTPException(status_code=400, detail="Invalid bounding box")

    width  = ne_lng - sw_lng
    height = ne_lat - sw_lat
    if width > 0.5 or height > 0.5:
        logger.warning(
            f"Large building query bbox ({width:.3f}° × {height:.3f}°) "
            f"at zoom {zoom} — consider reducing viewport"
        )

    view = _resolve_view(dataset, config)

    try:
        conn = ducklake_service._get_connection()
        try:
            if config.h3_enriched:
                cells = _bbox_to_h3_cells(sw_lng, sw_lat, ne_lng, ne_lat)
                sql = _build_h3_query(view, cells)
                params = (limit,)
                if occupancy:
                    sql = sql.replace(
                        "ORDER BY area_m2 DESC",
                        "AND occupancy = ? ORDER BY area_m2 DESC",
                    )
                    params = (occupancy, limit)
                filter_mode = "h3"
            else:
                sql = _build_bbox_query(config, view, sw_lng, sw_lat, ne_lng, ne_lat)
                params = (ne_lng, sw_lng, ne_lat, sw_lat, limit)
                if occupancy:
                    sql = sql.replace(
                        f"AND {config.quadkey_filter_sql()}",
                        f"AND {config.quadkey_filter_sql()} AND occupancy = ?",
                    )
                    params = (ne_lng, sw_lng, ne_lat, sw_lat, occupancy, limit)
                filter_mode = "bbox_struct"

            result = conn.execute(sql, params)
            description = result.description or []
            columns = [d[0] for d in description]
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        finally:
            conn.close()

        features = (
            _rows_to_building_features_h3(rows)
            if config.h3_enriched
            else _rows_to_building_features_bbox(rows)
        )

        return FeaturesResponse(
            dataset=dataset,
            count=len(features),
            features=features,
            query={
                "mode":      "buildings_bbox",
                "view":      view,
                "zoom":      zoom,
                "semantic":  zoom_levels.semantic_level(zoom),
                "sw_lng":    sw_lng,
                "sw_lat":    sw_lat,
                "ne_lng":    ne_lng,
                "ne_lat":    ne_lat,
                "occupancy": occupancy,
                "limit":     limit,
                "filter":    filter_mode,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Building query failed — dataset={dataset}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# GET /buildings/{dataset}/stats
# ---------------------------------------------------------------------------

@router.get(
    "/{dataset}/stats",
    summary="Building stats for a viewport",
    responses={
        400: {"description": "Zoom below threshold or unknown dataset"},
        500: {"description": "Expected view missing"},
    },
)
async def building_stats(
    dataset: str,
    zoom:    int   = Query(..., ge=0, le=20, description="Must be >= 14"),
    sw_lng:  float = Query(..., ge=-180, le=180),
    sw_lat:  float = Query(..., ge=-90,  le=90),
    ne_lng:  float = Query(..., ge=-180, le=180),
    ne_lat:  float = Query(..., ge=-90,  le=90),
):
    """
    Return occupancy breakdown and total count for a viewport.
    """
    _validate_building_dataset(dataset)
    config = building_registry.get(dataset)
    _enforce_zoom_gate(zoom, config)

    if sw_lng >= ne_lng or sw_lat >= ne_lat:
        raise HTTPException(status_code=400, detail="Invalid bounding box")

    view = _resolve_view(dataset, config)

    try:
        conn = ducklake_service._get_connection()
        try:
            if config.h3_enriched:
                cells = _bbox_to_h3_cells(sw_lng, sw_lat, ne_lng, ne_lat)
                cell_list = ", ".join(f"'{c}'" for c in cells)
                stats_sql = f"""
                    SELECT
                        COUNT(*)                                    AS total,
                        COUNT(*) FILTER (WHERE occupancy = 'RES')  AS residential,
                        COUNT(*) FILTER (WHERE occupancy = 'COM')  AS commercial,
                        COUNT(*) FILTER (WHERE occupancy = 'IND')  AS industrial,
                        COUNT(*) FILTER (WHERE occupancy = 'CIV')  AS civic,
                        COUNT(*) FILTER (WHERE occupancy = 'UNK')  AS unknown,
                        COUNT(*) FILTER (WHERE height_m IS NOT NULL) AS with_height
                    FROM {view}
                    WHERE h3_9 IN ({cell_list})
                """
                row = conn.execute(stats_sql).fetchone()
            else:
                quadkey_filter = config.quadkey_filter_sql()
                stats_sql = f"""
                    SELECT
                        COUNT(*)                                    AS total,
                        COUNT(*) FILTER (WHERE occupancy = 'RES')  AS residential,
                        COUNT(*) FILTER (WHERE occupancy = 'COM')  AS commercial,
                        COUNT(*) FILTER (WHERE occupancy = 'IND')  AS industrial,
                        COUNT(*) FILTER (WHERE occupancy = 'CIV')  AS civic,
                        COUNT(*) FILTER (WHERE occupancy = 'UNK')  AS unknown,
                        COUNT(*) FILTER (WHERE height IS NOT NULL) AS with_height
                    FROM {view}
                    WHERE
                        bbox.xmin <= ?
                        AND bbox.xmax >= ?
                        AND bbox.ymin <= ?
                        AND bbox.ymax >= ?
                        AND {quadkey_filter}
                """
                row = conn.execute(stats_sql, (ne_lng, sw_lng, ne_lat, sw_lat)).fetchone()

        finally:
            conn.close()

        total, res, com, ind, civ, unk, with_height = row

        return {
            "dataset":  dataset,
            "view":     view,
            "zoom":     zoom,
            "semantic": zoom_levels.semantic_level(zoom),
            "bbox":     {"sw_lng": sw_lng, "sw_lat": sw_lat, "ne_lng": ne_lng, "ne_lat": ne_lat},
            "counts": {
                "total":       total,
                "residential": res,
                "commercial":  com,
                "industrial":  ind,
                "civic":       civ,
                "unknown":     unk,
                "with_height": with_height,
            },
            "note": (
                "Geometry is bbox-filtered (pre-H3)."
                if not config.h3_enriched
                else "Geometry is h3_9-filtered (post-H3)."
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Building stats failed — dataset={dataset}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
