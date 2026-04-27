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
 
Query strategy (pre-H3):
    1. Bbox filter using the bbox STRUCT columns (cheap, no geometry parse)
       bbox.xmin <= ne_lng AND bbox.xmax >= sw_lng AND
       bbox.ymin <= ne_lat AND bbox.ymax >= sw_lat
 
    2. Quadkey prefix filter as secondary guard (dataset-level region filter)
       WHERE quadkey LIKE 'XXXXXXXXXX%'
 
    After H3 enrichment:
        WHERE h3_9 = ?   (replaces both filters above)
 
    The bbox STRUCT filter is preferred over ST_Intersects(geometry, envelope)
    because it avoids geometry parsing — the struct fields are plain doubles.
    For building footprints (polygons), the bbox filter has very low false-
    positive rate and the performance difference is significant at scale.
"""


import logging
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..Ingestion.BuildingRegistry import building_registry, MIN_BUILDING_ZOOM
from ..Services.DuckLakeService import ducklake_service
from ..Utils.Zoom import zoom_levels
from ..Models.responses import FeaturesResponse, Feature


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/buildings", tags=["buildings"])


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
    Parse the height field into meters.

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


def _rows_to_building_features(rows: List[Dict]) -> List[Feature]:
    """
    Convert DuckDB result rows to GeoJSON Feature objects for buildings.

    Uses bbox STRUCT for centroid approximation — avoids WKB parsing
    on every row. The centroid is the center of the bbox, accurate
    enough for point-of-interest display and H3 indexing.
    """
    features = []
    for row in rows:
        bbox = row.get("bbox") or {}

        # Centroid from bbox center (no WKB parse needed)
        xmin = bbox.get("xmin", 0)
        xmax = bbox.get("xmax", 0)
        ymin = bbox.get("ymin", 0)
        ymax = bbox.get("ymax", 0)
        center_lng = (xmin + xmax) / 2
        center_lat = (ymin + ymax) / 2

        # Approximate footprint area in m²
        width_m  = abs(xmax - xmin) * 111_320 * math.cos(math.radians(center_lat))
        height_m = abs(ymax - ymin) * 110_574
        area_m2  = width_m * height_m

        height_raw = row.get("height")
        height_m_val = _parse_height(height_raw)

        geometry = {
            "type":        "Point",
            "coordinates": [center_lng, center_lat],
        }

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

        features.append(Feature(
            type="Feature",
            properties=properties,
            geometry=geometry,
        ))

    return features


def _build_bbox_query(config, sw_lng: float, sw_lat: float, ne_lng: float, ne_lat: float) -> str:
    """
    Build the SQL query for bbox-filtered building retrieval.

    Uses bbox STRUCT columns for fast filtering — no geometry parsing.
    Applies dataset quadkey prefix filter as a secondary guard.

    Query plan:
        1. bbox STRUCT intersection (cheap double comparisons)
        2. Quadkey prefix filter (cheap string prefix scan)
        3. No geometry operations at all (pre-H3 path)

    After H3 enrichment this becomes:
        WHERE h3_9 = ?
    """
    view = "buildings_source"   # read_parquet inline — see caller
    quadkey_filter = config.quadkey_filter_sql()

    return f"""
        SELECT
            id,
            floorspace,
            occupancy,
            relation_id,
            quadkey,
            last_update,
            height,
            bbox,
            source
        FROM {view}
        WHERE
            -- Bbox STRUCT intersection (no geometry parse)
            bbox.xmin <= ?
            AND bbox.xmax >= ?
            AND bbox.ymin <= ?
            AND bbox.ymax >= ?
            -- Dataset region guard
            AND {quadkey_filter}
        ORDER BY
            -- Larger buildings first (approximated by bbox area)
            (bbox.xmax - bbox.xmin) * (bbox.ymax - bbox.ymin) DESC
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
        500: {"description": "Query execution error"},
    },
)
async def query_buildings(
    dataset:  str,
    zoom:     int   = Query(..., ge=0,   le=20,    description="Current map zoom level. Must be >= 14."),
    sw_lng:   float = Query(..., ge=-180, le=180,  description="Southwest longitude"),
    sw_lat:   float = Query(..., ge=-90,  le=90,   description="Southwest latitude"),
    ne_lng:   float = Query(..., ge=-180, le=180,  description="Northeast longitude"),
    ne_lat:   float = Query(..., ge=-90,  le=90,   description="Northeast latitude"),
    limit:    int   = Query(500,  ge=1,  le=2000,  description="Max buildings returned (lower default — polygons are heavier than points)"),
    occupancy: Optional[str] = Query(None, description="Filter by occupancy code: UNK, RES, COM, IND, CIV"),
):
    """
    Return building footprints within a bounding box.

    **Zoom gate**: zoom must be >= 14. Below this level buildings are not
    visible and querying them is rejected. This is by design — individual
    building footprints are meaningless at city or country scale.

    **Pre-H3 filtering**: Uses bbox STRUCT columns for spatial filtering.
    This avoids geometry parsing entirely. After H3 enrichment this will
    switch to WHERE h3_9 = ? for even faster lookups.

    **Feature ordering**: Larger buildings (by bbox area) are returned first.
    This ensures prominent landmarks (hospitals, malls, universities) appear
    before residential buildings when the limit is hit.

    **Geometry note**: Returns Point geometry at bbox center. Full polygon
    geometry will be available after the enrichment pipeline runs.
    """
    _validate_building_dataset(dataset)
    config = building_registry.get(dataset)

    # Zoom gate — non-negotiable
    _enforce_zoom_gate(zoom, config)

    if sw_lng >= ne_lng or sw_lat >= ne_lat:
        raise HTTPException(status_code=400, detail="Invalid bounding box")

    # Warn if bbox is large at building zoom — this can be slow
    width  = ne_lng - sw_lng
    height = ne_lat - sw_lat
    if width > 0.5 or height > 0.5:
        logger.warning(
            f"Large building query bbox ({width:.3f}° × {height:.3f}°) "
            f"at zoom {zoom} — consider reducing viewport"
        )

    try:
        glob = config.parquet_glob()
        conn = ducklake_service._get_connection()

        try:
            # Create inline view over Parquet glob
            conn.execute(f"""
                CREATE OR REPLACE TEMP VIEW buildings_source AS
                SELECT * FROM read_parquet('{glob}', hive_partitioning=false)
            """)

            sql = _build_bbox_query(config, sw_lng, sw_lat, ne_lng, ne_lat)

            params = (ne_lng, sw_lng, ne_lat, sw_lat, limit)

            if occupancy:
                # Inject occupancy filter — parameterized
                sql = sql.replace(
                    "AND {quadkey_filter}",
                    f"AND {config.quadkey_filter_sql()} AND occupancy = ?"
                )
                params = (ne_lng, sw_lng, ne_lat, sw_lat, occupancy, limit)

            result = conn.execute(sql, params)
            description = result.description or []
            columns = [d[0] for d in description]
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        finally:
            conn.close()

        features = _rows_to_building_features(rows)

        return FeaturesResponse(
            dataset=dataset,
            count=len(features),
            features=features,
            query={
                "mode":      "buildings_bbox",
                "zoom":      zoom,
                "semantic":  zoom_levels.semantic_level(zoom),
                "sw_lng":    sw_lng,
                "sw_lat":    sw_lat,
                "ne_lng":    ne_lng,
                "ne_lat":    ne_lat,
                "occupancy": occupancy,
                "limit":     limit,
                "filter":    "bbox_struct",   # will become 'h3' after enrichment
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

    Cheaper than the full query — returns aggregate stats only.
    Useful for client-side density visualization before fetching footprints.
    """
    _validate_building_dataset(dataset)
    config = building_registry.get(dataset)
    _enforce_zoom_gate(zoom, config)

    if sw_lng >= ne_lng or sw_lat >= ne_lat:
        raise HTTPException(status_code=400, detail="Invalid bounding box")

    try:
        glob = config.parquet_glob()
        quadkey_filter = config.quadkey_filter_sql()
        conn = ducklake_service._get_connection()

        try:
            conn.execute(f"""
                CREATE OR REPLACE TEMP VIEW buildings_source AS
                SELECT * FROM read_parquet('{glob}', hive_partitioning=false)
            """)

            stats_sql = f"""
                SELECT
                    COUNT(*)                                    AS total,
                    COUNT(*) FILTER (WHERE occupancy = 'RES')  AS residential,
                    COUNT(*) FILTER (WHERE occupancy = 'COM')  AS commercial,
                    COUNT(*) FILTER (WHERE occupancy = 'IND')  AS industrial,
                    COUNT(*) FILTER (WHERE occupancy = 'CIV')  AS civic,
                    COUNT(*) FILTER (WHERE occupancy = 'UNK')  AS unknown,
                    COUNT(*) FILTER (WHERE height IS NOT NULL) AS with_height
                FROM buildings_source
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
            "note": "Geometry is bbox-filtered (pre-H3). False positives possible at partition edges.",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Building stats failed — dataset={dataset}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
