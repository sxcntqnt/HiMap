"""
Catalog Routes — HiMap v3.0

Route declaration order matters in FastAPI — specific paths before
parameterized ones. Order within this file:

    /health                     — no params, always first
    /zoom/{zoom}                — specific prefix before /{key} routes
    /datasets                   — specific, before /datasets/{key}
    /datasets/{key}/views       — sub-path before bare /{key}
    /datasets/{key}/towns       — sub-path before bare /{key}
    /datasets/{key}             — parameterized, last in datasets group
    /towns                      — specific, before /towns/{key}
    /towns/{key}/bbox-params    — sub-path before bare /{key}
    /towns/{key}/h3-params      — sub-path before bare /{key}
    /towns/{key}                — parameterized, last in towns group

Filtering model:
    Dataset level:  WHERE country = 'canary-islands'
                    Applied at ingestion by the Partitioner.
                    country_filter must exactly match the osm.country column value.

    Town level:     WHERE centroid within bbox(town)
                    Applied at query time via the /query/all ST_Intersects call.

    These are orthogonal — dataset filters at write time, town filters at read time.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..Ingestion.DataRegistry import registry
from ..Services.DuckLakeService import ducklake_service
from ..Towns import town_registry, TownBase
from ..Generator.viewGenerator import ViewGenerator
from ..Models.responses import (
    DatasetInfo,
    DatasetsResponse,
    HealthStatus,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["catalog"])
view_generator = ViewGenerator(ducklake_service)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@router.get(
    "/health",
    response_model=HealthStatus,
    summary="Health check",
    responses={503: {"description": "Service unavailable"}},
)
async def health_check():
    """DuckLake connectivity. Returns latency, catalog type, version."""
    try:
        health = ducklake_service.health_check()
        return HealthStatus(
            status="healthy" if health["healthy"] else "unhealthy",
            service="DuckLake",
            version="3.0.0",
            catalog=health.get("catalog", "unknown"),
            latency_ms=health.get("latency_ms", 0),
            error=health.get("error"),
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Zoom — declared before /{key} routes to prevent shadowing
# ---------------------------------------------------------------------------

@router.get(
    "/zoom/{zoom}",
    summary="Zoom level metadata at a latitude",
    responses={400: {"description": "Zoom out of range"}},
)
async def get_zoom_info(zoom: int, lat: float = Query(0.0, ge=-90, le=90,
                                                       description="Center latitude for projection correction")):
    """
    Ground resolution, H3 column mapping, and semantic level for a zoom level.

    lat defaults to 0.0 (equatorial). Always pass the actual center latitude
    of your viewport — the corrected ground resolution differs significantly
    at mid-latitudes:

        zoom 12, equatorial:           38.22 m/px
        zoom 12, Las Palmas (28°):     33.69 m/px  (12% smaller)
        zoom 12, Prague     (50°):     24.52 m/px  (36% smaller)

    Also returns which h3_col to use for this zoom:
        zoom 0–10  → h3_7
        zoom 11–12 → h3_8
        zoom 13–14 → h3_9
        zoom 15+   → h3_10
    """
    from ...Utils.Zoom import zoom_levels
    try:
        return zoom_levels.describe(zoom, lat)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

@router.get(
    "/datasets",
    response_model=DatasetsResponse,
    summary="List registered datasets",
)
async def list_datasets():
    """
    All datasets registered in dataset_registry.py.

    country_filter values match the osm.country column exactly:
        'canary-islands', 'kenya', etc.
    Run SELECT DISTINCT country FROM osm; to verify values for new datasets.
    """
    datasets = []
    for key in registry.list():
        config = registry.get(key)
        datasets.append(DatasetInfo(
            key=key,
            country=config.country,
            base_path=config.base_path,
            h3_resolutions=config.h3_resolutions,
            has_buildings=config.has_buildings,
            has_roads=config.has_roads,
            country_filter=config.country_filter,
            bbox=config.bbox,
        ))
    return DatasetsResponse(count=len(datasets), datasets=datasets)


@router.get(
    "/datasets/{key}/views",
    summary="DuckDB view state for a dataset",
    responses={400: {"description": "Unknown dataset key"}},
)
async def get_dataset_views(key: str):
    """Which DuckDB views are active for this dataset. Debug endpoint."""
    if key not in registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset: '{key}'. Registered: {registry.list()}",
        )
    config   = registry.get(key)
    all_views = view_generator.list_views()
    expected = {
        "base_view":     config.view_name(key),
        "enriched_view": config.enriched_view_name(key),
    }
    return {
        "dataset":        key,
        "expected_views": expected,
        "active_views":   {name: (name in all_views) for name in expected.values()},
    }


@router.get(
    "/datasets/{key}/towns",
    summary="Towns linked to a dataset",
    responses={400: {"description": "Unknown dataset key"}},
)
async def get_dataset_towns(key: str):
    """
    All towns whose dataset_key matches this dataset.

    Useful for building a city-picker UI: given a dataset key, what
    named viewports can the user jump to?
    """
    if key not in registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset: '{key}'. Registered: {registry.list()}",
        )
    town_keys = town_registry.list_by_dataset(key)
    return {
        "dataset": key,
        "count":   len(town_keys),
        "towns":   [_town_summary(k) for k in town_keys],
    }


@router.get(
    "/datasets/{key}",
    response_model=DatasetInfo,
    summary="Get single dataset config",
    responses={400: {"description": "Unknown dataset key"}},
)
async def get_dataset(key: str):
    """Single dataset config including country_filter and base_path."""
    try:
        config = registry.get(key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return DatasetInfo(
        key=key,
        country=config.country,
        base_path=config.base_path,
        h3_resolutions=config.h3_resolutions,
        has_buildings=config.has_buildings,
        has_roads=config.has_roads,
        country_filter=config.country_filter,
        bbox=config.bbox,
    )


# ---------------------------------------------------------------------------
# Towns
# ---------------------------------------------------------------------------

def _town_summary(key: str) -> Dict[str, Any]:
    """Serialise a TownBase to a dict for API responses."""
    town = town_registry.get(key)
    sw_lng, sw_lat, ne_lng, ne_lat = town.bbox()
    return {
        "key":          key,
        "name":         town.name,
        "country_code": town.country_code,
        "dataset_key":  town.dataset_key,
        "center":  {"lat": town.lat,        "lng": town.lng},
        "extent":  {"lat_extent": town.lat_extent, "lng_extent": town.lng_extent},
        "bbox":    {"sw_lng": sw_lng, "sw_lat": sw_lat, "ne_lng": ne_lng, "ne_lat": ne_lat},
        "bbox_area_km2": round(town.bbox_area_km2(), 1),
        "linked":  town.dataset_key is not None,
    }


@router.get("/towns", summary="List all registered towns")
async def list_towns():
    """
    All towns in the TownRegistry.
    linked=true means the town has a dataset_key and is queryable via /query/all.
    linked=false means the town exists but no dataset is registered for it yet.
    """
    town_keys = town_registry.list()
    return {"count": len(town_keys), "towns": [_town_summary(k) for k in town_keys]}


@router.get(
    "/towns/{key}/bbox-params",
    summary="Ready-to-use bbox params for /query/all",
    responses={400: {"description": "Unknown or unlinked town"}},
)
async def get_town_bbox_params(
    key:   str,
    zoom:  Optional[int] = Query(None, ge=0, le=20,
                                  description="Include zoom for zoom-aware limit in response"),
    limit: int = Query(5000, ge=1, le=10000),
):
    """
    Complete params for a /query/all call scoped to this town's viewport.

    Pass zoom to get the zoom-aware limit and importance threshold
    echoed back — useful for clients building the request URL.

    Filter model:
        Dataset filter (country='...') is applied at Parquet read time via DuckDB view.
        Town bbox filter is applied by /query/all's ST_Intersects call.
        Both stack — features must be in the right country AND in the viewport.
    """
    if key not in town_registry:
        raise HTTPException(status_code=400, detail=f"Unknown town: '{key}'")

    town = town_registry.get(key)
    if not town.dataset_key:
        raise HTTPException(
            status_code=400,
            detail=f"Town '{key}' ({town.name}) is not linked to a dataset.",
        )

    try:
        params = town.to_bbox_params(limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from ...Utils.Zoom import zoom_levels as _zl
    zoom_info = _zl.describe(zoom, town.lat) if zoom is not None else None

    sw_lng, sw_lat, ne_lng, ne_lat = town.bbox()
    query_url = (
        f"/query/all?dataset={params['dataset']}"
        f"&sw_lng={sw_lng}&sw_lat={sw_lat}"
        f"&ne_lng={ne_lng}&ne_lat={ne_lat}"
        f"&limit={limit}"
        + (f"&zoom={zoom}" if zoom is not None else "")
    )

    return {
        "town":       key,
        "name":       town.name,
        "query_url":  query_url,
        "params":     params,
        "zoom_info":  zoom_info,
        "filter_note": {
            "dataset_filter": f"country = '{town.dataset_key}' (Parquet read-time via DuckDB view)",
            "town_filter":    f"centroid in bbox ({sw_lat:.4f},{sw_lng:.4f}) → ({ne_lat:.4f},{ne_lng:.4f})",
        },
    }


@router.get(
    "/towns/{key}/h3-params",
    summary="Ready-to-use H3 params for /query/h3",
    responses={400: {"description": "Unknown or unlinked town"}},
)
async def get_town_h3_params(
    key:        str,
    zoom:       Optional[int] = Query(None, ge=0, le=20,
                                       description="Derive resolution automatically from zoom"),
    resolution: Optional[int] = Query(None, ge=7, le=10,
                                       description="Override resolution explicitly"),
    limit:      int = Query(5000, ge=1, le=10000),
):
    """
    H3 params for the town center cell.

    Resolution priority: explicit resolution > zoom-derived > default (8).
    """
    if key not in town_registry:
        raise HTTPException(status_code=400, detail=f"Unknown town: '{key}'")

    town = town_registry.get(key)
    if not town.dataset_key:
        raise HTTPException(status_code=400, detail=f"Town '{key}' is not linked to a dataset.")

    # Resolve resolution
    if resolution is None:
        if zoom is not None:
            from ...Utils.Zoom import zoom_levels as _zl
            resolution = _zl.h3_resolution_for_zoom(zoom)
        else:
            resolution = 8

    if resolution not in [7, 8, 9, 10]:
        raise HTTPException(status_code=400, detail=f"Resolution {resolution} must be 7–10.")

    try:
        params = town.to_h3_params(resolution=resolution, limit=limit)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    query_url = (
        f"/query/h3?dataset={params['dataset']}"
        f"&h3_index={params['h3_index']}"
        f"&resolution={resolution}"
        f"&limit={limit}"
        + (f"&zoom={zoom}" if zoom is not None else "")
    )

    return {
        "town":       key,
        "name":       town.name,
        "query_url":  query_url,
        "params":     params,
        "center":     {"lat": town.lat, "lng": town.lng},
        "resolution": resolution,
        "zoom":       zoom,
    }


@router.get(
    "/towns/{key}",
    summary="Single town config",
    responses={400: {"description": "Unknown town key"}},
)
async def get_town(key: str):
    """Full config for a single registered town — center, extent, bbox, area, linked status."""
    if key not in town_registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown town: '{key}'. Registered: {town_registry.list()}",
        )
    return _town_summary(key)
