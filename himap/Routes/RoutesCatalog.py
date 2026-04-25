"""
Catalog Routes — HiMap v3.0

/health                         — DuckLake connectivity and latency
/datasets                       — list all registered datasets
/datasets/{key}                 — single dataset config
/datasets/{key}/views           — DuckDB view state (debug)
/towns                          — list all registered towns
/towns/{key}                    — single town config + derived spatial fields
/towns/{key}/bbox-params        — ready-to-use params for /query/all
/towns/{key}/h3-params          — ready-to-use params for /query/h3
/datasets/{key}/towns           — all towns linked to a dataset

Filtering model (important):
    Dataset level:  WHERE country = 'canary-islands'
                    Applied at ingestion time by the Partitioner.
                    The OSM source table has a `country` column with
                    admin-boundary-aware values like 'canary-islands',
                    'kenya', etc. DatasetConfig.country_filter must
                    match these exact strings.

    Town level:     WHERE centroid within bbox(town)
                    Applied at query time by the /query/all endpoint.
                    Towns provide the viewport — center ± extent.

    These two filters are orthogonal. The dataset filters by admin
    boundary at write time. The town filters by spatial viewport at
    read time. Neither duplicates the other.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

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
# Datasets
# ---------------------------------------------------------------------------

@router.get(
    "/datasets",
    response_model=DatasetsResponse,
    summary="List registered datasets",
)
async def list_datasets():
    """
    Return all datasets registered in dataset_registry.py.

    country_filter values match the OSM source table's `country` column
    exactly — e.g. 'canary-islands', 'kenya'. These are admin-boundary
    strings, not ISO codes.
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
    config = registry.get(key)
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
    Return all towns whose dataset_key matches this dataset.

    Useful for building city-picker UI — given a dataset, what named
    towns can the user jump to?
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
        "center": {
            "lat": town.lat,
            "lng": town.lng,
        },
        "extent": {
            "lat_extent": town.lat_extent,
            "lng_extent": town.lng_extent,
        },
        "bbox": {
            "sw_lng": sw_lng,
            "sw_lat": sw_lat,
            "ne_lng": ne_lng,
            "ne_lat": ne_lat,
        },
        "bbox_area_km2":  round(town.bbox_area_km2(), 1),
        "linked":         town.dataset_key is not None,
    }


@router.get(
    "/towns",
    summary="List all registered towns",
)
async def list_towns():
    """
    Return all towns registered in TownRegistry.

    linked=true means the town has a dataset_key and is queryable.
    linked=false means the town exists but no dataset has been registered
    for it yet — to_bbox_params() will raise on unlinked towns.
    """
    town_keys = town_registry.list()
    return {
        "count":  len(town_keys),
        "towns":  [_town_summary(k) for k in town_keys],
    }


@router.get(
    "/towns/{key}",
    summary="Single town config",
    responses={400: {"description": "Unknown town key"}},
)
async def get_town(key: str):
    """
    Full config for a single registered town.

    Returns center, extent, bbox, area, and whether the town is linked
    to a registered dataset.
    """
    if key not in town_registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown town: '{key}'. Registered: {town_registry.list()}",
        )
    return _town_summary(key)


@router.get(
    "/towns/{key}/bbox-params",
    summary="Ready-to-use bbox query params for a town",
    responses={
        400: {"description": "Unknown town or town not linked to a dataset"},
    },
)
async def get_town_bbox_params(key: str, limit: int = 5000):
    """
    Return query parameters ready to pass directly to /query/all.

    Filter model:
        Dataset-level country filter is applied by DuckDB at read time
        (via the enriched view, which reads Parquet already filtered by
        the Partitioner's ingestion WHERE country = '...' clause).

        Town-level bbox is the spatial viewport applied by /query/all's
        ST_Intersects call.

        Both filters stack — you get features that are both in the right
        country AND within the town viewport.
    """
    if key not in town_registry:
        raise HTTPException(status_code=400, detail=f"Unknown town: '{key}'")

    town = town_registry.get(key)

    if not town.dataset_key:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Town '{key}' ({town.name}) is not linked to a dataset. "
                f"Register the dataset first, then set town.dataset_key."
            ),
        )

    try:
        params = town.to_bbox_params(limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "town":        key,
        "name":        town.name,
        "query_url":   f"/query/all?dataset={params['dataset']}&sw_lng={params['sw_lng']}&sw_lat={params['sw_lat']}&ne_lng={params['ne_lng']}&ne_lat={params['ne_lat']}&limit={limit}",
        "params":      params,
        "filter_note": {
            "dataset_filter": f"country = '{town_registry.get(key).dataset_key}' (applied at Parquet read via DuckDB view)",
            "town_filter":    f"centroid within bbox sw=({params['sw_lat']:.4f},{params['sw_lng']:.4f}) ne=({params['ne_lat']:.4f},{params['ne_lng']:.4f})",
        },
    }


@router.get(
    "/towns/{key}/h3-params",
    summary="Ready-to-use H3 query params for a town",
    responses={
        400: {"description": "Unknown town, unlinked town, or h3 not installed"},
    },
)
async def get_town_h3_params(key: str, resolution: int = 8, limit: int = 5000):
    """
    Return H3 query parameters for the town center cell.

    The H3 index is computed from the town's center coordinate.
    Resolution 8 (~0.7 km²) is the default — matches the primary
    spatial anchor used in the Partitioner's zorder key.
    """
    if key not in town_registry:
        raise HTTPException(status_code=400, detail=f"Unknown town: '{key}'")

    town = town_registry.get(key)

    if not town.dataset_key:
        raise HTTPException(
            status_code=400,
            detail=f"Town '{key}' is not linked to a dataset.",
        )

    if resolution not in [7, 8, 9, 10]:
        raise HTTPException(
            status_code=400,
            detail=f"Resolution {resolution} not valid. Must be 7, 8, 9, or 10.",
        )

    try:
        params = town.to_h3_params(resolution=resolution, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "town":      key,
        "name":      town.name,
        "query_url": f"/query/h3?dataset={params['dataset']}&h3_index={params['h3_index']}&resolution={resolution}&limit={limit}",
        "params":    params,
        "center": {
            "lat": town.lat,
            "lng": town.lng,
        },
    }
