"""
Catalog Routes — HiMap v3.0

/health             — DuckLake connectivity and latency
/datasets           — list all registered datasets
/datasets/{key}     — single dataset config

Datasets are registered at startup via dataset_registry.py.
There is no runtime registration endpoint — adding a dataset
requires a code change in dataset_registry.py and a server restart.
This is intentional: dataset registration is a deployment event,
not a runtime event.
"""

import logging

from fastapi import APIRouter, HTTPException

from ...dataset_registry import registry
from ...Services.DuckLakeService import ducklake_service
from ...view_generator import ViewGenerator
from ..Models.responses import (
    DatasetInfo,
    DatasetsResponse,
    HealthStatus,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["catalog"])

view_generator = ViewGenerator(ducklake_service)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@router.get(
    "/health",
    response_model=HealthStatus,
    summary="Health check",
    responses={503: {"description": "Service unavailable"}},
)
async def health_check():
    """
    DuckLake connectivity check.
    Returns latency and catalog type (duckdb-only or postgis).
    """
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
# GET /datasets
# ---------------------------------------------------------------------------

@router.get(
    "/datasets",
    response_model=DatasetsResponse,
    summary="List registered datasets",
)
async def list_datasets():
    """
    Return all datasets registered in dataset_registry.py.

    Each entry includes the filter config so clients can verify
    what data is included in each dataset.
    """
    datasets = []
    for key in registry.list():
        config = registry.get(key)
        datasets.append(
            DatasetInfo(
                key=key,
                country=config.country,
                base_path=config.base_path,
                h3_resolutions=config.h3_resolutions,
                has_buildings=config.has_buildings,
                has_roads=config.has_roads,
                country_filter=config.country_filter,
                bbox=config.bbox,
            )
        )

    return DatasetsResponse(count=len(datasets), datasets=datasets)


# ---------------------------------------------------------------------------
# GET /datasets/{key}
# ---------------------------------------------------------------------------

@router.get(
    "/datasets/{key}",
    response_model=DatasetInfo,
    summary="Get single dataset config",
    responses={400: {"description": "Unknown dataset key"}},
)
async def get_dataset(key: str):
    """
    Return config for a single registered dataset.
    Useful for verifying filters and paths before running queries.
    """
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
# GET /datasets/{key}/views
# ---------------------------------------------------------------------------

@router.get(
    "/datasets/{key}/views",
    summary="List DuckDB views for a dataset",
    responses={400: {"description": "Unknown dataset key"}},
)
async def get_dataset_views(key: str):
    """
    Return the DuckDB view names active for this dataset.
    Useful for debugging view state after startup.
    """
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
        "active_views":   {
            name: (name in all_views) for name in expected.values()
        },
    }
