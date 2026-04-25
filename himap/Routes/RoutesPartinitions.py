"""
Partition Routes — HiMap v3.0

/partitions/{dataset}/{z}/{x}/{y}.parquet  — serve Parquet file directly
/partitions/{dataset}/manifest             — serve manifest.json

File paths are resolved through the dataset registry.
No hardcoded paths in this file.

Immutability contract:
    Parquet files are immutable once written.
    Cache-Control is set to 7 days for partition files.
    Manifest is set to 5 minutes (changes on each pipeline run).
"""

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import json

from ..Ingestion.DataRegistry import registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/partitions", tags=["partitions"])


def _resolve_partition_path(dataset: str, z: int, x: int, y: int) -> Path:
    """
    Resolve the filesystem path for a partition file.
    Delegates path root to dataset registry — no hardcoded paths.
    """
    if dataset not in registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset: '{dataset}'. Registered: {registry.list()}",
        )

    config = registry.get(dataset)
    base = Path(config.base_path.rstrip("/"))
    return base / f"z{z}" / str(x) / f"{y}.parquet"


# ---------------------------------------------------------------------------
# GET /partitions/{dataset}/{z}/{x}/{y}.parquet
# ---------------------------------------------------------------------------

@router.get(
    "/{dataset}/{z}/{x}/{y}.parquet",
    summary="Get partitioned Parquet file",
    response_class=FileResponse,
    responses={
        200: {"content": {"application/octet-stream": {}}},
        400: {"description": "Unknown dataset"},
        404: {"description": "Partition not found"},
    },
)
async def get_partition(dataset: str, z: int, x: int, y: int):
    """
    Serve a single Parquet partition file.

    Files are immutable — cached for 7 days by CDN and service worker.
    Path: {dataset_base_path}/z{z}/{x}/{y}.parquet
    """
    partition_path = _resolve_partition_path(dataset, z, x, y)

    if not partition_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Partition not found: dataset={dataset} z={z} x={x} y={y}",
        )

    return FileResponse(
        path=str(partition_path),
        media_type="application/octet-stream",
        headers={
            "X-Dataset":     dataset,
            "X-Partition-Z": str(z),
            "X-Partition-X": str(x),
            "X-Partition-Y": str(y),
            # Immutable — Parquet files never change after write
            "Cache-Control": "public, max-age=604800, immutable",
        },
    )


# ---------------------------------------------------------------------------
# GET /partitions/{dataset}/manifest
# ---------------------------------------------------------------------------

@router.get(
    "/{dataset}/manifest",
    summary="Get partition manifest",
    responses={
        400: {"description": "Unknown dataset"},
        404: {"description": "Manifest not found — run pipeline first"},
    },
)
async def get_manifest(dataset: str):
    """
    Serve the manifest.json for a dataset.

    Manifest is written by the Partitioner pipeline and contains:
        - tileKeys with entropy, size, and fetchPriority per partition
        - budgetHint for service worker prefetch queue
        - h3Resolutions available in this dataset

    Cached for 5 minutes — short TTL because manifests update on
    each pipeline run.
    """
    if dataset not in registry:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset: '{dataset}'. Registered: {registry.list()}",
        )

    config = registry.get(dataset)
    manifest_path = Path(config.base_path.rstrip("/")) / "manifest.json"

    if not manifest_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"Manifest not found for dataset '{dataset}'. "
                f"Run the partitioner pipeline first: "
                f"python partition_data.py --dataset {dataset}"
            ),
        )

    with open(manifest_path) as f:
        manifest = json.load(f)

    return JSONResponse(
        content=manifest,
        headers={
            "Cache-Control": "public, max-age=300",   # 5 min
            "X-Dataset":     dataset,
        },
    )
