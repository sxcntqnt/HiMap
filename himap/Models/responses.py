"""
Response Models — HiMap v3.0

Dead models removed (NodesResponse, VehiclesResponse, CorridorsResponse,
H3CellsResponse, AllDataResponse, CatalogsResponse, CatalogSetResponse,
ExportResponse, NearestVehiclesResponse, BoundsStatsResponse —
all tied to removed endpoints).

Kept and updated:
    ErrorDetail          — field-level error with loc + msg + type
    ErrorResponse        — standardized error envelope with timestamp
    HealthStatus         — richer (version, service, timestamp restored)
    Position             — shared geographic point
    FeaturesResponse     — standard spatial query response
    Feature              — GeoJSON feature
    DatasetInfo          — single dataset config summary
    DatasetsResponse     — /datasets list
    TileKey              — per-partition entry in manifest (updated to v3.0 fields)
    PartitionManifest    — full manifest written by Partitioner, served by API
    PartitionInfo        — single partition address + existence
    APIRootResponse      — / root endpoint
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ErrorDetail(BaseModel):
    loc:  List[str]
    msg:  str
    type: str


class ErrorResponse(BaseModel):
    status:    str                         = "error"
    code:      str
    message:   str
    details:   Optional[List[ErrorDetail]] = None
    timestamp: datetime                    = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthStatus(BaseModel):
    status:     Literal["healthy", "unhealthy"]
    service:    str     = "DuckLake"
    version:    str     = "3.0.0"
    catalog:    str                              # "duckdb-only" | "postgis"
    latency_ms: float   = Field(..., ge=0)
    error:      Optional[str]  = None
    timestamp:  datetime       = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Geography
# ---------------------------------------------------------------------------

class Position(BaseModel):
    """Geographic point — used wherever a lat/lng pair is returned."""
    lat: float = Field(..., ge=-90,  le=90)
    lng: float = Field(..., ge=-180, le=180)


# ---------------------------------------------------------------------------
# Spatial query responses
# ---------------------------------------------------------------------------

class Feature(BaseModel):
    """GeoJSON-compatible feature."""
    type:       str              = "Feature"
    properties: Dict[str, Any]
    geometry:   Dict[str, Any]


class FeaturesResponse(BaseModel):
    """
    Standard response for /query/all and /query/h3.

    Returns a flat feature list — client assembles FeatureCollection.
    Features are ordered by entropy_bucket ASC, importance_byte DESC
    (matches the VoI priority contract from the Matatu Pulse SSE layer).
    """
    dataset:  str
    count:    int
    features: List[Feature]
    query:    Dict[str, Any]    # echo of request params for client-side cache keying
    timestamp: datetime         = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Dataset registry responses
# ---------------------------------------------------------------------------

class DatasetInfo(BaseModel):
    """Single dataset config — returned by /datasets and /datasets/{key}."""
    key:            str
    country:        str
    base_path:      str
    h3_resolutions: List[int]
    has_buildings:  bool
    has_roads:      bool
    country_filter: Optional[str]
    bbox:           Optional[Any]   # tuple or None


class DatasetsResponse(BaseModel):
    """Response for /datasets."""
    count:    int
    datasets: List[DatasetInfo]


# ---------------------------------------------------------------------------
# Partition manifest
# Mirrors the structure written by Partitioner._write_manifest()
# and served by /partitions/{dataset}/manifest
# ---------------------------------------------------------------------------

class TileKey(BaseModel):
    """
    Per-partition entry in the manifest.

    v3.0 fields (entropyScore, compressedSizeBytes, partitionRunId,
    fetchPriority) are required — they feed the SSE manager VoI scoring.
    """
    z:                    int
    x:                    int
    y:                    int
    featureCount:         int
    entropyScore:         float
    compressedSizeBytes:  int
    partitionRunId:       str
    fetchPriority:        Literal["immediate", "background", "defer"]
    parquetUrl:           str     # relative path; client prepends base URL


class BudgetHint(BaseModel):
    """SW prefetch budget hints from the SSE contract."""
    maxImmediateBytes:  int
    maxBackgroundBytes: int


class PartitionManifest(BaseModel):
    """
    Full manifest for a dataset partition run.

    Written by Partitioner._write_manifest().
    Served by GET /partitions/{dataset}/manifest.
    Read by SSE manager on client connect to build TilePriorityManifest.
    """
    countryCode:    str
    tileZoom:       int
    h3Resolutions:  List[int]
    generatedAt:    str                  # ISO timestamp string from pipeline
    tile_count:     int
    total_features: int
    tileKeys:       List[TileKey]
    budgetHint:     BudgetHint


# ---------------------------------------------------------------------------
# Partition file info
# ---------------------------------------------------------------------------

class PartitionInfo(BaseModel):
    """Single partition address — used in debug/introspection responses."""
    dataset: str
    z:       int
    x:       int
    y:       int
    path:    str
    exists:  bool


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------

class APIRootResponse(BaseModel):
    """Response for GET /."""
    name:      str               = "HiMap Spatial Data API"
    version:   str               = "3.0.0"
    datasets:  List[str]
    endpoints: Dict[str, str]
