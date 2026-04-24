"""
Response models for HiMap API endpoints.
Provides standardized response schemas with validation.
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, validator
from datetime import datetime


class ErrorDetail(BaseModel):
    """Error detail."""
    loc: Optional[List[str]] = None
    msg: str


class ErrorResponse(BaseModel):
    """Error response."""
    status: str = "error"
    code: str
    message: str
    details: Optional[List[ErrorDetail]] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthStatus(BaseModel):
    """Health check response."""
    
    status: Literal["healthy", "unhealthy"] = Field(
        ...,
        description="Overall health status"
    )
    service: str = Field(
        default="DuckLake",
        description="Service name"
    )
    version: str = Field(
        default="2.0.0",
        description="API version"
    )
    catalog: str = Field(
        ...,
        description="Current catalog source (duckdb/postgis)"
    )
    latency_ms: float = Field(
        ...,
        ge=0,
        description="Query latency in milliseconds"
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if unhealthy"
    )
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Health check timestamp"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "service": "DuckLake",
                "version": "2.0.0",
                "catalog": "postgis",
                "latency_ms": 12.34,
                "timestamp": "2024-04-24T10:30:00"
            }
        }


class CatalogInfo(BaseModel):
    """Catalog information."""
    
    name: str = Field(..., description="Catalog name")
    description: str = Field(..., description="Catalog description")
    type: Literal["embedded", "external"] = Field(..., description="Catalog type")
    tables: Optional[int] = Field(
        default=None,
        description="Number of available tables"
    )


class CatalogsResponse(BaseModel):
    """Response for listing available catalogs."""
    
    service: str = Field(default="DuckLake")
    version: str = Field(default="2.0.0")
    engine: str = Field(default="DuckDB")
    catalog: str = Field(..., description="Current catalog configuration")
    healthy: bool = Field(..., description="Whether the catalog is healthy")
    latency_ms: float = Field(..., ge=0)
    available_catalogs: List[CatalogInfo] = Field(
        ...,
        description="List of available catalog sources"
    )
    note: str = Field(
        ...,
        description="Usage instructions"
    )
    timestamp: datetime = Field(default_factory=datetime.now)


class Position(BaseModel):
    """Geographic position."""
    
    lat: float = Field(..., ge=-90, le=90, description="Latitude")
    lng: float = Field(..., ge=-180, le=180, description="Longitude")
    
    class Config:
        schema_extra = {"example": {"lat": -1.2921, "lng": 36.8219}}


class NodeMetrics(BaseModel):
    """Traffic node metrics."""
    
    passenger_throughput: Optional[int] = Field(
        default=None,
        ge=0,
        description="Daily passenger throughput"
    )
    average_dwell_time: Optional[float] = Field(
        default=None,
        ge=0,
        description="Average dwell time in minutes"
    )
    peak_hour: Optional[str] = Field(
        default=None,
        description="Peak traffic hour"
    )
    saturation_level: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Saturation level (0-100)"
    )


class TrafficNode(BaseModel):
    """Traffic node data."""
    
    id: str = Field(..., description="Node ID")
    name: Optional[str] = Field(default=None, description="Node name")
    position: Position = Field(..., description="Node position")
    type: str = Field(..., description="Node type")
    metrics: NodeMetrics = Field(..., description="Node metrics")
    connected_routes: List[str] = Field(
        default_factory=list,
        description="Connected route IDs"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "id": "node_001",
                "name": "CBD Terminal",
                "position": {"lat": -1.2921, "lng": 36.8219},
                "type": "bus_station",
                "metrics": {
                    "passenger_throughput": 5000,
                    "average_dwell_time": 5.5,
                    "peak_hour": "08:00",
                    "saturation_level": 75.0
                },
                "connected_routes": ["route_1", "route_2"]
            }
        }


class NodesResponse(BaseModel):
    """Response for traffic nodes query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0, description="Number of nodes")
    bbox: List[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="Bounding box [minLng, minLat, maxLng, maxLat]"
    )
    timestamp: datetime = Field(default_factory=datetime.now)
    
    @validator('bbox')
    def validate_bbox(cls, v):
        if len(v) != 4:
            raise ValueError("Bounding box must have exactly 4 values")
        return v


class CorridorMetrics(BaseModel):
    """Corridor analytics metrics."""
    
    fuel_burn_rate: Optional[float] = Field(
        default=None,
        ge=0,
        description="Fuel burn rate (L/km)"
    )
    idling_hotspot_score: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Idling hotspot score (0-100)"
    )
    vehicle_stress_index: Optional[float] = Field(
        default=None,
        ge=0,
        description="Vehicle stress index"
    )
    average_speed: Optional[float] = Field(
        default=None,
        ge=0,
        description="Average speed (km/h)"
    )
    peak_flow_time: Optional[str] = Field(
        default=None,
        description="Peak traffic flow time"
    )


class Corridor(BaseModel):
    """Corridor data."""
    
    id: str = Field(..., description="Corridor ID")
    name: Optional[str] = Field(default=None, description="Corridor name")
    start_node: str = Field(..., description="Start node ID")
    end_node: str = Field(..., description="End node ID")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")
    metrics: CorridorMetrics = Field(..., description="Corridor metrics")


class CorridorsResponse(BaseModel):
    """Response for corridors query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)
    bbox: List[float] = Field(..., min_length=4, max_length=4)
    timestamp: datetime = Field(default_factory=datetime.now)


class VehicleMetrics(BaseModel):
    """Vehicle tracking metrics."""
    
    heading: Optional[float] = Field(
        default=None,
        ge=0,
        lt=360,
        description="Heading in degrees"
    )
    speed: Optional[float] = Field(
        default=None,
        ge=0,
        description="Speed (km/h)"
    )


class Vehicle(BaseModel):
    """Vehicle tracking data."""
    
    id: str = Field(..., description="Vehicle ID")
    sacco_id: str = Field(..., description="SACCO ID")
    sacco_name: str = Field(..., description="SACCO name")
    plate_number: str = Field(..., description="License plate")
    capacity: int = Field(..., ge=1, description="Passenger capacity")
    current_position: Position = Field(..., description="Current position")
    heading: Optional[float] = Field(default=None, ge=0, lt=360)
    speed: Optional[float] = Field(default=None, ge=0)
    status: Literal["active", "inactive"] = Field(..., description="Vehicle status")
    last_updated: Optional[str] = Field(
        default=None,
        description="Last update timestamp (ISO 8601)"
    )


class VehiclesResponse(BaseModel):
    """Response for vehicles query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)
    bbox: List[float] = Field(..., min_length=4, max_length=4)
    timestamp: datetime = Field(default_factory=datetime.now)


class NearestVehicle(Vehicle):
    """Vehicle with distance information."""
    
    distance: float = Field(
        ...,
        ge=0,
        description="Distance to query point (meters)"
    )


class NearestVehiclesResponse(BaseModel):
    """Response for nearest vehicles query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)
    query_point: Position = Field(..., description="Query point coordinates")
    timestamp: datetime = Field(default_factory=datetime.now)


class H3CellProperties(BaseModel):
    """H3 cell properties."""
    
    feature_count: Optional[int] = Field(default=None, ge=0)
    road_count: Optional[int] = Field(default=None, ge=0)
    building_count: Optional[int] = Field(default=None, ge=0)
    poi_count: Optional[int] = Field(default=None, ge=0)
    density_score: Optional[float] = Field(default=None, ge=0)


class H3Cell(BaseModel):
    """H3 grid cell data."""
    
    cell_id: str = Field(..., description="H3 cell ID")
    resolution: int = Field(..., ge=0, le=15, description="H3 resolution")
    boundary: Dict[str, Any] = Field(..., description="GeoJSON polygon")
    center: Position = Field(..., description="Cell center")
    properties: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional properties"
    )


class H3CellsResponse(BaseModel):
    """Response for H3 cells query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)
    bbox: List[float] = Field(..., min_length=4, max_length=4)
    timestamp: datetime = Field(default_factory=datetime.now)


class H3CellQueryResponse(BaseModel):
    """Response for H3 cell-based feature query."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    h3_cell: str = Field(..., description="Queried H3 cell ID")
    h3_resolution: int = Field(..., ge=0, le=15)
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)
    timestamp: datetime = Field(default_factory=datetime.now)


class BoundsStats(BaseModel):
    """Bounding box statistics."""
    
    node_count: int = Field(..., ge=0)
    vehicle_count: int = Field(..., ge=0)
    corridor_count: int = Field(..., ge=0)


class BoundsStatsResponse(BaseModel):
    """Response for bounds statistics query."""
    
    stats: BoundsStats = Field(..., description="Bounding box statistics")
    bbox: List[float] = Field(..., min_length=4, max_length=4)
    timestamp: datetime = Field(default_factory=datetime.now)


class DataTypeCollection(BaseModel):
    """Collection of features for a specific data type."""
    
    type: Literal["FeatureCollection"] = Field(default="FeatureCollection")
    features: List[Dict[str, Any]] = Field(..., description="GeoJSON features")
    count: int = Field(..., ge=0)


class AllDataResponse(BaseModel):
    """Response for querying all data types."""
    
    nodes: DataTypeCollection = Field(..., description="Traffic nodes")
    corridors: DataTypeCollection = Field(..., description="Corridors")
    vehicles: DataTypeCollection = Field(..., description="Vehicles")
    h3_cells: DataTypeCollection = Field(..., description="H3 cells")
    bbox: List[float] = Field(..., min_length=4, max_length=4)
    timestamp: datetime = Field(default_factory=datetime.now)


class TileKey(BaseModel):
    """Tile manifest entry."""
    
    z: int = Field(..., ge=0, le=20)
    x: int = Field(..., ge=0)
    y: int = Field(..., ge=0)
    feature_count: int = Field(..., ge=0)
    road_count: Optional[int] = Field(default=None, ge=0)
    building_count: Optional[int] = Field(default=None, ge=0)
    parquet_url: str = Field(..., description="CDN URL to Parquet file")


class PartitionManifest(BaseModel):
    """Partition manifest for a city."""
    
    city_id: str = Field(..., description="City identifier")
    country_code: str = Field(..., min_length=2, max_length=2)
    tile_zoom: int = Field(..., ge=0, le=20)
    zorder_range: List[int] = Field(
        ...,
        min_length=2,
        max_length=2,
        description="[min, max] z-order keys"
    )
    h3_7_cells: List[str] = Field(..., description="List of H3 resolution 7 cells")
    tile_count: int = Field(..., ge=0)
    total_features: int = Field(..., ge=0)
    tile_keys: List[TileKey] = Field(..., description="Individual tile metadata")
    generated_at: datetime = Field(default_factory=datetime.now)


class CatalogSetResponse(BaseModel):
    """Response for setting catalog configuration."""
    
    message: str = Field(..., description="Status message")
    catalog: str = Field(..., description="Configured catalog")
    host: Optional[str] = Field(default=None, description="PostGIS host if applicable")
    database: Optional[str] = Field(default=None, description="PostGIS database if applicable")
    timestamp: datetime = Field(default_factory=datetime.now)


class ExportResponse(BaseModel):
    """Response for data export."""
    
    message: str = Field(..., description="Export status")
    file_path: str = Field(..., description="Path to exported file")
    file_size: Optional[int] = Field(
        default=None,
        ge=0,
        description="File size in bytes"
    )
    record_count: int = Field(..., ge=0, description="Number of exported records")
    validated: bool = Field(..., description="Whether Parquet validation passed")
    timestamp: datetime = Field(default_factory=datetime.now)


class APIRootResponse(BaseModel):
    """Root API information response."""
    
    message: str = Field(default="HiMap Spatial Data API")
    version: str = Field(default="2.0.0")
    description: str = Field(...)
    architecture: Dict[str, str] = Field(...)
    endpoints: Dict[str, str] = Field(...)


# Union type for any response
AnyResponse = (
    NodesResponse |
    VehiclesResponse |
    CorridorsResponse |
    H3CellsResponse |
    AllDataResponse |
    HealthStatus |
    CatalogsResponse |
    ErrorResponse
)
