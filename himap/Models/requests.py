"""
Request validation models for HiMap API endpoints.
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator, root_validator


class Coordinates(BaseModel):
    """Geographic coordinates with validation."""
    
    lat: float = Field(
        ...,
        ge=-90.0,
        le=90.0,
        description="Latitude in decimal degrees",
        example=-1.2921
    )
    lng: float = Field(
        ...,
        ge=-180.0,
        le=180.0,
        description="Longitude in decimal degrees",
        example=36.8219
    )
    
    class Config:
        schema_extra = {
            "example": {"lat": -1.2921, "lng": 36.8219}
        }


class BoundingBox(BaseModel):
    """Bounding box with southwest and northeast corners."""
    
    south_west_lng: float = Field(
        ...,
        ge=-180.0,
        le=180.0,
        description="Southwest longitude",
        example=36.65
    )
    south_west_lat: float = Field(
        ...,
        ge=-90.0,
        le=90.0,
        description="Southwest latitude",
        example=-1.45
    )
    north_east_lng: float = Field(
        ...,
        ge=-180.0,
        le=180.0,
        description="Northeast longitude",
        example=36.95
    )
    north_east_lat: float = Field(
        ...,
        ge=-90.0,
        le=90.0,
        description="Northeast latitude",
        example=-1.15
    )
    
    @validator('north_east_lng')
    def validate_longitude_range(cls, v, values):
        """Ensure northeast longitude is >= southwest longitude."""
        if 'south_west_lng' in values and v < values['south_west_lng']:
            raise ValueError("Northeast longitude must be >= southwest longitude")
        return v
    
    @validator('north_east_lat')
    def validate_latitude_range(cls, v, values):
        """Ensure northeast latitude is >= southwest latitude."""
        if 'south_west_lat' in values and v < values['south_west_lat']:
            raise ValueError("Northeast latitude must be >= southwest latitude")
        return v
    
    @validator('south_west_lng', 'north_east_lng')
    def validate_no_wraparound(cls, v):
        """Prevent queries that wrap around the antimeridian."""
        return v
    
    @root_validator
    def validate_bbox_size(cls, values):
        """Validate bounding box is not too large."""
        sw_lng = values.get('south_west_lng')
        sw_lat = values.get('south_west_lat')
        ne_lng = values.get('north_east_lng')
        ne_lat = values.get('north_east_lat')
        
        if all(v is not None for v in [sw_lng, sw_lat, ne_lng, ne_lat]):
            width = abs(ne_lng - sw_lng)
            height = abs(ne_lat - sw_lat)
            
            # Prevent extremely large queries (>10 degrees)
            if width > 10:
                raise ValueError(f"Bounding box width ({width:.2f}°) exceeds maximum (10°)")
            if height > 10:
                raise ValueError(f"Bounding box height ({height:.2f}°) exceeds maximum (10°)")
        
        return values
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15
            }
        }


class NodeQueryParams(BaseModel):
    """Parameters for traffic node queries."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    node_types: Optional[List[str]] = Field(
        default=None,
        description="Filter by node types"
    )
    min_saturation: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Minimum saturation level (0-100)"
    )
    limit: int = Field(
        default=500,
        ge=1,
        le=10000,
        description="Maximum number of results"
    )
    
    @root_validator
    def validate_bbox(cls, values):
        """Validate bounding box dimensions."""
        sw_lng = values.get('south_west_lng')
        ne_lng = values.get('north_east_lng')
        sw_lat = values.get('south_west_lat')
        ne_lat = values.get('north_east_lat')
        
        if sw_lng and ne_lng and sw_lng > ne_lng:
            raise ValueError("North-east longitude must be >= south-west longitude")
        if sw_lat and ne_lat and sw_lat > ne_lat:
            raise ValueError("North-east latitude must be >= south-west latitude")
        
        return values
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "limit": 500
            }
        }


class VehicleQueryParams(BaseModel):
    """Parameters for vehicle tracking queries."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    status: Optional[str] = Field(
        default="active",
        pattern=r"^(active|inactive|all)$",
        description="Filter by vehicle status"
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of results"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "status": "active",
                "limit": 1000
            }
        }


class NearestVehicleParams(BaseModel):
    """Parameters for nearest vehicle queries."""
    
    point_lng: float = Field(..., ge=-180.0, le=180.0, description="Query point longitude")
    point_lat: float = Field(..., ge=-90.0, le=90.0, description="Query point latitude")
    limit: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of nearest vehicles to return"
    )
    max_distance: int = Field(
        default=5000,
        ge=100,
        le=50000,
        description="Maximum search distance in meters"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "point_lng": 36.82,
                "point_lat": -1.29,
                "limit": 10,
                "max_distance": 5000
            }
        }


class CorridorQueryParams(BaseModel):
    """Parameters for corridor analytics queries."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    limit: int = Field(
        default=200,
        ge=1,
        le=5000,
        description="Maximum number of results"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "limit": 200
            }
        }


class H3QueryParams(BaseModel):
    """Parameters for H3 grid queries."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    resolution: int = Field(
        default=9,
        ge=0,
        le=15,
        description="H3 resolution (0-15)"
    )
    limit: int = Field(
        default=5000,
        ge=1,
        le=20000,
        description="Maximum number of results"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "resolution": 9,
                "limit": 5000
            }
        }


class H3CellQueryParams(BaseModel):
    """Parameters for H3 cell-based queries."""
    
    h3_cell: str = Field(
        ...,
        min_length=15,
        max_length=16,
        pattern=r"^[0-9a-f]{15,16}$",
        description="H3 cell ID (15-16 character hex string)",
        example="87344325effffff"
    )
    city: str = Field(
        default="nairobi",
        min_length=1,
        max_length=64,
        description="City identifier"
    )
    country: str = Field(
        default="KE",
        min_length=2,
        max_length=2,
        pattern=r"^[A-Z]{2}$",
        description="ISO country code (2 letters)"
    )
    resolution: int = Field(
        default=8,
        ge=7,
        le=10,
        description="H3 resolution to query"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "h3_cell": "87344325effffff",
                "city": "nairobi",
                "country": "KE",
                "resolution": 8
            }
        }


class AllDataQueryParams(BaseModel):
    """Parameters for querying all data types."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    node_types: Optional[List[str]] = Field(default=None)
    min_saturation: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    h3_resolution: int = Field(default=9, ge=0, le=15)
    vehicle_limit: int = Field(default=1000, ge=1, le=10000)
    nodes_limit: int = Field(default=500, ge=1, le=10000)
    corridors_limit: int = Field(default=200, ge=1, le=5000)
    h3_limit: int = Field(default=5000, ge=1, le=20000)
    
    @root_validator
    def validate_total_limit(cls, values):
        """Validate total limit doesn't exceed safety threshold."""
        total = sum([
            values.get('vehicle_limit', 0),
            values.get('nodes_limit', 0),
            values.get('corridors_limit', 0),
            values.get('h3_limit', 0)
        ])
        
        if total > 50000:
            raise ValueError(f"Total limit ({total}) exceeds maximum (50000)")
        
        return values
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "nodes_limit": 500,
                "vehicle_limit": 1000,
                "corridors_limit": 200
            }
        }


class ExportParams(BaseModel):
    """Parameters for data export."""
    
    south_west_lng: float = Field(..., ge=-180.0, le=180.0)
    south_west_lat: float = Field(..., ge=-90.0, le=90.0)
    north_east_lng: float = Field(..., ge=-180.0, le=180.0)
    north_east_lat: float = Field(..., ge=-90.0, le=90.0)
    node_types: Optional[List[str]] = Field(default=None)
    min_saturation: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    h3_resolution: int = Field(default=9, ge=0, le=15)
    validate: bool = Field(default=True, description="Validate Parquet file after creation")
    
    class Config:
        schema_extra = {
            "example": {
                "south_west_lng": 36.65,
                "south_west_lat": -1.45,
                "north_east_lng": 36.95,
                "north_east_lat": -1.15,
                "validate": True
            }
        }


class PartitionExportParams(BaseModel):
    """Parameters for partitioned data export."""
    
    z: int = Field(
        ...,
        ge=0,
        le=20,
        description="Quadtree zoom level"
    )
    x: int = Field(
        ...,
        ge=0,
        description="Quadtree X coordinate"
    )
    y: int = Field(
        ...,
        ge=0,
        description="Quadtree Y coordinate"
    )
    city: str = Field(
        default="nairobi",
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9_-]+$",
        description="City identifier (lowercase alphanumeric)"
    )
    country: str = Field(
        default="KE",
        min_length=2,
        max_length=2,
        pattern=r"^[A-Z]{2}$",
        description="ISO country code"
    )
    format: Literal["geojson", "raw"] = Field(
        default="geojson",
        description="Output format"
    )
    
    @validator('x', 'y')
    def validate_tile_coordinates(cls, v, values):
        """Validate tile coordinates are within valid range for zoom level."""
        z = values.get('z')
        if z is not None and v is not None:
            max_coord = 2 ** z - 1
            if v > max_coord:
                raise ValueError(f"Tile coordinate {v} exceeds maximum {max_coord} for zoom {z}")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "z": 10,
                "x": 618,
                "y": 480,
                "city": "nairobi",
                "country": "KE",
                "format": "geojson"
            }
        }
