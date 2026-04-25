"""
Request Models — HiMap v3.0

Dead models removed (NodeQueryParams, VehicleQueryParams, CorridorQueryParams,
AllDataQueryParams, ExportParams — all referenced dead tables/endpoints).

Kept and updated:
    Coordinates          — shared geographic point type
    BoundingBox          — with size guard (max 10°) and crossing validation
    BBoxQueryParams      — /query/all bbox mode
    H3QueryParams        — /query/h3 H3 index mode
    PartitionParams      — /partitions/{dataset}/{z}/{x}/{y}.parquet
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Shared primitives
# ---------------------------------------------------------------------------

class Coordinates(BaseModel):
    """Geographic coordinates."""
    lat: float = Field(..., ge=-90.0,  le=90.0,  description="Latitude in decimal degrees",  examples=[-1.2921])
    lng: float = Field(..., ge=-180.0, le=180.0, description="Longitude in decimal degrees", examples=[36.8219])


class BoundingBox(BaseModel):
    """
    Bounding box with southwest and northeast corners.

    Validates:
        - ne_lng >= sw_lng
        - ne_lat >= sw_lat
        - width  <= 10° (prevents runaway scans)
        - height <= 10° (prevents runaway scans)
    """
    sw_lng: float = Field(..., ge=-180.0, le=180.0, description="Southwest longitude", examples=[36.65])
    sw_lat: float = Field(..., ge=-90.0,  le=90.0,  description="Southwest latitude",  examples=[-1.45])
    ne_lng: float = Field(..., ge=-180.0, le=180.0, description="Northeast longitude", examples=[36.95])
    ne_lat: float = Field(..., ge=-90.0,  le=90.0,  description="Northeast latitude",  examples=[-1.15])

    @model_validator(mode="after")
    def validate_bbox(self):
        if self.sw_lng >= self.ne_lng:
            raise ValueError("sw_lng must be less than ne_lng")
        if self.sw_lat >= self.ne_lat:
            raise ValueError("sw_lat must be less than ne_lat")

        width  = abs(self.ne_lng - self.sw_lng)
        height = abs(self.ne_lat - self.sw_lat)

        if width > 10:
            raise ValueError(f"Bounding box width ({width:.2f}°) exceeds maximum (10°)")
        if height > 10:
            raise ValueError(f"Bounding box height ({height:.2f}°) exceeds maximum (10°)")

        return self


# ---------------------------------------------------------------------------
# Query params
# ---------------------------------------------------------------------------

class BBoxQueryParams(BaseModel):
    """
    Bounding box query — /query/all

    Flat params (not nested BoundingBox) because FastAPI reads
    Query() params as flat key=value pairs, not nested objects.
    Validation mirrors BoundingBox rules.
    """
    dataset: str   = Field(..., description="Registered dataset key (e.g. 'canary', 'kenya')")
    sw_lng:  float = Field(..., ge=-180.0, le=180.0, description="Southwest longitude")
    sw_lat:  float = Field(..., ge=-90.0,  le=90.0,  description="Southwest latitude")
    ne_lng:  float = Field(..., ge=-180.0, le=180.0, description="Northeast longitude")
    ne_lat:  float = Field(..., ge=-90.0,  le=90.0,  description="Northeast latitude")
    limit:   int   = Field(5000, ge=1, le=10000, description="Max features returned")

    @model_validator(mode="after")
    def validate_bbox(self):
        if self.sw_lng >= self.ne_lng:
            raise ValueError("sw_lng must be less than ne_lng")
        if self.sw_lat >= self.ne_lat:
            raise ValueError("sw_lat must be less than ne_lat")

        width  = abs(self.ne_lng - self.sw_lng)
        height = abs(self.ne_lat - self.sw_lat)

        if width > 10:
            raise ValueError(f"Bounding box width ({width:.2f}°) exceeds maximum (10°)")
        if height > 10:
            raise ValueError(f"Bounding box height ({height:.2f}°) exceeds maximum (10°)")

        return self


class H3QueryParams(BaseModel):
    """
    H3 index query — /query/h3

    Preferred query mode — faster than bbox because it hits
    the h3_8/h3_9 column index directly without geometry computation.
    """
    dataset:    str = Field(..., description="Registered dataset key")
    h3_index:   str = Field(
        ...,
        min_length=15,
        max_length=16,
        pattern=r"^[0-9a-f]{15,16}$",
        description="H3 cell index (15–16 character hex string)",
        examples=["87344325effffff"],
    )
    resolution: int = Field(8,    ge=7, le=10,    description="H3 resolution (7–10)")
    limit:      int = Field(5000, ge=1, le=10000, description="Max features returned")


# ---------------------------------------------------------------------------
# Partition params
# ---------------------------------------------------------------------------

class PartitionParams(BaseModel):
    """
    Partition file address — /partitions/{dataset}/{z}/{x}/{y}.parquet

    Validates that x and y are within the valid range for the given zoom level.
    """
    dataset: str = Field(..., description="Registered dataset key")
    z:       int = Field(..., ge=0, le=20, description="Quadtree zoom level")
    x:       int = Field(..., ge=0,        description="Quadtree X coordinate")
    y:       int = Field(..., ge=0,        description="Quadtree Y coordinate")

    @model_validator(mode="after")
    def validate_tile_coordinates(self):
        max_coord = (2 ** self.z) - 1
        if self.x > max_coord:
            raise ValueError(f"x={self.x} exceeds max {max_coord} for zoom {self.z}")
        if self.y > max_coord:
            raise ValueError(f"y={self.y} exceeds max {max_coord} for zoom {self.z}")
        return self
