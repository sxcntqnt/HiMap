"""
TownBase — HiMap v3.0

A Town is a named spatial viewport with a center coordinate and extent.
It bridges human geography ("Nairobi") to the machine primitives the
query layer needs (bbox tuple, H3 index, dataset key).

Design rules:
    - A Town never queries the database itself
    - A Town produces inputs for ViewGenerator / DuckLakeService callers
    - A Town links to exactly one registered dataset key (or None if unregistered)
    - Extent is stored as half-width/half-height in degrees — the bbox is
      symmetric around the center: center ± extent

Coordinate convention (explicit, no ambiguity):
    lat  = latitude  (north/south, -90 to +90)
    lng  = longitude (east/west, -180 to +180)
"""

from dataclasses import dataclass, field
from typing import Optional, Tuple
import math


@dataclass
class TownBase:
    """
    Base class for all registered towns.

    Attributes:
        name:        Human-readable town name
        lat:         Center latitude  (decimal degrees, -90 to +90)
        lng:         Center longitude (decimal degrees, -180 to +180)
        lat_extent:  Half-height of default viewport in degrees
        lng_extent:  Half-width  of default viewport in degrees
        dataset_key: Registered dataset key this town belongs to.
                     None = town exists but dataset not yet registered.
        country_code: ISO-2 country code
    """
    name:        str
    lat:         float
    lng:         float
    lat_extent:  float
    lng_extent:  float
    dataset_key: Optional[str] = None
    country_code: Optional[str] = None

    # ------------------------------------------------------------------
    # Spatial primitives
    # ------------------------------------------------------------------

    def bbox(self) -> Tuple[float, float, float, float]:
        """
        Return bounding box as (sw_lng, sw_lat, ne_lng, ne_lat).

        Matches the parameter order expected by:
            - BBoxQueryParams (sw_lng, sw_lat, ne_lng, ne_lat)
            - ST_MakeEnvelope(sw_lng, sw_lat, ne_lng, ne_lat)
            - ViewGenerator.bbox_query() parameters
        """
        return (
            self.lng - self.lng_extent,   # sw_lng
            self.lat - self.lat_extent,   # sw_lat
            self.lng + self.lng_extent,   # ne_lng
            self.lat + self.lat_extent,   # ne_lat
        )

    def bbox_width_deg(self) -> float:
        """Full width of the viewport in degrees longitude."""
        return self.lng_extent * 2

    def bbox_height_deg(self) -> float:
        """Full height of the viewport in degrees latitude."""
        return self.lat_extent * 2

    def bbox_area_km2(self) -> float:
        """
        Approximate area of the bounding box in km².
        Uses equirectangular approximation — sufficient for viewport sizing.
        """
        lat_km = self.lat_extent * 2 * 111.32
        lng_km = self.lng_extent * 2 * 111.32 * math.cos(math.radians(self.lat))
        return lat_km * lng_km

    def h3_index(self, resolution: int = 8) -> str:
        """
        Return the H3 cell index for the town center at a given resolution.

        Uses the printf pattern consistent with the Partitioner pipeline:
            printf('%x', h3_latlng_to_cell(lat, lng, res)::BIGINT)

        Requires h3 Python package. Returns None if unavailable.
        """
        try:
            import h3
            return h3.latlng_to_cell(self.lat, self.lng, resolution)
        except ImportError:
            return None

    # ------------------------------------------------------------------
    # Query param factories
    # ------------------------------------------------------------------

    def to_bbox_params(self, limit: int = 5000) -> dict:
        """
        Return a dict ready to pass to the /query/all endpoint or
        BBoxQueryParams — all required fields included.

        Raises ValueError if dataset_key is not set.
        """
        if not self.dataset_key:
            raise ValueError(
                f"Town '{self.name}' has no dataset_key. "
                f"Register the dataset first, then set town.dataset_key."
            )
        sw_lng, sw_lat, ne_lng, ne_lat = self.bbox()
        return {
            "dataset": self.dataset_key,
            "sw_lng":  sw_lng,
            "sw_lat":  sw_lat,
            "ne_lng":  ne_lng,
            "ne_lat":  ne_lat,
            "limit":   limit,
        }

    def to_h3_params(self, resolution: int = 8, limit: int = 5000) -> dict:
        """
        Return a dict ready to pass to the /query/h3 endpoint or
        H3QueryParams.

        Raises ValueError if dataset_key is not set or h3 unavailable.
        """
        if not self.dataset_key:
            raise ValueError(
                f"Town '{self.name}' has no dataset_key."
            )
        idx = self.h3_index(resolution)
        if idx is None:
            raise RuntimeError("h3 package not installed — cannot generate H3 index.")
        return {
            "dataset":    self.dataset_key,
            "h3_index":   idx,
            "resolution": resolution,
            "limit":      limit,
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Raise ValueError if any field is out of range.
        Called by TownRegistry.register() before accepting a town.
        """
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"{self.name}: lat {self.lat} out of range [-90, 90]")
        if not (-180 <= self.lng <= 180):
            raise ValueError(f"{self.name}: lng {self.lng} out of range [-180, 180]")
        if self.lat_extent <= 0:
            raise ValueError(f"{self.name}: lat_extent must be positive")
        if self.lng_extent <= 0:
            raise ValueError(f"{self.name}: lng_extent must be positive")

        sw_lng, sw_lat, ne_lng, ne_lat = self.bbox()
        width  = ne_lng - sw_lng
        height = ne_lat - sw_lat

        if width > 10:
            raise ValueError(
                f"{self.name}: bbox width {width:.2f}° exceeds 10° limit. "
                f"Reduce lng_extent."
            )
        if height > 10:
            raise ValueError(
                f"{self.name}: bbox height {height:.2f}° exceeds 10° limit. "
                f"Reduce lat_extent."
            )

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        sw_lng, sw_lat, ne_lng, ne_lat = self.bbox()
        return (
            f"<Town: {self.name} | "
            f"center=({self.lat:.4f}, {self.lng:.4f}) | "
            f"bbox=({sw_lng:.4f},{sw_lat:.4f} → {ne_lng:.4f},{ne_lat:.4f}) | "
            f"dataset={self.dataset_key or 'unlinked'}>"
        )
