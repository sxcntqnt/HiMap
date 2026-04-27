"""
Zoom Level Service — HiMap v3.0

Two problems with the original Zoom.py:

    1. Ground resolution values are at the EQUATOR only.
       At latitude φ: actual_res = equatorial_res × cos(φ)
       Without this correction:
           Canary Islands (28°): 12% error
           Prague         (50°): 36% error

    2. No H3 resolution mapping.
       The system uses H3 as its spatial index. Knowing which H3
       resolution corresponds to which zoom level is critical for:
           - deciding which h3_col to query (h3_7, h3_8, h3_9, h3_10)
           - computing entropy at the right granularity
           - choosing partition resolution in the Partitioner

This module provides both, plus the bridge between them.

Zoom levels in this system:
    Quadtree z/x/y (partition addressing): zoom 10 (default partition level)
    Map render zoom (client-side):         0–20 (standard Web Mercator)
    H3 resolution (spatial index):         7–10 (stored in Parquet)

The three are related but independent. This module maps between all three.

Semantic zoom contract (from HiMap Contract Layer v1):
    0–6   continental
    7–9   metro
    10–12 city
    13–15 neighborhood
    16+   street
"""

import math
from pathlib import Path
from typing import Dict, Optional, Tuple

from .Utils import degrees_to_meters_lng, latitude_scale_factor


# ---------------------------------------------------------------------------
# H3 resolution metadata (from H3 specification — do not change)
# ---------------------------------------------------------------------------

# Average cell area in km²
H3_CELL_AREAS_KM2: Dict[int, float] = {
    0:  4_250_546.848,
    1:    607_220.978,
    2:     86_745.854,
    3:     12_392.264,
    4:      1_770.324,
    5:        252.903,
    6:         36.129,
    7:          5.162,
    8:          0.737,
    9:          0.105,
    10:         0.015,
    11:         0.00216,
    12:         0.000309,
    13:         0.0000441,
    14:         0.0000063,
    15:         0.0000009,
}

# Average edge length in meters
H3_EDGE_LENGTHS_M: Dict[int, float] = {
    0:  1_107_712.591,
    1:    418_676.005,
    2:    158_244.655,
    3:     59_800.888,
    4:     22_606.379,
    5:      8_544.408,
    6:      3_229.482,
    7:      1_220.629,
    8:        461.354,
    9:        174.375,
    10:        65.907,
    11:        24.910,
    12:         9.415,
    13:         3.559,
    14:         1.348,
    15:         0.509,
}

# H3 resolution → map zoom range (lower, upper inclusive)
# Based on matching H3 cell diameter to map ground resolution at equator.
# These are the zoom levels at which an H3 cell at that resolution
# occupies roughly 1–4 screen tiles — the natural query granularity.
H3_TO_ZOOM_RANGE: Dict[int, Tuple[int, int]] = {
    7:  (9,  10),   # ~5 km²   — metro/city partition scale
    8:  (11, 12),   # ~0.7 km² — neighborhood / district
    9:  (13, 14),   # ~0.1 km² — street block
    10: (15, 16),   # ~15k m²  — fine-grained (building level)
}

# Map zoom → preferred H3 resolution for querying
# At each map zoom level, use this H3 resolution for filtering/indexing.
# Aligned with the Partitioner's h3_7 through h3_10 columns.
ZOOM_TO_H3_RESOLUTION: Dict[int, int] = {
    0:  7,
    1:  7,
    2:  7,
    3:  7,
    4:  7,
    5:  7,
    6:  7,
    7:  7,
    8:  7,
    9:  7,
    10: 7,
    11: 8,
    12: 8,
    13: 9,
    14: 9,
    15: 10,
    16: 10,
    17: 10,
    18: 10,
    19: 10,
    20: 10,
}

# Semantic zoom labels (from HiMap Contract Layer v1)
SEMANTIC_ZOOM: Dict[Tuple[int, int], str] = {
    (0,  6):  "continental",
    (7,  9):  "metro",
    (10, 12): "city",
    (13, 15): "neighborhood",
    (16, 20): "street",
}


# ---------------------------------------------------------------------------
# ZoomLevel — latitude-aware ground resolution
# ---------------------------------------------------------------------------

class ZoomLevel:
    """
    Latitude-aware zoom level service.

    Ground resolution values in zoom_levels.txt are at the equator.
    All methods in this class correct for latitude using cos(φ).

    Usage:
        zl = ZoomLevel.from_file(Path("zoom_levels.txt"))

        # Ground resolution at zoom 12 in Nairobi (lat -1.3°)
        res = zl.ground_resolution_m(zoom=12, lat=-1.2921)
        # → 38.22 m/pixel (nearly equatorial)

        # Ground resolution at zoom 12 in Prague (lat 50.1°)
        res = zl.ground_resolution_m(zoom=12, lat=50.0755)
        # → 24.52 m/pixel (36% smaller than equatorial)

        # Which H3 resolution to use at zoom 12?
        h3_res = zl.h3_resolution_for_zoom(12)
        # → 8

        # What zoom level matches H3 res 9?
        lo, hi = zl.zoom_range_for_h3(9)
        # → (13, 14)
    """

    def __init__(
        self,
        zoom_data: Dict[int, float],
        min_zoom: int,
        max_zoom: int,
    ):
        self._data    = zoom_data      # zoom → equatorial ground res (m/px)
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, file_path: Path) -> "ZoomLevel":
        """
        Load from zoom_levels.txt format:
            19 : 1128.497220
            18 : 2256.994440
            ...
        """
        data = {}
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) != 2:
                    continue
                zoom  = int(parts[0].strip())
                value = float(parts[1].strip())
                data[zoom] = value

        if not data:
            raise ValueError(f"No zoom data loaded from {file_path}")

        return cls(
            zoom_data=data,
            min_zoom=min(data.keys()),
            max_zoom=max(data.keys()),
        )

    @classmethod
    def from_dict(cls, data: Dict[int, float]) -> "ZoomLevel":
        """Construct from a dictionary directly — useful for testing."""
        return cls(
            zoom_data=data,
            min_zoom=min(data.keys()),
            max_zoom=max(data.keys()),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_zoom(self, zoom: int) -> None:
        if not (self.min_zoom <= zoom <= self.max_zoom):
            raise ValueError(
                f"Zoom {zoom} out of range [{self.min_zoom}, {self.max_zoom}]"
            )

    # ------------------------------------------------------------------
    # Ground resolution (latitude-corrected)
    # ------------------------------------------------------------------

    def ground_resolution_equatorial(self, zoom: int) -> float:
        """
        Equatorial ground resolution in meters per pixel at a given zoom.
        Raw value from zoom_levels.txt — not latitude-corrected.
        Use ground_resolution_m() for accurate per-location values.
        """
        self._validate_zoom(zoom)
        return self._data[zoom]

    def ground_resolution_m(self, zoom: int, lat: float) -> float:
        """
        Latitude-corrected ground resolution in meters per pixel.

        Formula:
            resolution(zoom, lat) = equatorial_res(zoom) × cos(lat)

        Args:
            zoom: Web Mercator zoom level
            lat:  Latitude in decimal degrees

        Returns:
            Ground resolution in meters per pixel at this zoom and latitude
        """
        self._validate_zoom(zoom)
        return self._data[zoom] * latitude_scale_factor(lat)

    def ground_resolution_ratio(
        self,
        zoom1: int,
        zoom2: int,
        lat: Optional[float] = None,
    ) -> float:
        """
        Ratio of ground resolution at zoom1 to zoom2.

        If lat is provided, both resolutions are latitude-corrected
        (the cos factor cancels, so the ratio is latitude-independent —
        but the method accepts lat for API consistency).

        Args:
            zoom1: Numerator zoom level
            zoom2: Denominator zoom level
            lat:   Optional latitude (does not affect the ratio)

        Returns:
            ground_res(zoom1) / ground_res(zoom2)
        """
        self._validate_zoom(zoom1)
        self._validate_zoom(zoom2)
        return self._data[zoom1] / self._data[zoom2]

    # ------------------------------------------------------------------
    # Zoom ↔ H3 resolution mapping
    # ------------------------------------------------------------------

    def h3_resolution_for_zoom(self, zoom: int) -> int:
        """
        Return the preferred H3 resolution for a given map zoom level.

        This determines which h3_col to use for filtering:
            h3_7 at zoom 0–10  (metro/city scale)
            h3_8 at zoom 11–12 (neighborhood)
            h3_9 at zoom 13–14 (street block)
            h3_10 at zoom 15+  (fine-grained)

        Constrained to resolutions 7–10 — the columns stored in Parquet.
        """
        zoom = max(self.min_zoom, min(self.max_zoom, zoom))
        return ZOOM_TO_H3_RESOLUTION.get(zoom, 7)

    def zoom_range_for_h3(self, h3_resolution: int) -> Tuple[int, int]:
        """
        Return the (min_zoom, max_zoom) range at which a given H3 resolution
        is the natural query granularity.

        Args:
            h3_resolution: H3 resolution (7–10 for stored columns)

        Returns:
            (min_zoom, max_zoom) inclusive
        """
        if h3_resolution not in H3_TO_ZOOM_RANGE:
            raise ValueError(
                f"H3 resolution {h3_resolution} not in stored range 7–10. "
                f"Valid: {sorted(H3_TO_ZOOM_RANGE.keys())}"
            )
        return H3_TO_ZOOM_RANGE[h3_resolution]

    def h3_column_for_zoom(self, zoom: int) -> str:
        """
        Return the Parquet column name to filter on at a given zoom level.

        Directly usable in DuckDB queries:
            WHERE {col} = ?

        Returns:
            One of: 'h3_7', 'h3_8', 'h3_9', 'h3_10'
        """
        return f"h3_{self.h3_resolution_for_zoom(zoom)}"

    # ------------------------------------------------------------------
    # Semantic zoom
    # ------------------------------------------------------------------

    def semantic_level(self, zoom: int) -> str:
        """
        Return the semantic zoom label for a given zoom level.

        From the HiMap Contract Layer v1:
            0–6:   continental
            7–9:   metro
            10–12: city
            13–15: neighborhood
            16+:   street
        """
        for (lo, hi), label in SEMANTIC_ZOOM.items():
            if lo <= zoom <= hi:
                return label
        return "street"  # above 20

    # ------------------------------------------------------------------
    # Pixel ↔ degree conversions (at a specific zoom and latitude)
    # ------------------------------------------------------------------

    def pixels_to_degrees_lng(self, zoom: int, lat: float, pixels: int) -> float:
        """
        Convert a pixel count to longitude degrees at a given zoom and latitude.

        Useful for computing how much of the world a viewport covers.
        """
        res_m_per_px = self.ground_resolution_m(zoom, lat)
        meters = res_m_per_px * pixels
        # meters → degrees longitude at this latitude
        return meters / (111_320.0 * latitude_scale_factor(lat))

    def pixels_to_degrees_lat(self, zoom: int, lat: float, pixels: int) -> float:
        """
        Convert a pixel count to latitude degrees at a given zoom.

        Latitude scale is nearly constant — cos correction not needed.
        """
        res_m_per_px = self.ground_resolution_m(zoom, lat)
        meters = res_m_per_px * pixels
        return meters / 110_574.0

    def viewport_extent_degrees(
        self,
        zoom: int,
        lat: float,
        viewport_width_px: int,
        viewport_height_px: int,
    ) -> Tuple[float, float]:
        """
        Return (lat_extent, lng_extent) in degrees for a viewport at a zoom level.

        These are HALF-widths — the same convention used by TownBase.

        Args:
            zoom:               Map zoom level
            lat:                Center latitude
            viewport_width_px:  Viewport width in pixels
            viewport_height_px: Viewport height in pixels

        Returns:
            (lat_extent, lng_extent) — half-widths in degrees

        Example:
            A 1280×720 viewport at zoom 12 in Nairobi:
            → (0.171°, 0.209°) approximately
        """
        lat_extent = self.pixels_to_degrees_lat(zoom, lat, viewport_height_px // 2)
        lng_extent = self.pixels_to_degrees_lng(zoom, lat, viewport_width_px  // 2)
        return lat_extent, lng_extent

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def describe(self, zoom: int, lat: float) -> dict:
        """
        Return a full description of a zoom level at a given latitude.
        Useful for debugging and API introspection.
        """
        self._validate_zoom(zoom)
        h3_res = self.h3_resolution_for_zoom(zoom)
        return {
            "zoom":                      zoom,
            "lat":                       lat,
            "semantic_level":            self.semantic_level(zoom),
            "ground_resolution_equatorial_m": self._data[zoom],
            "ground_resolution_corrected_m":  round(self.ground_resolution_m(zoom, lat), 3),
            "latitude_scale_factor":     round(latitude_scale_factor(lat), 6),
            "h3_resolution":             h3_res,
            "h3_column":                 f"h3_{h3_res}",
            "h3_edge_length_m":          H3_EDGE_LENGTHS_M[h3_res],
            "h3_cell_area_km2":          H3_CELL_AREAS_KM2[h3_res],
        }


# ---------------------------------------------------------------------------
# Module-level convenience instance
# Loads from the standard zoom_levels.txt path relative to this file.
# Import zoom_levels everywhere — do not instantiate ZoomLevel directly.
# ---------------------------------------------------------------------------

_ZOOM_FILE = Path(__file__).parent / "zoom_levels.txt"

if _ZOOM_FILE.exists():
    zoom_levels = ZoomLevel.from_file(_ZOOM_FILE)
else:
    # Fallback: built-in values from the spec (identical to zoom_levels.txt)
    zoom_levels = ZoomLevel.from_dict({
        19: 1_128.497220,
        18: 2_256.994440,
        17: 4_513.988880,
        16: 9_027.977761,
        15: 18_055.955520,
        14: 36_111.911040,
        13: 72_223.822090,
        12: 144_447.644200,
        11: 288_895.288400,
        10: 577_790.576700,
        9:  1_155_581.153000,
        8:  2_311_162.307000,
        7:  4_622_324.614000,
        6:  9_244_649.227000,
        5:  18_489_298.450000,
        4:  36_978_596.910000,
        3:  73_957_193.820000,
        2:  147_914_387.600000,
        1:  295_828_775.300000,
        0:  591_657_550.500000,
    })
