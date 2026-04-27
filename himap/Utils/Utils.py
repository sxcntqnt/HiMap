"""
Spatial Utilities — HiMap v3.0

Accurate coordinate math for a multi-latitude system.

Critical projection note:
    All ground distances in this system must be latitude-corrected.
    The zoom_levels.txt ground resolutions are at the EQUATOR.
    At latitude φ: actual_ground_res = equatorial_res × cos(φ)

    Kenya    (lat ≈ -1.3°): cos correction ≈ 0.9997 — nearly equatorial, negligible
    Canary   (lat ≈ 28.1°): cos correction ≈ 0.8813 — ~12% smaller ground per pixel
    Prague   (lat ≈ 50.1°): cos correction ≈ 0.6406 — ~36% smaller ground per pixel

    Ignoring this makes distance estimates wrong by up to 36% at mid-latitudes.

WGS84 approximations used:
    1° latitude  ≈ 110,574 m   (nearly constant — ellipsoid effect <1%)
    1° longitude = 111,320 × cos(lat) m

Removed from original Utils.py:
    - Google Static Maps API helpers (dead — system serves Parquet, not tiles)
    - PIL image utilities (dead)
    - get_growth_ratio_to_Prague (obsolete — TownBase carries extents directly)
"""

import math
from typing import Optional, Tuple

from geopy import distance as geopy_distance

# WGS84 degree-to-meter constants
_LAT_METERS_PER_DEGREE = 110_574.0          # nearly constant globally
_LNG_METERS_PER_DEGREE_AT_EQUATOR = 111_320.0


# ---------------------------------------------------------------------------
# Degree ↔ Meter conversions (latitude-corrected)
# ---------------------------------------------------------------------------

def degrees_to_meters_lng(lat: float, degrees: float) -> float:
    """
    Convert longitude degrees to meters at a given latitude.

    The longitude scale shrinks toward the poles:
        meters = degrees × 111,320 × cos(lat)

    Args:
        lat:     Latitude in decimal degrees (determines scale factor)
        degrees: Longitude span in degrees

    Returns:
        Distance in meters
    """
    return degrees * _LNG_METERS_PER_DEGREE_AT_EQUATOR * math.cos(math.radians(lat))


def degrees_to_meters_lat(degrees: float) -> float:
    """
    Convert latitude degrees to meters.
    Nearly constant globally — ellipsoid correction <1%.

    Args:
        degrees: Latitude span in degrees

    Returns:
        Distance in meters
    """
    return degrees * _LAT_METERS_PER_DEGREE


def meters_to_degrees_lng(lat: float, meters: float) -> float:
    """
    Convert meters to longitude degrees at a given latitude.

    Args:
        lat:    Latitude in decimal degrees
        meters: Distance in meters

    Returns:
        Longitude span in degrees
    """
    cos_lat = math.cos(math.radians(lat))
    if cos_lat < 1e-10:
        raise ValueError("Cannot convert longitude degrees at or near the poles")
    return meters / (_LNG_METERS_PER_DEGREE_AT_EQUATOR * cos_lat)


def meters_to_degrees_lat(meters: float) -> float:
    """
    Convert meters to latitude degrees.

    Args:
        meters: Distance in meters

    Returns:
        Latitude span in degrees
    """
    return meters / _LAT_METERS_PER_DEGREE


# ---------------------------------------------------------------------------
# Latitude correction factor
# ---------------------------------------------------------------------------

def latitude_scale_factor(lat: float) -> float:
    """
    Return the longitudinal scale factor at a given latitude.

    This is the ratio by which ground distances shrink relative to the equator:
        factor = cos(lat)

    Use cases:
        - Correcting equatorial ground resolution to actual ground resolution
        - Scaling extent degrees to consistent metric distances across towns

    Examples:
        Kenya  (lat -1.3°): 0.9997  — effectively equatorial
        Canary (lat 28.1°): 0.8813  — 12% correction needed
        Prague (lat 50.1°): 0.6406  — 36% correction needed
    """
    return math.cos(math.radians(lat))


def growth_ratio(lat1: float, lat2: float, offset_deg: float = 0.01) -> float:
    """
    Compute the distance ratio between two latitudes for the same degree offset.

    Answers: "how many meters does 0.01° of longitude represent at lat1
    compared to lat2?"

    This is the corrected version of the original _get_growth_ratio.
    Uses geopy for accuracy (accounts for WGS84 ellipsoid).

    Args:
        lat1:       Reference latitude (e.g. town center)
        lat2:       Comparison latitude
        offset_deg: Longitude offset to measure (default 0.01°)

    Returns:
        ratio = distance_at_lat1 / distance_at_lat2
        > 1.0 means lat1 is closer to equator (larger ground per degree)
        < 1.0 means lat1 is further from equator (smaller ground per degree)
    """
    d1 = geopy_distance.distance((lat1, 0), (lat1, offset_deg)).meters
    d2 = geopy_distance.distance((lat2, 0), (lat2, offset_deg)).meters
    return d1 / d2


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def haversine_distance(
    lat1: float, lng1: float,
    lat2: float, lng2: float,
) -> float:
    """
    Great-circle distance between two points in meters.

    Uses the haversine formula — accurate for distances up to ~1000 km.
    For higher accuracy use geopy.distance.geodesic (WGS84 ellipsoid).

    Args:
        lat1, lng1: First point in decimal degrees
        lat2, lng2: Second point in decimal degrees

    Returns:
        Distance in meters
    """
    R = 6_371_000.0  # Earth radius in meters

    φ1 = math.radians(lat1)
    φ2 = math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lng2 - lng1)

    a = (math.sin(Δφ / 2) ** 2
         + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2)

    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def geodesic_distance(
    lat1: float, lng1: float,
    lat2: float, lng2: float,
) -> float:
    """
    WGS84 ellipsoid distance between two points in meters.

    More accurate than haversine for long distances.
    Uses geopy's geodesic implementation.
    """
    return geopy_distance.geodesic((lat1, lng1), (lat2, lng2)).meters


# ---------------------------------------------------------------------------
# Bounding box geometry
# ---------------------------------------------------------------------------

def bbox_area_km2(
    sw_lng: float, sw_lat: float,
    ne_lng: float, ne_lat: float,
) -> float:
    """
    Approximate area of a bounding box in km².

    Uses equirectangular approximation at the center latitude.
    Accurate enough for viewport sizing (error <1% for boxes <10°).

    Args:
        sw_lng, sw_lat: South-west corner
        ne_lng, ne_lat: North-east corner

    Returns:
        Area in km²
    """
    center_lat = (sw_lat + ne_lat) / 2.0
    width_m    = degrees_to_meters_lng(center_lat, ne_lng - sw_lng)
    height_m   = degrees_to_meters_lat(ne_lat - sw_lat)
    return (width_m * height_m) / 1_000_000.0


def bbox_dimensions_m(
    sw_lng: float, sw_lat: float,
    ne_lng: float, ne_lat: float,
) -> Tuple[float, float]:
    """
    Return the (width_m, height_m) of a bounding box in meters.

    Width is latitude-corrected. Height is constant.
    """
    center_lat = (sw_lat + ne_lat) / 2.0
    width_m  = degrees_to_meters_lng(center_lat, ne_lng - sw_lng)
    height_m = degrees_to_meters_lat(ne_lat - sw_lat)
    return width_m, height_m


def extent_degrees_from_meters(
    lat: float,
    width_m: float,
    height_m: float,
) -> Tuple[float, float]:
    """
    Convert a desired viewport size in meters to lat/lng extents (half-widths).

    Used to set TownBase.lat_extent and lng_extent from a known metric size.

    Args:
        lat:      Center latitude (determines longitude scale)
        width_m:  Desired viewport width in meters
        height_m: Desired viewport height in meters

    Returns:
        (lat_extent, lng_extent) in degrees — half-widths for TownBase
    """
    lat_extent = meters_to_degrees_lat(height_m / 2)
    lng_extent = meters_to_degrees_lng(lat, width_m / 2)
    return lat_extent, lng_extent


# ---------------------------------------------------------------------------
# H3 utilities
# ---------------------------------------------------------------------------

def h3_cells_in_bbox(
    sw_lng: float, sw_lat: float,
    ne_lng: float, ne_lat: float,
    resolution: int,
) -> int:
    """
    Estimate the number of H3 cells that cover a bounding box at a given resolution.

    Uses the H3 average cell area for the resolution.
    Actual count may differ slightly due to hexagon geometry at boundaries.

    Args:
        sw_lng, sw_lat: South-west corner
        ne_lng, ne_lat: North-east corner
        resolution:     H3 resolution (0–15)

    Returns:
        Estimated H3 cell count
    """
    # H3 average cell areas in km² (from H3 specification)
    H3_CELL_AREAS_KM2 = {
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
    if resolution not in H3_CELL_AREAS_KM2:
        raise ValueError(f"H3 resolution must be 0–15, got {resolution}")

    box_area   = bbox_area_km2(sw_lng, sw_lat, ne_lng, ne_lat)
    cell_area  = H3_CELL_AREAS_KM2[resolution]
    return max(1, round(box_area / cell_area))


def h3_edge_length_m(resolution: int) -> float:
    """
    Return the average H3 hexagon edge length in meters for a resolution.

    From H3 specification. Used to understand spatial granularity.
    """
    # Average edge lengths in meters (from H3 specification)
    H3_EDGE_LENGTHS_M = {
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
    if resolution not in H3_EDGE_LENGTHS_M:
        raise ValueError(f"H3 resolution must be 0–15, got {resolution}")
    return H3_EDGE_LENGTHS_M[resolution]
