"""
HiMap Towns — Spatial viewport registry

Provides named town viewports with center coordinates, bounding boxes,
H3 indices, and dataset linkage.

Usage:
    from himap.Towns import town_registry

    # Get a town
    nairobi = town_registry.get("nairobi")

    # Get bbox params ready for /query/all
    params = nairobi.to_bbox_params()
    # → {"dataset": "kenya", "sw_lng": ..., "sw_lat": ..., "ne_lng": ..., "ne_lat": ..., "limit": 5000}

    # Get H3 index for town center
    idx = nairobi.h3_index(resolution=8)

    # List all towns linked to a dataset
    kenya_towns = town_registry.list_by_dataset("kenya")
    # → ["kisumu", "mombasa", "nairobi"]

    # List all registered towns
    all_towns = town_registry.list()
"""

from .TownBase import TownBase
from .TownRegistry import TownRegistry, town_registry
from .Prague import Prague

__all__ = [
    "TownBase",
    "TownRegistry",
    "town_registry",
    "Prague",
]
