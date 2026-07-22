"""
HiMap Towns — Spatial viewport registry

Provides named town viewports with center coordinates, bounding boxes,
H3 indices, and dataset linkage.

Population: town_registry starts empty (see TownRegistry.py) and is
filled here, in order:
    1. register_curated()   — hand-picked entries (wins collisions)
    2. register_generated() — bulk-discovered from Towns/towns/*.py,
                               skipping any key curated already claimed

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
    # → ["kisumu", "mombasa", "nairobi", ... plus every other generated Kenyan town]

    # List all registered towns
    all_towns = town_registry.list()
"""

import logging

from .TownBase import TownBase
from .TownRegistry import TownRegistry, town_registry
from .Prague import Prague
from ..Registry import register_curated, register_generated

logger = logging.getLogger(__name__)

register_curated(town_registry)
_generated = register_generated(town_registry)
logger.info(
    f"Towns registry populated: {len(town_registry)} total "
    f"({len(town_registry) - len(_generated)} curated, {len(_generated)} generated)"
)

__all__ = [
    "TownBase",
    "TownRegistry",
    "town_registry",
    "Prague",
    "register_curated",
    "register_generated",
]
