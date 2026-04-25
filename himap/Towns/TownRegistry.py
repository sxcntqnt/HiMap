"""
Town Registry — HiMap v3.0

Mirrors the DatasetRegistry pattern exactly.
Same register/get/list interface — callers work with both registries
the same way.

Town keys are lowercase, hyphenated:
    "nairobi", "las-palmas", "santa-cruz", "prague"

Linking towns to datasets:
    A town's dataset_key must match a key in the DatasetRegistry.
    The registry does NOT enforce this at register time (the dataset
    may be registered after the town), but to_bbox_params() and
    to_h3_params() will raise if dataset_key is unset.

Adding a new town:
    town_registry.register(
        "mombasa",
        TownBase(
            name="Mombasa",
            lat=-4.0435,
            lng=39.6682,
            lat_extent=0.12,
            lng_extent=0.15,
            dataset_key="kenya",
            country_code="KE",
        )
    )
"""

from typing import Dict, List, Optional
from .TownBase import TownBase


class TownRegistry:
    """
    Registry of all towns known to HiMap.
    Populated at startup. Read-only at query time.
    """

    def __init__(self) -> None:
        self._towns: Dict[str, TownBase] = {}

    def register(self, key: str, town: TownBase) -> None:
        """
        Register a town under the given key.

        Validates the town's spatial fields before accepting.
        Raises ValueError if key already registered.
        """
        if key in self._towns:
            raise ValueError(
                f"Town '{key}' already registered. "
                f"Call deregister() first if replacement is intentional."
            )
        town.validate()
        self._towns[key] = town

    def deregister(self, key: str) -> None:
        """Remove a town. Use in tests and migrations."""
        self._towns.pop(key, None)

    def get(self, key: str) -> TownBase:
        """
        Return town for a registered key.
        Raises ValueError (not KeyError) — callers convert to HTTP 400.
        """
        if key not in self._towns:
            raise ValueError(
                f"Unknown town: '{key}'. "
                f"Registered towns: {sorted(self._towns.keys())}"
            )
        return self._towns[key]

    def list(self) -> List[str]:
        """Sorted list of registered town keys."""
        return sorted(self._towns.keys())

    def list_by_dataset(self, dataset_key: str) -> List[str]:
        """Return all town keys linked to a given dataset."""
        return sorted(
            k for k, t in self._towns.items()
            if t.dataset_key == dataset_key
        )

    def all(self) -> Dict[str, TownBase]:
        """Shallow copy of the full registry."""
        return dict(self._towns)

    def __contains__(self, key: str) -> bool:
        return key in self._towns

    def __len__(self) -> int:
        return len(self._towns)


# ---------------------------------------------------------------------------
# Global registry instance
# Import this everywhere. Do not instantiate TownRegistry directly.
# ---------------------------------------------------------------------------

town_registry = TownRegistry()


# ---------------------------------------------------------------------------
# Registered towns
#
# Extents are half-widths in degrees — bbox is center ± extent.
# Validate: (extent * 2) must not exceed 10° in either axis.
#
# Naming convention: lowercase, hyphenated, no country suffix
#   "nairobi" not "nairobi_ke" or "NairobiKE"
# ---------------------------------------------------------------------------

# — Kenya ——————————————————————————————————————————————————————————————————

town_registry.register(
    "nairobi",
    TownBase(
        name="Nairobi",
        lat=-1.2921,
        lng=36.8219,
        lat_extent=0.18,     # ~40 km north-south
        lng_extent=0.22,     # ~40 km east-west
        dataset_key="kenya",
        country_code="KE",
    )
)

town_registry.register(
    "mombasa",
    TownBase(
        name="Mombasa",
        lat=-4.0435,
        lng=39.6682,
        lat_extent=0.10,
        lng_extent=0.12,
        dataset_key="kenya",
        country_code="KE",
    )
)

town_registry.register(
    "kisumu",
    TownBase(
        name="Kisumu",
        lat=-0.1022,
        lng=34.7617,
        lat_extent=0.08,
        lng_extent=0.10,
        dataset_key="kenya",
        country_code="KE",
    )
)

# — Canary Islands ————————————————————————————————————————————————————————

town_registry.register(
    "las-palmas",
    TownBase(
        name="Las Palmas de Gran Canaria",
        lat=28.1235,
        lng=-15.4366,
        lat_extent=0.10,
        lng_extent=0.12,
        dataset_key="canary",
        country_code="ES",
    )
)

town_registry.register(
    "santa-cruz",
    TownBase(
        name="Santa Cruz de Tenerife",
        lat=28.4636,
        lng=-16.2518,
        lat_extent=0.10,
        lng_extent=0.12,
        dataset_key="canary",
        country_code="ES",
    )
)

# — Unlinked (dataset not yet registered) ————————————————————————————————

town_registry.register(
    "prague",
    TownBase(
        name="Prague",
        lat=50.0755,
        lng=14.4378,
        lat_extent=0.07,    # original x_growth / y_growth converted and corrected
        lng_extent=0.12,
        dataset_key=None,   # no dataset registered yet
        country_code="CZ",
    )
)

# Add new towns here:
#
# town_registry.register(
#     "lagos",
#     TownBase(
#         name="Lagos",
#         lat=6.5244,
#         lng=3.3792,
#         lat_extent=0.15,
#         lng_extent=0.18,
#         dataset_key="lagos",   # register dataset first
#         country_code="NG",
#     )
# )
