"""
Curated town registrations — HiMap v3.0

Hand-picked, hand-tuned town entries — as opposed to the bulk-generated
ones under Towns/towns/ (see auto_registry.py). These register first and
win on key collisions.

Lives in himap/Registry/ — a top-level package, sibling of Towns/, not
nested inside it. That's a deliberate choice: this is where any other
registry's registration data (DataRegistry, BuildingRegistry) could
move to later, not just Towns'.
"""

from ..Towns.TownBase import TownBase
from ..Towns.TownRegistry import TownRegistry


def register_curated(registry: TownRegistry) -> None:
    """
    Register the hand-picked towns. Call once, at Towns package import
    time (see Towns/__init__.py) — calling twice will raise on the
    second call, same as any other TownRegistry.register() collision.
    """

    # — Kenya ——————————————————————————————————————————————————————————

    registry.register(
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

    registry.register(
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

    registry.register(
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

    # — Canary Islands ————————————————————————————————————————————————

    registry.register(
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

    registry.register(
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

    # — Unlinked (dataset not yet registered) ————————————————————————

    registry.register(
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

    # Add new curated towns here — for anything that needs hand-tuned
    # extents or isn't in towns.txt. Everything else should go through
    # generate_towns.py instead and be picked up by auto_registry.py.
