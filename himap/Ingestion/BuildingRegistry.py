"""
Building Registry — HiMap v3.0

Separate registry for OpenBuildingMap (OBM) datasets.
These are distinct from OSM data — different schema, different source,
different partitioning scheme (quadkey, not H3 yet).

Schema (OpenBuildingMap Parquet):
    id          BIGINT
    floorspace  VARCHAR
    occupancy   VARCHAR    — 'UNK', 'RES', 'COM', etc.
    relation_id VARCHAR
    quadkey     VARCHAR    — Bing Maps quadkey (hierarchical string prefix)
    last_update TIMESTAMP WITH TIME ZONE
    height      VARCHAR    — 'HBET:1-2', numeric string, or None
    geometry    GEOMETRY   — WKB polygon/multipolygon
    bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
    source      VARCHAR    — 'OSM'

Zoom gate (non-negotiable):
    Buildings are only queryable at zoom >= MIN_BUILDING_ZOOM (14).
    Below this zoom, individual building footprints are not visible
    and querying them is wasteful. The API enforces this gate — clients
    cannot bypass it by passing a low zoom level.

Quadkey filtering (pre-H3):
    Quadkeys are hierarchical prefix strings.
    A zoom-10 quadkey 'XXXXXXXXXX' is a prefix of every zoom-18
    quadkey inside that tile.
    WHERE quadkey LIKE 'XXXXXXXXXX%' is a cheap prefix scan that
    replaces ST_Intersects for coarse filtering.
    After H3 enrichment: WHERE h3_9 = ? will replace this.

Occupancy codes:
    UNK — unknown
    RES — residential
    COM — commercial
    IND — industrial
    CIV — civic/institutional

Height encoding:
    'HBET:1-2'  — height between 1 and 2 stories
    '12.5'      — numeric string (meters)
    None        — unknown
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# Minimum zoom level for building queries — enforced at API boundary
MIN_BUILDING_ZOOM = 14


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class BuildingConfig:
    """
    Configuration for a single OpenBuildingMap dataset partition.

    base_path:      Root path for this building dataset's Parquet files.
                    Local:  "./lake/buildings/ke/"
                    S3:     "s3://us-west-2.opendata.source.coop/tge-labs/openbuildingmap/"

    country:        ISO-2 country code.

    osm_dataset_key: The dataset key in DatasetRegistry this building
                    dataset corresponds to. Used to validate that the
                    caller has access to the right dataset.
                    e.g. "kenya" links buildings to the Kenya OSM dataset.

    quadkey_prefixes: Optional list of quadkey prefixes that cover this
                    region. Used for fast pre-H3 filtering.
                    If None, no prefix filter is applied (full scan).

    zoom_gate:      Minimum zoom level for queries. Defaults to
                    MIN_BUILDING_ZOOM (14). Can be raised per-dataset
                    for very dense urban areas.

    has_height:     Whether height data is available in this partition.
    has_occupancy:  Whether occupancy classification is available.
    """
    country:         str
    base_path:       str
    osm_dataset_key: Optional[str]    = None

    quadkey_prefixes: Optional[List[str]] = None
    zoom_gate:        int              = MIN_BUILDING_ZOOM

    has_height:       bool             = True
    has_occupancy:    bool             = True

    def parquet_glob(self) -> str:
        """Glob pattern for DuckDB read_parquet()."""
        return f"{self.base_path.rstrip('/')}/**/*.parquet"

    def view_name(self, key: str) -> str:
        return f"{key}_buildings_view"

    def quadkey_filter_sql(self, quadkey_prefix: Optional[str] = None) -> str:
        """
        Return a WHERE fragment for quadkey prefix filtering.

        Priority:
            1. Caller-supplied quadkey_prefix (from viewport)
            2. Dataset's quadkey_prefixes (region filter)
            3. No filter (full scan — expensive for large datasets)
        """
        if quadkey_prefix:
            return f"quadkey LIKE '{quadkey_prefix}%'"

        if self.quadkey_prefixes:
            clauses = [f"quadkey LIKE '{p}%'" for p in self.quadkey_prefixes]
            return "(" + " OR ".join(clauses) + ")"

        return "1=1"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class BuildingRegistry:
    """
    Registry of all OpenBuildingMap datasets.
    Same register/get/list interface as DatasetRegistry and TownRegistry.
    """

    def __init__(self) -> None:
        self._datasets: Dict[str, BuildingConfig] = {}

    def register(self, key: str, config: BuildingConfig) -> None:
        if key in self._datasets:
            raise ValueError(
                f"Building dataset '{key}' already registered. "
                f"Call deregister() first if replacement is intentional."
            )
        self._datasets[key] = config

    def deregister(self, key: str) -> None:
        self._datasets.pop(key, None)

    def get(self, key: str) -> BuildingConfig:
        if key not in self._datasets:
            raise ValueError(
                f"Unknown building dataset: '{key}'. "
                f"Registered: {sorted(self._datasets.keys())}"
            )
        return self._datasets[key]

    def list(self) -> List[str]:
        return sorted(self._datasets.keys())

    def for_osm_dataset(self, osm_key: str) -> Optional[str]:
        """
        Return the building dataset key linked to an OSM dataset key.
        Returns None if no building dataset is linked.
        """
        for k, config in self._datasets.items():
            if config.osm_dataset_key == osm_key:
                return k
        return None

    def __contains__(self, key: str) -> bool:
        return key in self._datasets

    def __len__(self) -> int:
        return len(self._datasets)


# ---------------------------------------------------------------------------
# Global instance
# ---------------------------------------------------------------------------

building_registry = BuildingRegistry()

_LAKE_ROOT = os.getenv("HIMAP_LAKE_ROOT", "./lake")
_OBM_ROOT  = os.getenv(
    "HIMAP_OBM_ROOT",
    "s3://us-west-2.opendata.source.coop/tge-labs/openbuildingmap"
)

# Kenya buildings — linked to the 'kenya' OSM dataset
building_registry.register(
    "kenya-buildings",
    BuildingConfig(
        country="KE",
        base_path=os.getenv("HIMAP_BUILDINGS_KENYA", f"{_OBM_ROOT}/"),
        osm_dataset_key="kenya",
        # Quadkey prefixes covering Kenya (zoom 3 quadkeys)
        # Run: SELECT DISTINCT SUBSTR(quadkey, 1, 4) FROM buildings WHERE <kenya bbox>
        # to derive these for your actual data
        quadkey_prefixes=["1223", "1232", "1233"],
        zoom_gate=MIN_BUILDING_ZOOM,
        has_height=True,
        has_occupancy=True,
    )
)

# Canary Islands buildings
building_registry.register(
    "canary-buildings",
    BuildingConfig(
        country="ES",
        base_path=os.getenv("HIMAP_BUILDINGS_CANARY", f"{_OBM_ROOT}/"),
        osm_dataset_key="canary",
        quadkey_prefixes=["1202", "1203"],
        zoom_gate=MIN_BUILDING_ZOOM,
        has_height=True,
        has_occupancy=True,
    )
)

# Add new building datasets here:
#
# building_registry.register(
#     "lagos-buildings",
#     BuildingConfig(
#         country="NG",
#         base_path=os.getenv("HIMAP_BUILDINGS_LAGOS", f"{_OBM_ROOT}/"),
#         osm_dataset_key="lagos",
#         quadkey_prefixes=["1122"],
#     )
# )
