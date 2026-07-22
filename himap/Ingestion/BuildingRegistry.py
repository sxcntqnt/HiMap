"""
Building Registry — HiMap v3.0

Separate registry for OpenBuildingMap (OBM) datasets.
These are distinct from OSM data — different schema, different source,
different partitioning scheme (quadkey, transitioning to H3).

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
    source      VARCHAR    — 'OSM', 'Google', 'Microsoft', etc.

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
    After H3 enrichment: WHERE h3_9 = ? will replace this — see
    `h3_enriched` below, which flips the routes layer over to that path.

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
from typing import Dict, List, Optional, Tuple


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

    country_filter / bbox:
                    Compatibility fields so this config can be logged and
                    handled by the same code path as DatasetConfig (OSM)
                    in partition_data.py. Buildings datasets are already
                    scoped by `base_path` + `quadkey_prefixes`, so these
                    are typically left as None — they exist purely so
                    `main()` doesn't need an isinstance branch just to
                    print a log line.

    dataset_kind:   Marker used by partition_data.py to decide how to
                    materialize the source before calling Partitioner.
                    Always "buildings" for this registry.

    h3_enriched:    Flip this to True once `partition_data.py` has been
                    run for this dataset key and the enriched, H3-
                    partitioned output exists. The routes layer reads
                    this flag to decide whether it can query by
                    `h3_9 = ?` or must fall back to the pre-H3
                    bbox/quadkey path.
    """
    country:         str
    base_path:       str
    osm_dataset_key: Optional[str]    = None

    quadkey_prefixes: Optional[List[str]] = None
    zoom_gate:        int              = MIN_BUILDING_ZOOM

    has_height:       bool             = True
    has_occupancy:    bool             = True

    # --- compatibility with the OSM DatasetConfig / Partitioner CLI ---
    country_filter:   Optional[str]           = None
    bbox:             Optional[Tuple[float, float, float, float]] = None
    dataset_kind:     str                     = "buildings"
    h3_enriched:      bool                    = False

    # UTM EPSG code used by the partitioner to compute true area_m2 /
    # perimeter_m (ST_Transform target). Pick the zone that covers this
    # dataset's region — ST_Area on raw WGS84 coordinates gives degrees²,
    # not meters, so this must be set per dataset rather than assumed.
    utm_epsg:         int                     = 32637  # UTM 37N — Kenya default

    def parquet_glob(self) -> str:
        """Glob pattern for DuckDB read_parquet() over the RAW source
        (pre-enrichment OpenBuildingMap parquet at base_path)."""
        return f"{self.base_path.rstrip('/')}/**/*.parquet"

    def enriched_parquet_glob(self, lake_root: Optional[str] = None) -> str:
        """
        Glob pattern over the ENRICHED, H3-partitioned output that
        partition_data.py writes for this dataset.

        Must match Partitioner.run_pipeline's output_path for building
        datasets: {lake_root}/{country_lower}/buildings/ — nested under
        a 'buildings' subdirectory specifically so it can never collide
        with the OSM dataset output at {lake_root}/{country_lower}/,
        even when both share the same country code.
        """
        root = lake_root or os.getenv("HIMAP_LAKE_ROOT", "./lake")
        return f"{root.rstrip('/')}/{self.country.lower()}/buildings/**/*.parquet"

    def view_name(self, key: str) -> str:
        """Raw pass-through view name (pre-enrichment source)."""
        return f"{key}_buildings_view"

    def partitioned_view_name(self, key: str) -> str:
        """Pass-through view name over the enriched/partitioned lake output."""
        return f"{key}_buildings_partitioned"

    def enriched_view_name(self, key: str) -> str:
        """View name for the enriched tier — partitioned view + derived geometry columns."""
        return f"{key}_buildings_enriched"

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

    def materialize_to_duckdb(self, conn, table_name: str = "buildings") -> int:
        """
        Load this dataset's raw parquet into a table inside an already-open
        DuckDB connection, so it can be handed to Partitioner exactly the
        way an OSM `--db-path`/`--table` pair already is.

        This is the entire integration seam with partition_data.py — no
        changes to Partitioner itself are needed as long as its stages
        operate generically on `geometry` rather than assuming OSM-only
        columns.

        Returns the row count loaded, for logging.
        """
        glob = self.parquet_glob()
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning=false)
        """)
        (count,) = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return count


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
        h3_enriched=False,  # flip to True after partition_data.py runs for this key
        utm_epsg=32637,     # UTM 37N
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
        h3_enriched=False,
        utm_epsg=32628,     # UTM 28N — covers the Canary Islands
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
