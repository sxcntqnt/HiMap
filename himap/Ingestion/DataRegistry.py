"""
Dataset Registry — HiMap v3.0

Single source of truth for every dataset in the lake.

Rules:
    - Adding a dataset = one registry.register() call
    - All path resolution flows through this module
    - No dataset name or path appears anywhere else in the codebase
    - Dataset key is the API-facing identifier (used in ?dataset= param)

Registering a new country:
    registry.register(
        "kenya",
        DatasetConfig(
            country="KE",
            base_path="./lake/ke/nairobi/",
            h3_resolutions=[7, 8, 9, 10],
        )
    )

That is the only change required to make a new dataset queryable.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class DatasetConfig:
    """
    Configuration for a single registered dataset.

    base_path:       Root path for this dataset's Parquet lake partition.
                     Local:  "./lake/es/"
                     S3:     "s3://himap-lake/es/"
                     The view generator appends "**/*.parquet" for glob reads.

    country:         ISO-2 country code.

    h3_resolutions:  H3 resolutions present in this dataset's Parquet files.
                     Must match what the Partitioner wrote. Default [7,8,9,10].

    country_filter:  Value matched against the osm.country column at ingestion.
                     e.g. "canary-islands" filters to only those source rows.
                     None means no filter — process all rows in the source table.

    bbox:            Optional bounding box filter (sw_lng, sw_lat, ne_lng, ne_lat).
                     Applied as ST_Intersects at ingestion if set.
                     If both country_filter and bbox are set, both are applied (AND).

    has_buildings:   Semantic hint for query planners.
    has_roads:       Semantic hint for query planners.
    """
    country:        str
    base_path:      str

    h3_resolutions: List[int]                        = field(default_factory=lambda: [7, 8, 9, 10])
    country_filter: Optional[str]                    = None
    bbox:           Optional[tuple]                  = None   # (sw_lng, sw_lat, ne_lng, ne_lat)
    has_buildings:  bool                             = True
    has_roads:      bool                             = True

    def parquet_glob(self) -> str:
        """Glob pattern used by DuckDB read_parquet()."""
        return f"{self.base_path.rstrip('/')}/**/*.parquet"

    def view_name(self, key: str) -> str:
        """Canonical DuckDB view name for this dataset."""
        return f"{key}_view"

    def enriched_view_name(self, key: str) -> str:
        """Enriched DuckDB view name (centroid + H3 + entropy columns)."""
        return f"{key}_enriched"


    def source_filter_sql(self) -> str:
        """
        Return the WHERE clause fragment for ingestion-time filtering.
        Returns '1=1' when no filter is configured (process all rows).
        """
        clauses = ["geom IS NOT NULL"]

        if self.country_filter:
            clauses.append(f"country = '{self.country_filter}'")

        if self.bbox:
            sw_lng, sw_lat, ne_lng, ne_lat = self.bbox
            clauses.append(
                f"ST_Intersects("
                f"ST_GeomFromWKB(geom), "
                f"ST_MakeEnvelope({sw_lng}, {sw_lat}, {ne_lng}, {ne_lat})"
                f")"
            )

        return " AND ".join(clauses)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class DatasetRegistry:
    """
    Registry of all datasets known to HiMap.
    Populated at startup. Read-only at query time.
    """

    def __init__(self) -> None:
        self._datasets: Dict[str, DatasetConfig] = {}

    def register(self, key: str, config: DatasetConfig) -> None:
        """
        Register a dataset under the given key.
        Raises ValueError if key is already registered (prevents silent clobber).
        """
        if key in self._datasets:
            raise ValueError(
                f"Dataset '{key}' already registered. "
                f"Call deregister() first if replacement is intentional."
            )
        self._datasets[key] = config

    def deregister(self, key: str) -> None:
        """Remove a dataset. Use in tests and dataset migrations."""
        self._datasets.pop(key, None)

    def get(self, key: str) -> DatasetConfig:
        """
        Return config for a registered dataset.
        Raises ValueError (not KeyError) — callers convert this to HTTP 400.
        """
        if key not in self._datasets:
            raise ValueError(
                f"Unknown dataset: '{key}'. "
                f"Registered datasets: {sorted(self._datasets.keys())}"
            )
        return self._datasets[key]

    def list(self) -> List[str]:
        """Sorted list of registered dataset keys."""
        return sorted(self._datasets.keys())

    def all(self) -> Dict[str, DatasetConfig]:
        """Shallow copy of the full registry."""
        return dict(self._datasets)

    def __contains__(self, key: str) -> bool:
        return key in self._datasets

    def __len__(self) -> int:
        return len(self._datasets)


# ---------------------------------------------------------------------------
# Global registry instance
# Import this everywhere. Do not instantiate DatasetRegistry directly.
# ---------------------------------------------------------------------------

registry = DatasetRegistry()

# ---------------------------------------------------------------------------
# Registered datasets
#
# base_path reads from environment first, falls back to local lake layout:
#     ./lake/{country_lower}/{city}/
#
# Same code works locally and in production — only the env var changes.
# ---------------------------------------------------------------------------

_LAKE_ROOT = os.getenv("HIMAP_LAKE_ROOT", "./lake")

# IMPORTANT: country_filter values must match the OSM source table's
# `country` column exactly — these are admin-boundary strings, not ISO codes.
# Confirmed values from the data:
#   'canary-islands'  (not 'ES', not 'Spain', not 'Canary Islands')
#   'kenya'           (not 'KE', not 'Kenya')
# Run: SELECT DISTINCT country FROM osm; to verify values for new datasets.

registry.register(
    "canary",
    DatasetConfig(
        country="ES",
        base_path=os.getenv("HIMAP_DATASET_CANARY", f"{_LAKE_ROOT}/es/"),
        h3_resolutions=[7, 8, 9, 10],
        country_filter="canary-islands",   # exact match against osm.country column
    ),
)

registry.register(
    "kenya",
    DatasetConfig(
        country="KE",
        base_path=os.getenv("HIMAP_DATASET_KENYA", f"{_LAKE_ROOT}/ke/"),
        h3_resolutions=[7, 8, 9, 10],
        country_filter="kenya",            # exact match against osm.country column
    ),
)

# Whole-Africa dataset — no filter, processes all rows in the source table
# registry.register(
#     "africa",
#     DatasetConfig(
#         country="MULTI",
#         base_path=os.getenv("HIMAP_DATASET_AFRICA", f"{_LAKE_ROOT}/africa/"),
#         country_filter=None,
#         bbox=None,
#     ),
# )

# Adding a new dataset:
# 1. Run: SELECT DISTINCT country FROM osm WHERE country LIKE '%tanzania%';
#    to confirm the exact country column value in the source data.
# 2. Register here with that exact string as country_filter.
# 3. Run the pipeline: python partition_data.py --dataset tanzania
#
# registry.register(
#     "tanzania",
#     DatasetConfig(
#         country="TZ",
#         base_path=os.getenv("HIMAP_DATASET_TANZANIA", f"{_LAKE_ROOT}/tz/"),
#         country_filter="tanzania",   # verify against SELECT DISTINCT country FROM osm
#     ),
# )
