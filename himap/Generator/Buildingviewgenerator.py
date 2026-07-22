"""
Building View Generator — HiMap v3.0

Responsibility: for every registered building dataset, create DuckDB views
that make it queryable without re-issuing CREATE VIEW on every request.

Three view tiers per building dataset — more than OSM's two, because
buildings genuinely have two different physical sources, not one:

    {key}_buildings_view         — raw pass-through over the pre-enrichment
                                    OpenBuildingMap parquet at
                                    BuildingConfig.base_path. Always
                                    buildable, even before partition_data.py
                                    has ever run for this dataset. This is
                                    what the pre-H3 bbox/quadkey query path
                                    in building_routes.py reads.

    {key}_buildings_partitioned  — raw pass-through over the ENRICHED,
                                    H3-partitioned output that
                                    partition_data.py writes (centroid,
                                    h3_7..h3_10, area_m2, height_m, etc.).
                                    Only buildable once that pipeline has
                                    actually run — see build().

    {key}_buildings_enriched     — {key}_buildings_partitioned plus
                                    runtime-derived geometry columns
                                    (centroid_geom, bbox_geom), matching
                                    the OSM ViewGenerator's enriched tier.

Which tier a query should read from is a correctness question, not a
performance one: the partitioned/enriched tiers do not exist — and their
underlying columns (h3_9, area_m2, height_m, ...) are not populated —
until BuildingConfig.h3_enriched is True. Querying them before that point
returns either an empty view or a file-not-found error, since
partition_data.py hasn't written anything to enriched_parquet_glob() yet.

Usage:
    from himap.Services.DuckLakeService import ducklake_service
    from himap.Generator.buildingViewGenerator import BuildingViewGenerator

    bvg = BuildingViewGenerator(ducklake_service)
    bvg.build_all()                # called once at startup, alongside
                                    # the OSM ViewGenerator.build_all()
    bvg.build("kenya-buildings")   # called again after partition_data.py
                                    # runs for a dataset, to pick up the
                                    # newly-written partitioned/enriched
                                    # views (or after h3_enriched flips)

Rules (same as ViewGenerator):
    - Views are CREATE OR REPLACE — safe to call repeatedly
    - Views reference glob paths from BuildingConfig — path changes
      propagate automatically on next build_all()
    - No dataset-specific logic lives here — BuildingRegistry owns all config
"""

import logging
from typing import List, Optional

from ..Services.DuckLakeService import DuckLakeService
from ..Ingestion.BuildingRegistry import (
    BuildingConfig,
    BuildingRegistry,
    building_registry as default_building_registry,
)

logger = logging.getLogger(__name__)


# Columns present in the raw OpenBuildingMap parquet (base_path).
_RAW_COLUMNS = [
    "id", "floorspace", "occupancy", "relation_id", "quadkey",
    "last_update", "height", "geometry", "bbox", "source",
]

# Columns present in the enriched/partitioned lake output — the LOCKED_SCHEMA
# fields relevant to buildings (OSM-only fields like highway/amenity are
# always NULL for these rows and intentionally omitted here for a cleaner
# API surface; query the raw Parquet directly if you need them).
_PARTITIONED_COLUMNS = [
    "feature_id", "feature_type", "geometry",
    "centroid_lat", "centroid_lng", "country_code",
    "h3_7", "h3_8", "h3_9", "h3_10",
    "tile_z", "tile_x", "tile_y", "zorder_key",
    "entropy_bucket", "cell_variance", "importance_byte", "entropy_score",
    "compressed_size_bytes", "partition_run_id",
    "floorspace", "occupancy", "height_raw", "height_m", "height_floors",
    "quadkey", "relation_id", "source_provider", "area_m2", "perimeter_m",
]


class BuildingViewGenerator:
    """
    Builds and rebuilds DuckDB views over both the raw and enriched
    building Parquet sources.

    One instance per application lifetime. Call build_all() at startup,
    same as ViewGenerator.
    """

    def __init__(
        self,
        ducklake: DuckLakeService,
        reg: Optional[BuildingRegistry] = None,
    ) -> None:
        self.ducklake = ducklake
        self.registry = reg or default_building_registry

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_all(self) -> List[str]:
        """
        Build views for every registered building dataset.
        Returns list of dataset keys that were successfully built (the
        raw tier at minimum — partitioned/enriched tiers are best-effort,
        see build()).
        """
        built = []
        for key in self.registry.list():
            try:
                self.build(key)
                built.append(key)
            except Exception as e:
                logger.error(f"Building view build failed for '{key}': {e}")
        logger.info(f"Building views built for: {built}")
        return built

    def build(self, key: str) -> None:
        """
        Build the raw view unconditionally. Build the partitioned +
        enriched views only if config.h3_enriched is set — attempting
        them before partition_data.py has run would create a view over
        a glob with nothing behind it (or, worse, a stale one from a
        previous dataset at the same path).

        Safe to call repeatedly — uses CREATE OR REPLACE throughout.
        """
        config = self.registry.get(key)
        conn = self.ducklake._get_connection()
        try:
            self._create_source_view(conn, key, config)
            logger.info(f"View created: {config.view_name(key)}")

            if config.h3_enriched:
                self._create_partitioned_view(conn, key, config)
                self._create_enriched_view(conn, key, config)
                logger.info(
                    f"Views created: {config.partitioned_view_name(key)}, "
                    f"{config.enriched_view_name(key)}"
                )
            else:
                logger.info(
                    f"'{key}' is not h3_enriched yet — skipping "
                    f"{config.partitioned_view_name(key)} / "
                    f"{config.enriched_view_name(key)}. Run "
                    f"partition_data.py for this dataset, flip "
                    f"h3_enriched=True, then call build('{key}') again."
                )
        finally:
            conn.close()

    def drop(self, key: str) -> None:
        """Drop all view tiers for a building dataset. Use before deregistering."""
        config = self.registry.get(key)
        conn = self.ducklake._get_connection()
        try:
            conn.execute(f"DROP VIEW IF EXISTS {config.view_name(key)}")
            conn.execute(f"DROP VIEW IF EXISTS {config.partitioned_view_name(key)}")
            conn.execute(f"DROP VIEW IF EXISTS {config.enriched_view_name(key)}")
            logger.info(f"Building views dropped for '{key}'")
        finally:
            conn.close()

    def list_views(self) -> List[str]:
        """List all HiMap-managed building views currently in DuckDB."""
        rows = self.ducklake.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
              AND table_schema = 'main'
              AND table_name LIKE '%\\_buildings\\_%' ESCAPE '\\'
            ORDER BY table_name
        """)
        return [r["table_name"] for r in rows]

    # ------------------------------------------------------------------
    # View construction
    # ------------------------------------------------------------------

    def _create_source_view(self, conn, key: str, config: BuildingConfig) -> None:
        """
        Raw pass-through view over the pre-enrichment OpenBuildingMap
        parquet. Columns match the source schema exactly — no transform.
        """
        glob = config.parquet_glob()
        view = config.view_name(key)
        columns = ",\n                ".join(_RAW_COLUMNS)

        conn.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT
                {columns}
            FROM read_parquet('{glob}', hive_partitioning=false)
        """)

    def _create_partitioned_view(self, conn, key: str, config: BuildingConfig) -> None:
        """
        Raw pass-through view over the enriched, H3-partitioned lake
        output written by partition_data.py / Partitioner.
        """
        glob = config.enriched_parquet_glob()
        view = config.partitioned_view_name(key)
        columns = ",\n                ".join(_PARTITIONED_COLUMNS)

        conn.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT
                {columns}
            FROM read_parquet('{glob}', hive_partitioning=false)
        """)

    def _create_enriched_view(self, conn, key: str, config: BuildingConfig) -> None:
        """
        Enriched tier — adds centroid_geom / bbox_geom derived at read
        time, same rationale as ViewGenerator's OSM enriched tier: cheap
        to compute from stored coordinates, not worth storing in Parquet.
        """
        base_view = config.partitioned_view_name(key)
        enriched_view = config.enriched_view_name(key)

        conn.execute(f"""
            CREATE OR REPLACE VIEW {enriched_view} AS
            SELECT
                *,
                ST_Point(centroid_lng, centroid_lat)   AS centroid_geom,
                ST_Envelope(ST_GeomFromWKB(geometry))  AS bbox_geom
            FROM {base_view}
        """)
