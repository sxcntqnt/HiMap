"""
View Generator — HiMap v3.0

Responsibility: for every registered dataset, create a DuckDB view that
makes the Parquet lake queryable as if it were a table.

Two view tiers per dataset:

    {key}_view      — raw pass-through over read_parquet(glob)
                      Columns match LOCKED_SCHEMA exactly.

    {key}_enriched  — adds runtime-derived columns for query convenience:
                        centroid_geom  GEOMETRY (from centroid_lat/lng)
                        bbox_geom      GEOMETRY (envelope from geometry)
                      These are not stored in Parquet (they cost compute),
                      but are cheap to derive and useful for spatial filters.

Usage:
    from himap.Services.DuckLakeService import ducklake_service
    from himap.dataset_registry import registry
    from himap.view_generator import ViewGenerator

    vg = ViewGenerator(ducklake_service)
    vg.build_all()                        # called once at startup
    vg.build("nairobi")                   # called after new dataset registered

Rules:
    - Views are CREATE OR REPLACE — safe to call repeatedly
    - Views reference the glob path from DatasetConfig — path changes
      propagate automatically on next build_all()
    - No dataset-specific logic lives here — the registry owns all config
"""

import logging
from typing import List, Optional

from ..Services.DuckLakeService import DuckLakeService
from ..Ingestion.DataRegistry import DatasetConfig, DatasetRegistry, registry as default_registry

logger = logging.getLogger(__name__)


class ViewGenerator:
    """
    Builds and rebuilds DuckDB views over the Parquet lake.

    One instance per application lifetime. Call build_all() at startup.
    """

    def __init__(
        self,
        ducklake: DuckLakeService,
        reg: Optional[DatasetRegistry] = None,
    ) -> None:
        self.ducklake = ducklake
        self.registry = reg or default_registry

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_all(self) -> List[str]:
        """
        Build views for every registered dataset.
        Returns list of dataset keys that were successfully built.
        """
        built = []
        for key in self.registry.list():
            try:
                self.build(key)
                built.append(key)
            except Exception as e:
                logger.error(f"View build failed for '{key}': {e}")
        logger.info(f"Views built for: {built}")
        return built

    def build(self, key: str) -> None:
        """
        Build both view tiers for a single registered dataset.
        Safe to call repeatedly — uses CREATE OR REPLACE.
        """
        config = self.registry.get(key)
        conn = self.ducklake._get_connection()
        try:
            self._create_base_view(conn, key, config)
            self._create_enriched_view(conn, key, config)
            logger.info(f"Views created: {key}_view, {key}_enriched")
        finally:
            conn.close()

    def drop(self, key: str) -> None:
        """Drop both view tiers for a dataset. Use before deregistering."""
        conn = self.ducklake._get_connection()
        try:
            conn.execute(f"DROP VIEW IF EXISTS {key}_view")
            conn.execute(f"DROP VIEW IF EXISTS {key}_enriched")
            logger.info(f"Views dropped: {key}_view, {key}_enriched")
        finally:
            conn.close()

    def list_views(self) -> List[str]:
        """List all HiMap-managed views currently in DuckDB."""
        rows = self.ducklake.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
              AND table_schema = 'main'
            ORDER BY table_name
        """)
        return [r["table_name"] for r in rows]

    # ------------------------------------------------------------------
    # View construction
    # ------------------------------------------------------------------

    def _create_base_view(
        self,
        conn,
        key: str,
        config: DatasetConfig,
    ) -> None:
        """
        Tier 1: raw pass-through view over read_parquet(glob).
        Columns match LOCKED_SCHEMA exactly — no transformation.
        """
        glob = config.parquet_glob()
        view = config.view_name(key)

        conn.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT
                feature_id,
                feature_type,
                geometry,
                centroid_lat,
                centroid_lng,
                country_code,
                h3_7,
                h3_8,
                h3_9,
                h3_10,
                tile_z,
                tile_x,
                tile_y,
                zorder_key,
                entropy_bucket,
                cell_variance,
                importance_byte,
                entropy_score,
                compressed_size_bytes,
                partition_run_id,
                highway,
                building,
                amenity,
                landuse,
                population,
                road_class
            FROM read_parquet('{glob}', hive_partitioning=false)
        """)

    def _create_enriched_view(
        self,
        conn,
        key: str,
        config: DatasetConfig,
    ) -> None:
        """
        Tier 2: enriched view — adds geometry columns derived at read-time.

        centroid_geom: ST_Point from stored centroid_lat/lng.
                       Used for fast point-in-polygon and distance queries.

        bbox_geom:     ST_Envelope of the feature geometry.
                       Used for bounding box intersection filters as an
                       alternative to full ST_Intersects on the raw geometry.

        Both are intentionally NOT stored in Parquet (compute cost at write,
        compression penalty). They are cheap at read time from stored coords.
        """
        base_view = config.view_name(key)
        enriched_view = config.enriched_view_name(key)

        conn.execute(f"""
            CREATE OR REPLACE VIEW {enriched_view} AS
            SELECT
                *,
                ST_Point(centroid_lng, centroid_lat)       AS centroid_geom,
                ST_Envelope(ST_GeomFromWKB(geometry))      AS bbox_geom
            FROM {base_view}
        """)

    # ------------------------------------------------------------------
    # Zoom-aware query helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _zoom_limit(zoom: Optional[int]) -> int:
        """
        Return the appropriate feature limit for a given zoom level.

        At low zoom the user sees the whole city — only high-importance
        landmarks matter. At street zoom, every feature in the viewport
        is meaningful.

        Zoom range → limit:
            0–9  (continental → metro):  500   — only major features
            10–12 (city):               2000   — key infrastructure
            13–15 (neighborhood):       5000   — full street detail
            16+  (street):              5000   — maximum detail
        """
        if zoom is None:
            return 5000
        if zoom <= 9:
            return 500
        if zoom <= 12:
            return 2000
        return 5000

    @staticmethod
    def _importance_threshold(zoom: Optional[int]) -> int:
        """
        Return the minimum importance_byte for a given zoom level.

        At low zoom, only high-importance features (hospitals, major roads,
        landmarks) should be returned. At street zoom, all features qualify.

        importance_byte is 0–127. Higher = more important.

        Zoom range → min importance:
            0–9:   80  — only top ~37% by importance (landmarks, motorways)
            10–12: 40  — top ~69% (main roads, commercial, amenities)
            13+:    0  — everything
        """
        if zoom is None:
            return 0
        if zoom <= 9:
            return 80
        if zoom <= 12:
            return 40
        return 0

    # ------------------------------------------------------------------
    # Query helpers (used by API layer — no SQL outside this module)
    # ------------------------------------------------------------------

    def bbox_query(self, key: str, zoom: Optional[int] = None) -> str:
        """
        Return parameterized SQL for a bounding box query.

        Parameters (positional):
            sw_lng, sw_lat, ne_lng, ne_lat

        Zoom-aware:
            - Limit scales with zoom (500 at metro, 5000 at street)
            - importance_byte threshold filters low-importance features
              at low zoom levels — only landmarks at metro scale

        Usage:
            sql = vg.bbox_query("nairobi", zoom=12)
            rows = ducklake.execute(sql, (sw_lng, sw_lat, ne_lng, ne_lat))
        """
        config   = self.registry.get(key)
        view     = config.enriched_view_name(key)
        limit    = self._zoom_limit(zoom)
        min_imp  = self._importance_threshold(zoom)

        importance_clause = (
            f"AND importance_byte >= {min_imp}" if min_imp > 0 else ""
        )

        return f"""
            SELECT *
            FROM {view}
            WHERE ST_Intersects(
                centroid_geom,
                ST_MakeEnvelope(?, ?, ?, ?)
            )
            {importance_clause}
            ORDER BY entropy_bucket ASC, importance_byte DESC
            LIMIT {limit}
        """

    def h3_query(self, key: str, resolution: int = 8, zoom: Optional[int] = None) -> str:
        """
        Return parameterized SQL for an H3 index query.

        Parameters (positional):
            h3_index  (string)

        Zoom-aware:
            - Resolution defaults to 8 but should be passed from
              zoom_levels.h3_resolution_for_zoom(zoom)
            - Limit and importance threshold scale with zoom

        Usage:
            sql = vg.h3_query("nairobi", resolution=8, zoom=12)
            rows = ducklake.execute(sql, (h3_index,))
        """
        config  = self.registry.get(key)
        view    = config.view_name(key)
        h3_col  = f"h3_{resolution}"
        limit   = self._zoom_limit(zoom)
        min_imp = self._importance_threshold(zoom)

        if resolution not in config.h3_resolutions:
            raise ValueError(
                f"Resolution {resolution} not available for dataset '{key}'. "
                f"Available: {config.h3_resolutions}"
            )

        importance_clause = (
            f"AND importance_byte >= {min_imp}" if min_imp > 0 else ""
        )

        return f"""
            SELECT *
            FROM {view}
            WHERE {h3_col} = ?
            {importance_clause}
            ORDER BY importance_byte DESC
            LIMIT {limit}
        """

    def h3_query_for_zoom(self, key: str, zoom: int) -> str:
        """
        Return parameterized SQL for an H3 index query, deriving resolution
        and limit automatically from the map zoom level.

        Parameters (positional):
            h3_index  (string)

        Usage:
            sql = vg.h3_query_for_zoom("nairobi", zoom=12)
            # → queries h3_8, limit=2000, importance >= 40
        """
        from .Utils.Zoom import zoom_levels
        resolution = zoom_levels.h3_resolution_for_zoom(zoom)
        return self.h3_query(key, resolution=resolution, zoom=zoom)
