"""
DuckLake Service — Connection and Execution Layer

Responsibility: one thing only.
    Provide configured DuckDB connections and execute queries against them.

Does NOT know about:
    - datasets
    - table names
    - parquet paths
    - spatial domains

Those concerns live in:
    - dataset_registry.py  → what datasets exist and where
    - view_generator.py    → how to expose datasets as queryable views
    - API query layer      → what queries to run against those views
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import duckdb
from shapely import wkb

logger = logging.getLogger(__name__)


class DuckLakeService:
    """
    DuckDB connection pool and query executor.

    Manages:
        - Extension installation and loading (spatial, h3, postgres)
        - Optional PostGIS catalog attachment
        - Connection configuration (memory limit, threads)
        - Generic query execution

    All callers are responsible for knowing what they query.
    This class has no opinion about tables, datasets, or domains.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        memory_limit: str = "2GB",
        threads: int = 4,
        postgis_catalog: Optional[Dict[str, str]] = None,
    ):
        """
        Args:
            db_path:         Path to DuckDB file, or ":memory:"
            memory_limit:    Per-connection memory cap (e.g. "2GB")
            threads:         Thread count per connection
            postgis_catalog: Optional PostGIS connection params:
                             { host, port, database, user, password }
        """
        self.db_path         = db_path
        self.memory_limit    = memory_limit
        self.threads         = threads
        self.postgis_catalog = postgis_catalog

        self._init_extensions()

        logger.info(
            f"DuckLakeService ready — db={db_path}, "
            f"mem={memory_limit}, threads={threads}, "
            f"catalog={'postgis' if postgis_catalog else 'duckdb-only'}"
        )

    # ------------------------------------------------------------------
    # Extension management
    # ------------------------------------------------------------------

    def _init_extensions(self) -> None:
        """
        Install and verify required extensions once at startup.
        Subsequent _get_connection() calls only LOAD (not re-install).
        """
        conn = duckdb.connect(self.db_path)
        try:
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")
            conn.execute("INSTALL h3 from community")
            conn.execute("LOAD h3")

            if self.postgis_catalog:
                conn.execute("INSTALL postgres")
                conn.execute("LOAD postgres")
                self._attach_postgis_catalog(conn)

            logger.info(
                "Extensions ready: spatial, h3" +
                (", postgres" if self.postgis_catalog else "")
            )
        finally:
            conn.close()

    def _attach_postgis_catalog(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Attach PostGIS database as 'postgis' schema inside a connection."""
        if not self.postgis_catalog:
            return
        c = self.postgis_catalog
        try:
            conn.execute(f"""
                ATTACH 'dbname={c['database']} user={c['user']}
                        password={c['password']} host={c['host']}
                        port={c['port']}'
                AS postgis (TYPE POSTGRES)
            """)
            logger.info("PostGIS catalog attached as 'postgis'")
        except Exception as e:
            logger.warning(f"PostGIS catalog attach failed: {e}")

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Return a configured DuckDB connection with extensions loaded.
        Caller is responsible for closing the connection.
        """
        conn = duckdb.connect(self.db_path)
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")
        conn.execute("LOAD spatial")
        conn.execute("LOAD h3")
        if self.postgis_catalog:
            conn.execute("LOAD postgres")
        return conn

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, query: str, params: tuple = ()) -> List[Dict]:
        """
        Execute a query, return results as list of dicts.

        Opens and closes its own connection — use for single-shot queries.
        For multi-statement workflows, use _get_connection() directly.
        """
        conn = None
        try:
            conn = self._get_connection()
            result = conn.execute(query, params)
            description = result.description or []
            columns = [d[0] for d in description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error(f"Query (first 300): {query[:300]}")
            raise
        finally:
            if conn:
                conn.close()

    # ------------------------------------------------------------------
    # Geometry utility
    # ------------------------------------------------------------------

    @staticmethod
    def wkb_to_geojson(wkb_hex: str) -> Dict:
        """Convert WKB hex string to GeoJSON geometry dict."""
        try:
            geom = wkb.loads(bytes.fromhex(wkb_hex), hex=True)
            return json.loads(geom.__geo_interface__)
        except Exception as e:
            logger.error(f"WKB → GeoJSON failed: {e}")
            return {"type": "Point", "coordinates": [0, 0]}

    # ------------------------------------------------------------------
    # Catalog introspection
    # ------------------------------------------------------------------

    def list_tables(self) -> List[Dict]:
        """
        List all tables visible to this connection.
        Returns source ('duckdb' or 'postgis') alongside name and type.
        """
        conn = self._get_connection()
        tables = []
        try:
            rows = conn.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()
            for name, ttype in rows:
                tables.append({"name": name, "type": ttype, "source": "duckdb"})

            if self.postgis_catalog:
                try:
                    rows = conn.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = 'postgis'
                    """).fetchall()
                    for name, ttype in rows:
                        tables.append({"name": name, "type": ttype, "source": "postgis"})
                except Exception as e:
                    logger.warning(f"Could not list PostGIS tables: {e}")
        finally:
            conn.close()
        return tables

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Verify connectivity and report latency."""
        start = time.time()
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            conn.close()
            return {
                "healthy":    True,
                "latency_ms": round((time.time() - start) * 1000, 2),
                "catalog":    "postgis" if self.postgis_catalog else "duckdb-only",
            }
        except Exception as e:
            return {
                "healthy":    False,
                "latency_ms": round((time.time() - start) * 1000, 2),
                "error":      str(e),
            }


# ---------------------------------------------------------------------------
# Module-level singleton
# Override at application startup via environment / config injection.
# ---------------------------------------------------------------------------
ducklake_service = DuckLakeService(
    db_path=":memory:",
    memory_limit="2GB",
    threads=4,
)
