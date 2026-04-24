import os
import logging
from contextlib import contextmanager
from typing import Optional, Generator, Any, Dict
import duckdb
import threading

logger = logging.getLogger(__name__)


class DuckDBConnectionPool:
    """Optimized DuckDB connection manager with singleton pattern.
    
    Unlike PostgreSQL, DuckDB is embedded. We reuse base connections and
    use .cursor() for thread safety (official recommended pattern).
    """

    _instance: Optional['DuckDBConnectionPool'] = None
    _base_connections: list[duckdb.DuckDBPyConnection] = []
    _lock = threading.Lock()

    # Configuration
    _database_path: str = ":memory:"
    _spatial_loaded: bool = False

    def __new__(cls) -> 'DuckDBConnectionPool':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(self,
                   database: Optional[str] = None,
                   read_only: bool = False,
                   config: Optional[Dict[str, Any]] = None,
                   max_connections: int = 8) -> None:
        """Initialize DuckDB database and preload spatial extension."""
        if self._base_connections:
            logger.warning("DuckDB connection pool already initialized")
            return

        config = config or {}
        # Good defaults for spatial + analytical workloads
        config.setdefault("threads", os.cpu_count() or 4)
        config.setdefault("memory_limit", "80%")

        self._database_path = database or os.getenv('DUCKDB_DATABASE', ':memory:')

        with self._lock:
            for _ in range(max_connections):
                con = duckdb.connect(
                    database=self._database_path,
                    read_only=read_only,
                    config=config
                )
                # Auto-load spatial extension (GEOMETRY type + ST_ functions)
                try:
                    con.execute("INSTALL spatial;")
                    con.execute("LOAD spatial;")
                except Exception as e:
                    logger.warning(f"Failed to load spatial extension: {e}")

                self._base_connections.append(con)

            self._spatial_loaded = True

        logger.info(f"DuckDB initialized → {self._database_path} "
                   f"(connections={max_connections}, threads={config.get('threads')})")

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get a thread-safe connection (cursor) with automatic cleanup.
        
        Returns a cursor created from the base connection — this is the
        recommended way for multi-threaded use in DuckDB.
        """
        if not self._base_connections:
            raise RuntimeError("DuckDB not initialized. Call initialize() first.")

        conn = None
        try:
            with self._lock:
                # Use first base connection + .cursor() for thread safety
                base_conn = self._base_connections[0]
                conn = base_conn.cursor()

            yield conn

        except Exception as e:
            logger.error(f"DuckDB query error: {e}")
            # DuckDB handles most rollbacks automatically via MVCC
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                try:
                    conn.close()   # Close the child cursor only
                except:
                    pass

    def execute(self, query: str, parameters: Any = None):
        """Convenience method: execute query and return as pandas DataFrame."""
        with self.get_connection() as conn:
            if parameters is not None:
                return conn.execute(query, parameters).df()
            return conn.execute(query).df()

    def close_all(self) -> None:
        """Close all connections."""
        with self._lock:
            for con in self._base_connections:
                try:
                    con.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            self._base_connections.clear()
            self._spatial_loaded = False
            logger.info("All DuckDB connections closed")

    def health_check(self) -> bool:
        """Perform a simple health check."""
        try:
            with self.get_connection() as conn:
                conn.execute("SELECT 1").fetchone()
                return True
        except Exception as e:
            logger.error(f"DuckDB health check failed: {e}")
            return False

    def ensure_spatial(self) -> None:
        """Ensure spatial extension is loaded (call if needed after initialize)."""
        if not self._spatial_loaded and self._base_connections:
            try:
                with self.get_connection() as conn:
                    conn.execute("INSTALL spatial;")
                    conn.execute("LOAD spatial;")
                self._spatial_loaded = True
                logger.info("Spatial extension loaded")
            except Exception as e:
                logger.error(f"Failed to load spatial extension: {e}")


# Global singleton instance (drop-in replacement for postgis_pool)
duckdb_pool = DuckDBConnectionPool()
