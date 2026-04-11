import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
from contextlib import contextmanager
from typing import Optional, Generator

logger = logging.getLogger(__name__)

class PostGISConnectionPool:
    """Optimized PostGIS connection pool with singleton pattern"""
    
    _instance: Optional['PostGISConnectionPool'] = None
    _pool: Optional[pool.ThreadedConnectionPool] = None
    
    def __new__(cls) -> 'PostGISConnectionPool':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self, 
                   host: str = None,
                   port: int = None,
                   database: str = None,
                   user: str = None,
                   password: str = None,
                   minconn: int = 1,
                   maxconn: int = 20) -> None:
        """Initialize the connection pool with optimized settings"""
        if self._pool is not None:
            logger.warning("Connection pool already initialized")
            return
            
        # Use environment variables as fallback
        self._pool = pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host or os.getenv('POSTGIS_HOST', 'localhost'),
            port=port or int(os.getenv('POSTGIS_PORT', '5432')),
            database=database or os.getenv('POSTGIS_DB', 'himap'),
            user=user or os.getenv('POSTGIS_USER', 'postgres'),
            password=password or os.getenv('POSTGIS_PASSWORD', ''),
            cursor_factory=RealDictCursor,
            # Optimized connection settings
            connect_timeout=10,
            options='-c statement_timeout=30000'  # 30 second statement timeout
        )
        logger.info(f"PostGIS connection pool initialized with {minconn}-{maxconn} connections")
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Get a connection from the pool with automatic cleanup"""
        if self._pool is None:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
        
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error getting connection from pool: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("All PostGIS connections closed")
    
    def health_check(self) -> bool:
        """Check if the database is accessible"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

# Global instance for easy access
postgis_pool = PostGISConnectionPool()