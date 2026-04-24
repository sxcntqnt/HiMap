"""
DuckLake Service - Unified Spatial Data Access

DuckLake combines DuckDB's analytical engine with PostGIS catalog integration,
providing a single interface for querying spatial data whether it's stored in
DuckDB native tables or accessed via PostGIS catalog.

Architecture:
- DuckDB as the query engine (vectorized, columnar)
- PostGIS catalog for spatial table discovery and metadata
- PostgreSQL scanner extension for querying PostGIS tables directly
- Spatial extension for geometry operations

This replaces both DuckDBService and PostGISService.
"""

import json
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb

logger = logging.getLogger(__name__)


class DuckLakeService:
    """
    DuckLake service - DuckDB engine with PostGIS catalog integration.
    
    Provides unified access to:
    1. Native DuckDB tables (fast, in-memory or persistent)
    2. PostGIS catalog tables (via PostgreSQL scanner extension)
    
    All spatial queries use DuckDB's spatial extension regardless of source.
    """
    
    def __init__(self,
                 db_path: str = ":memory:",
                 memory_limit: str = "2GB",
                 threads: int = 4,
                 postgis_catalog: Optional[Dict[str, str]] = None):
        """
        Initialize DuckLake service.
        
        Args:
            db_path: Path to DuckDB file (default: ":memory:")
            memory_limit: Memory limit per connection (e.g., "2GB")
            threads: Number of threads to use per connection
            postgis_catalog: Optional PostGIS connection for catalog queries:
                {
                    'host': 'localhost',
                    'port': '5432',
                    'database': 'himap',
                    'user': 'postgres',
                    'password': ''
                }
        """
        self.db_path = db_path
        self.memory_limit = memory_limit
        self.threads = threads
        self.postgis_catalog = postgis_catalog
        self._conn = None
        self._init_extensions()
        
        logger.info(f"DuckLakeService initialized: db_path={db_path}, "
                   f"memory_limit={memory_limit}, threads={threads}")
        
        if postgis_catalog:
            logger.info(f"PostGIS catalog configured: {postgis_catalog['host']}:{postgis_catalog['port']}")
    
    def _init_extensions(self):
        """Install and load required DuckDB extensions."""
        conn = duckdb.connect(self.db_path)
        try:
            # Core extensions
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")
            conn.execute("INSTALL h3")
            conn.execute("LOAD h3")
            
            # PostgreSQL scanner for PostGIS catalog access
            if self.postgis_catalog:
                conn.execute("INSTALL postgres")
                conn.execute("LOAD postgres")
                self._attach_postgis_catalog(conn)
            
            logger.info("DuckDB extensions loaded: spatial, h3, postgres")
        finally:
            conn.close()
    
    def _attach_postgis_catalog(self, conn):
        """Attach PostGIS database as catalog."""
        if not self.postgis_catalog:
            return
        
        try:
            conn.execute(f"""
                ATTACH 'dbname={self.postgis_catalog['database']} "
                "user={self.postgis_catalog['user']} "
                "password={self.postgis_catalog['password']} "
                "host={self.postgis_catalog['host']} "
                "port={self.postgis_catalog['port']}' "
                "AS postgis (TYPE POSTGRES)
            """)
            logger.info("PostGIS catalog attached as 'postgis'")
        except Exception as e:
            logger.warning(f"Failed to attach PostGIS catalog: {e}")
    
    def _get_connection(self):
        """Get DuckDB connection with configured limits."""
        conn = duckdb.connect(self.db_path)
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")
        conn.execute("LOAD spatial")
        conn.execute("LOAD h3")
        if self.postgis_catalog:
            conn.execute("LOAD postgres")
        return conn
    
    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute query and return results as list of dicts."""
        conn = None
        try:
            conn = self._get_connection()
            result = conn.execute(query, params)
            columns = [desc[0] for desc in description] if (description := result.description) else []
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"DuckLake query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _wkb_to_geojson(self, wkb_hex: str) -> Dict:
        """Convert WKB hex to GeoJSON geometry."""
        try:
            geom = wkb.loads(bytes.fromhex(wkb_hex), hex=True)
            return json.loads(geom.__geo_interface__)
        except Exception as e:
            logger.error(f"WKB to GeoJSON conversion failed: {e}")
            return {"type": "Point", "coordinates": [0, 0]}
    
    def _get_table_reference(self, table_name: str) -> str:
        """
        Get fully qualified table reference.
        
        Returns:
            'postgis.{table}' if PostGIS catalog is available and table exists there
            '{table}' for native DuckDB tables
        """
        if not self.postgis_catalog:
            return table_name
        
        # Check if table exists in PostGIS catalog
        conn = self._get_connection()
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'postgis' AND table_name = '{table_name}'
            """).fetchone()
            if result and result[0] > 0:
                return f"postgis.{table_name}"
        except:
            pass
        finally:
            conn.close()
        
        return table_name
    
    # Traffic Node Queries
    
    def get_nodes_in_bounds(self, bounds: Dict[str, Any],
                           node_types: Optional[List[str]] = None,
                           min_saturation: Optional[float] = None) -> List[Dict]:
        """Get all traffic nodes within a bounding box."""
        table = self._get_table_reference("traffic_nodes")
        
        query = f"""
            SELECT
                id,
                name,
                ST_AsWKB(geom) as geom,
                node_type,
                passenger_throughput,
                average_dwell_time,
                peak_hour,
                saturation_level,
                connected_routes
            FROM {table}
            WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(?, ?, ?, ?, 4326)
            )
        """
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        if node_types:
            query += " AND node_type = ANY(?)"
            params.append(node_types)
        
        if min_saturation is not None:
            query += " AND saturation_level >= ?"
            params.append(min_saturation)
        
        query += " ORDER BY passenger_throughput DESC LIMIT 500"
        
        results = self._execute_query(query, tuple(params))
        
        return [{
            'id': row['id'],
            'name': row['name'],
            'position': {
                'lat': wkb.loads(bytes.fromhex(row['geom'])).y if row['geom'] else 0,
                'lng': wkb.loads(bytes.fromhex(row['geom'])).x if row['geom'] else 0
            },
            'type': row['node_type'],
            'metrics': {
                'passengerThroughput': row['passenger_throughput'],
                'averageDwellTime': row['average_dwell_time'],
                'peakHour': row['peak_hour'],
                'saturationLevel': row['saturation_level']
            },
            'connectedRoutes': row['connected_routes'] or []
        } for row in results]
    
    # Corridor Analytics Queries
    
    def get_corridors_in_bounds(self, bounds: Dict[str, Any]) -> List[Dict]:
        """Get corridors within bounds."""
        table = self._get_table_reference("corridor_analytics")
        
        query = f"""
            SELECT
                id,
                name,
                start_node,
                end_node,
                ST_AsWKB(geom) as geom,
                fuel_burn_rate,
                idling_hotspot_score,
                vehicle_stress_index,
                average_speed,
                peak_flow_time
            FROM {table}
            WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(?, ?, ?, ?, 4326)
            )
            ORDER BY idling_hotspot_score DESC
            LIMIT 200
        """
        
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        results = self._execute_query(query, tuple(params))
        
        return [{
            'id': row['id'],
            'name': row['name'],
            'startNode': row['start_node'],
            'endNode': row['end_node'],
            'geometry': self._wkb_to_geojson(row['geom']),
            'metrics': {
                'fuelBurnRate': row['fuel_burn_rate'],
                'idlingHotspotScore': row['idling_hotspot_score'],
                'vehicleStressIndex': row['vehicle_stress_index'],
                'averageSpeed': row['average_speed'],
                'peakFlowTime': row['peak_flow_time']
            }
        } for row in results]
    
    # Vehicle Tracking Queries
    
    def get_vehicles_in_bounds(self, bounds: Dict[str, Any]) -> List[Dict]:
        """Get active vehicles within bounds."""
        vehicles_table = self._get_table_reference("vehicles")
        saccos_table = self._get_table_reference("saccos")
        
        query = f"""
            SELECT
                v.id,
                v.sacco_id,
                s.name as sacco_name,
                v.plate_number,
                v.capacity,
                ST_AsWKB(v.position) as position,
                v.heading,
                v.speed,
                v.status,
                v.last_updated
            FROM {vehicles_table} v
            JOIN {saccos_table} s ON v.sacco_id = s.id
            WHERE ST_Intersects(
                v.position,
                ST_MakeEnvelope(?, ?, ?, ?, 4326)
            )
            AND v.status = 'active'
            ORDER BY v.last_updated DESC
            LIMIT 1000
        """
        
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        results = self._execute_query(query, tuple(params))
        
        return [{
            'id': row['id'],
            'saccoId': row['sacco_id'],
            'saccoName': row['sacco_name'],
            'plateNumber': row['plate_number'],
            'capacity': row['capacity'],
            'currentPosition': {
                'lat': wkb.loads(bytes.fromhex(row['position'])).y,
                'lng': wkb.loads(bytes.fromhex(row['position'])).x
            },
            'heading': row['heading'],
            'speed': row['speed'],
            'status': row['status'],
            'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None
        } for row in results]
    
    # H3 Grid Queries
    
    def get_h3_cells_in_bounds(self, bounds: Dict[str, Any],
                              resolution: int = 9) -> List[Dict]:
        """Get H3 cells within bounds."""
        table = self._get_table_reference("h3_cells")
        
        query = f"""
            SELECT
                h3_cell_id,
                resolution,
                ST_AsWKB(h3_boundary) as h3_boundary,
                ST_AsWKB(h3_center) as h3_center,
                properties
            FROM {table}
            WHERE resolution = ?
            AND ST_Intersects(
                h3_boundary,
                ST_MakeEnvelope(?, ?, ?, ?, 4326)
            )
            LIMIT 5000
        """
        
        params = [
            resolution,
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        results = self._execute_query(query, tuple(params))
        
        return [{
            'cellId': row['h3_cell_id'],
            'resolution': row['resolution'],
            'boundary': self._wkb_to_geojson(row['h3_boundary']),
            'center': {
                'lng': wkb.loads(bytes.fromhex(row['h3_center'])).x,
                'lat': wkb.loads(bytes.fromhex(row['h3_center'])).y
            },
            'properties': row['properties']
        } for row in results]
    
    # Catalog Discovery
    
    def list_catalog_tables(self) -> List[Dict]:
        """List all available tables in the catalog (DuckDB + PostGIS)."""
        conn = self._get_connection()
        tables = []
        
        try:
            # DuckDB native tables
            result = conn.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()
            for row in result:
                tables.append({
                    'name': row[0],
                    'type': row[1],
                    'source': 'duckdb'
                })
            
            # PostGIS catalog tables
            if self.postgis_catalog:
                try:
                    result = conn.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = 'postgis'
                    """).fetchall()
                    for row in result:
                        tables.append({
                            'name': row[0],
                            'type': row[1],
                            'source': 'postgis'
                        })
                except Exception as e:
                    logger.warning(f"Could not list PostGIS tables: {e}")
        finally:
            conn.close()
        
        return tables
    
    # Health Check
    
    def health_check(self) -> Dict[str, Any]:
        """Health check query."""
        start = time.time()
        
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            conn.close()
            latency = (time.time() - start) * 1000
            
            return {
                'healthy': True,
                'latency': round(latency, 2),
                'catalog': 'postgis' if self.postgis_catalog else 'duckdb-only'
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'healthy': False,
                'latency': (time.time() - start) * 1000,
                'error': str(e)
            }


# Singleton instance for easy access
ducklake_service = DuckLakeService()