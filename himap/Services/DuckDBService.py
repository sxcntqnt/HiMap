import json
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb

logger = logging.getLogger(__name__)

class DuckDBService:
    """DuckDB service for spatial queries - future replacement for PostGIS"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._connection = None
        self._initialize_extensions()
    
    def _get_connection(self):
        """Get or create DuckDB connection"""
        if self._connection is None:
            self._connection = duckdb.connect(self.db_path)
        return self._connection
    
    def _initialize_extensions(self):
        """Initialize required DuckDB extensions"""
        try:
            conn = self._get_connection()
            # Install and load spatial extension
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")
            logger.info("DuckDB spatial extension loaded")
        except Exception as e:
            logger.error(f"Failed to load DuckDB extensions: {e}")
            raise
    
    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a query and return results as list of dicts"""
        try:
            conn = self._get_connection()
            result = conn.execute(query, params)
            # Convert to list of dictionaries
            columns = [desc[0] for desc in description] if (description := result.description) else []
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"DuckDB query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def _wkb_to_geojson(self, wkb_hex: str) -> Dict:
        """Convert WKB hex to GeoJSON geometry"""
        try:
            geom = wkb.loads(bytes.fromhex(wkb_hex), hex=True)
            return json.loads(geom.__geo_interface__)
        except Exception as e:
            logger.error(f"WKB to GeoJSON conversion failed: {e}")
            return {"type": "Point", "coordinates": [0, 0]}
    
    # Traffic Node Queries (similar to PostGIS but adapted for DuckDB)
    
    def get_nodes_in_bounds(self, bounds: Dict[str, Any], 
                           node_types: Optional[List[str]] = None,
                           min_saturation: Optional[float] = None) -> List[Dict]:
        """Get all traffic nodes within a bounding box"""
        # This assumes we have a nodes table with WKB geometry
        query = """
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
            FROM traffic_nodes
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
    
    # Other methods would follow similar patterns...
    # For brevity, I'm implementing the core structure
    
    def health_check(self) -> Dict[str, Any]:
        """Health check query"""
        import time
        start = time.time()
        
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            latency = (time.time() - start) * 1000  # Convert to milliseconds
            
            return {
                'healthy': True,
                'latency': round(latency, 2)
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'healthy': False,
                'latency': (time.time() - start) * 1000
            }

# Singleton instance for easy migration
duckdb_service = DuckDBService()