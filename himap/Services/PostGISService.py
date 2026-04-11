import json
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from shapely import wkb
import pyarrow as pa
import pyarrow.parquet as pq
from ..Database.PostGISPool import postgis_pool

logger = logging.getLogger(__name__)

class PostGISService:
    """Optimized PostGIS service for spatial queries"""
    
    def __init__(self):
        self._ensure_pool_initialized()
    
    def _ensure_pool_initialized(self):
        """Ensure the connection pool is initialized"""
        if postgis_pool._pool is None:
            postgis_pool.initialize()
    
    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a query and return results as list of dicts"""
        try:
            with postgis_pool.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
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
    
    # Traffic Node Queries
    
    def get_nodes_in_bounds(self, bounds: Dict[str, Any], 
                           node_types: Optional[List[str]] = None,
                           min_saturation: Optional[float] = None) -> List[Dict]:
        """Get all traffic nodes within a bounding box with optimizations"""
        query = """
            SELECT 
                id,
                name,
                ST_AsBinary(geom) as geom,
                node_type,
                passenger_throughput,
                average_dwell_time,
                peak_hour,
                saturation_level,
                connected_routes
            FROM traffic_nodes
            WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            )
        """
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        if node_types:
            query += " AND node_type = ANY(%s)"
            params.append(node_types)
        
        if min_saturation is not None:
            query += " AND saturation_level >= %s"
            params.append(min_saturation)
        
        query += " ORDER BY passenger_throughput DESC LIMIT 500"
        
        results = self._execute_query(query, tuple(params))
        
        return [{
            'id': row['id'],
            'name': row['name'],
            'position': {
                'lat': row['geom'].y if hasattr(row['geom'], 'y') else 
                       wkb.loads(bytes.fromhex(row['geom'])).y,
                'lng': row['geom'].x if hasattr(row['geom'], 'x') else 
                       wkb.loads(bytes.fromhex(row['geom'])).x
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
    
    def get_node_by_id(self, node_id: str) -> Optional[Dict]:
        """Get node by ID with full details"""
        query = """
            SELECT 
                id,
                name,
                ST_AsBinary(geom) as geom,
                node_type,
                passenger_throughput,
                average_dwell_time,
                peak_hour,
                saturation_level,
                connected_routes
            FROM traffic_nodes
            WHERE id = %s
        """
        
        results = self._execute_query(query, (node_id,))
        
        if not results:
            return None
            
        row = results[0]
        geom = wkb.loads(bytes.fromhex(row['geom'])) if isinstance(row['geom'], str) else row['geom']
        
        return {
            'id': row['id'],
            'name': row['name'],
            'position': {
                'lat': geom.y,
                'lng': geom.x
            },
            'type': row['node_type'],
            'metrics': {
                'passengerThroughput': row['passenger_throughput'],
                'averageDwellTime': row['average_dwell_time'],
                'peakHour': row['peak_hour'],
                'saturationLevel': row['saturation_level']
            },
            'connectedRoutes': row['connected_routes'] or []
        }
    
    # Corridor Analytics Queries
    
    def get_corridors_in_bounds(self, bounds: Dict[str, Any]) -> List[Dict]:
        """Get corridors within bounds"""
        query = """
            SELECT 
                id,
                name,
                start_node,
                end_node,
                ST_AsBinary(geom) as geom,
                fuel_burn_rate,
                idling_hotspot_score,
                vehicle_stress_index,
                average_speed,
                peak_flow_time
            FROM corridor_analytics
            WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
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
        """Get active vehicles within bounds"""
        query = """
            SELECT 
                v.id,
                v.sacco_id,
                s.name as sacco_name,
                v.plate_number,
                v.capacity,
                ST_AsBinary(v.position) as position,
                v.heading,
                v.speed,
                v.status,
                v.last_updated
            FROM vehicles v
            JOIN saccos s ON v.sacco_id = s.id
            WHERE ST_Intersects(
                v.position,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
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
    
    def get_nearest_vehicles(self, point: Dict[str, float], 
                           limit: int = 10, 
                           max_distance: int = 5000) -> List[Dict]:
        """Get nearest vehicles to a point"""
        query = """
            SELECT 
                v.id,
                v.sacco_id,
                s.name as sacco_name,
                v.plate_number,
                v.capacity,
                ST_AsBinary(v.position) as position,
                v.heading,
                v.speed,
                v.status,
                v.last_updated,
                ST_Distance(v.position::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance
            FROM vehicles v
            JOIN saccos s ON v.sacco_id = s.id
            WHERE ST_DWithin(
                v.position::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
            AND v.status = 'active'
            ORDER BY distance ASC
            LIMIT %s
        """
        
        params = [
            point['lng'], point['lat'],  # For ST_MakePoint in ST_DWithin
            point['lng'], point['lat'],  # For ST_MakePoint in ST_DWithin distance calc
            max_distance,
            limit
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
            'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
            'distance': float(row['distance'])
        } for row in results]
    
    # H3 Grid Queries
    
    def get_h3_cells_in_bounds(self, bounds: Dict[str, Any], 
                              resolution: int = 9) -> List[Dict]:
        """Get H3 cells within bounds"""
        query = """
            SELECT 
                h3_cell_id,
                resolution,
                ST_AsBinary(h3_boundary) as h3_boundary,
                ST_AsBinary(h3_center) as h3_center,
                properties
            FROM h3_cells
            WHERE resolution = %s
            AND ST_Intersects(
                h3_boundary,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
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
    
    # GeoJSON Export
    
    def get_nodes_as_geojson(self, bounds: Dict[str, Any]) -> Dict:
        """Export traffic nodes as GeoJSON"""
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(feature)
            ) as geojson
            FROM (
                SELECT 
                    jsonb_build_object(
                        'type', 'Feature',
                        'id', id,
                        'geometry', ST_AsGeoJSON(geom)::json,
                        'properties', jsonb_build_object(
                            'name', name,
                            'node_type', node_type,
                            'passenger_throughput', passenger_throughput,
                            'saturation_level', saturation_level,
                            'peak_hour', peak_hour
                        )
                    ) as feature
                FROM traffic_nodes
                WHERE ST_Intersects(
                    geom,
                    ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                )
            ) features
        """
        
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        results = self._execute_query(query, tuple(params))
        return results[0]['geojson'] if results else {"type": "FeatureCollection", "features": []}
    
    def get_full_map_as_geojson(self, bounds: Dict[str, Any]) -> Dict:
        """Export all traffic data as GeoJSON (nodes + corridors)"""
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(feature ORDER BY type, id)
            ) as geojson
            FROM (
                SELECT 
                    'Node' as type,
                    id,
                    jsonb_build_object(
                        'type', 'Feature',
                        'id', id,
                        'geometry', ST_AsGeoJSON(geom)::json,
                        'properties', jsonb_build_object(
                            'category', 'node',
                            'name', name,
                            'node_type', node_type,
                            'throughput', passenger_throughput
                        )
                    ) as feature
                FROM traffic_nodes
                WHERE ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                
                UNION ALL
                
                SELECT 
                    'Corridor' as type,
                    id,
                    jsonb_build_object(
                        'type', 'Feature',
                        'id', id,
                        'geometry', ST_AsGeoJSON(geom)::json,
                        'properties', jsonb_build_object(
                            'category', 'corridor',
                            'name', name,
                            'fuel_burn', fuel_burn_rate,
                            'stress_index', vehicle_stress_index
                        )
                    ) as feature
                FROM corridor_analytics
                WHERE ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
            ) combined
        """
        
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat'],
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ]
        
        results = self._execute_query(query, tuple(params))
        return results[0]['geojson'] if results else {"type": "FeatureCollection", "features": []}
    
    # Utility Queries
    
    def get_bounds_stats(self, bounds: Dict[str, Any]) -> Dict[str, int]:
        """Get statistics for a bounding box"""
        query = """
            SELECT 
                (SELECT COUNT(*) FROM traffic_nodes WHERE ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))) as node_count,
                (SELECT COUNT(*) FROM vehicles WHERE ST_Intersects(position, ST_MakeEnvelope(%s, %s, %s, %s, 4326))) as vehicle_count,
                (SELECT COUNT(*) FROM corridor_analytics WHERE ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))) as corridor_count
        """
        
        # Repeat params for each subquery
        params = [
            bounds['southWest']['lng'],
            bounds['southWest']['lat'],
            bounds['northEast']['lng'],
            bounds['northEast']['lat']
        ] * 3
        
        results = self._execute_query(query, tuple(params))
        row = results[0]
        
        return {
            'nodeCount': int(row['node_count']),
            'vehicleCount': int(row['vehicle_count']),
            'corridorCount': int(row['corridor_count'])
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Health check query"""
        import time
        start = time.time()
        
        try:
            healthy = postgis_pool.health_check()
            latency = (time.time() - start) * 1000  # Convert to milliseconds
            
            return {
                'healthy': healthy,
                'latency': round(latency, 2)
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'healthy': False,
                'latency': (time.time() - start) * 1000
            }

# Singleton instance
postgis_service = PostGISService()