import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import logging
from typing import List, Dict, Any, Union
import subprocess
import json

logger = logging.getLogger(__name__)

class ParquetExporter:
    """Optimized Parquet export with validation"""
    
    @staticmethod
    def export_nodes(nodes: List[Dict], output_path: str, validate: bool = True) -> str:
        """Export traffic nodes to Parquet format"""
        if not nodes:
            logger.warning("No nodes to export")
            return output_path
        
        # Flatten the data for efficient Parquet storage
        flattened_data = []
        for node in nodes:
            flat_node = {
                'id': node['id'],
                'name': node['name'],
                'type': node['type'],
                'lat': node['position']['lat'],
                'lng': node['position']['lng'],
                'passengerThroughput': node['metrics']['passengerThroughput'],
                'averageDwellTime': node['metrics']['averageDwellTime'],
                'peakHour': node['metrics']['peakHour'],
                'saturationLevel': node['metrics']['saturationLevel'],
                'connectedRoutes': json.dumps(node['connectedRoutes']) if node['connectedRoutes'] else '[]'
            }
            flattened_data.append(flat_node)
        
        # Create PyArrow table
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with compression
        pq.write_table(
            table, 
            output_path,
            compression='SNAPPY',  # Good balance of speed and compression
            use_dictionary=True,   # Efficient for repeated values
            write_statistics=True  # Enable query optimizations
        )
        
        logger.info(f"Exported {len(nodes)} nodes to {output_path}")
        
        if validate:
            ParquetExporter._validate_parquet(output_path)
        
        return output_path
    
    @staticmethod
    def export_corridors(corridors: List[Dict], output_path: str, validate: bool = True) -> str:
        """Export corridors to Parquet format"""
        if not corridors:
            logger.warning("No corridors to export")
            return output_path
        
        # Flatten corridor data
        flattened_data = []
        for corridor in corridors:
            flat_corridor = {
                'id': corridor['id'],
                'name': corridor['name'],
                'startNode': corridor['startNode'],
                'endNode': corridor['endNode'],
                'fuelBurnRate': corridor['metrics']['fuelBurnRate'],
                'idlingHotspotScore': corridor['metrics']['idlingHotspotScore'],
                'vehicleStressIndex': corridor['metrics']['vehicleStressIndex'],
                'averageSpeed': corridor['metrics']['averageSpeed'],
                'peakFlowTime': corridor['metrics']['peakFlowTime'],
                'geometry_wkt': ParquetExporter._geometry_to_wkt(corridor['geometry'])
            }
            flattened_data.append(flat_corridor)
        
        # Create PyArrow table
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with compression
        pq.write_table(
            table, 
            output_path,
            compression='SNAPPY',
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Exported {len(corridors)} corridors to {output_path}")
        
        if validate:
            ParquetExporter._validate_parquet(output_path)
        
        return output_path
    
    @staticmethod
    def export_vehicles(vehicles: List[Dict], output_path: str, validate: bool = True) -> str:
        """Export vehicles to Parquet format"""
        if not vehicles:
            logger.warning("No vehicles to export")
            return output_path
        
        # Flatten vehicle data
        flattened_data = []
        for vehicle in vehicles:
            flat_vehicle = {
                'id': vehicle['id'],
                'saccoId': vehicle['saccoId'],
                'saccoName': vehicle['saccoName'],
                'plateNumber': vehicle['plateNumber'],
                'capacity': vehicle['capacity'],
                'lat': vehicle['currentPosition']['lat'],
                'lng': vehicle['currentPosition']['lng'],
                'heading': vehicle['heading'],
                'speed': vehicle['speed'],
                'status': vehicle['status'],
                'lastUpdated': vehicle['lastUpdated']
            }
            flattened_data.append(flat_vehicle)
        
        # Create PyArrow table
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with compression
        pq.write_table(
            table, 
            output_path,
            compression='SNAPPY',
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Exported {len(vehicles)} vehicles to {output_path}")
        
        if validate:
            ParquetExporter._validate_parquet(output_path)
        
        return output_path
    
    @staticmethod
    def export_h3_cells(cells: List[Dict], output_path: str, validate: bool = True) -> str:
        """Export H3 cells to Parquet format"""
        if not cells:
            logger.warning("No H3 cells to export")
            return output_path
        
        # Flatten H3 cell data
        flattened_data = []
        for cell in cells:
            flat_cell = {
                'cellId': cell['cellId'],
                'resolution': cell['resolution'],
                'lat': cell['center']['lat'],
                'lng': cell['center']['lng'],
                'properties': json.dumps(cell['properties']) if cell['properties'] else '{}'
            }
            flattened_data.append(flat_cell)
        
        # Create PyArrow table
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with compression
        pq.write_table(
            table, 
            output_path,
            compression='SNAPPY',
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Exported {len(cells)} H3 cells to {output_path}")
        
        if validate:
            ParquetExporter._validate_parquet(output_path)
        
        return output_path
    
    @staticmethod
    def export_geojson_feature_collection(geojson: Dict, output_path: str, validate: bool = True) -> str:
        """Export GeoJSON FeatureCollection to Parquet format"""
        if not geojson or 'features' not in geojson or not geojson['features']:
            logger.warning("No features to export")
            return output_path
        
        # Flatten GeoJSON features
        flattened_data = []
        for feature in geojson['features']:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            
            flat_feature = {
                'id': str(feature.get('id', '')),
                'type': feature.get('type', 'Feature'),
                'geometry_type': geom.get('type', 'Unknown') if geom else 'Unknown',
                'geometry_wkt': ParquetExporter._geometry_to_wkt(geom) if geom else None,
                **{f'prop_{k}': v for k, v in props.items()}  # Prefix properties to avoid conflicts
            }
            flattened_data.append(flat_feature)
        
        # Create PyArrow table
        df = pd.DataFrame(flattened_data)
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with compression
        pq.write_table(
            table, 
            output_path,
            compression='SNAPPY',
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Exported {len(geojson['features'])} GeoJSON features to {output_path}")
        
        if validate:
            ParquetExporter._validate_parquet(output_path)
        
        return output_path
    
    @staticmethod
    def _geometry_to_wkt(geometry: Dict) -> str:
        """Convert GeoJSON geometry to WKT string"""
        try:
            from shapely.geometry import shape
            geom = shape(geometry)
            return geom.wkt
        except Exception as e:
            logger.error(f"Error converting geometry to WKT: {e}")
            return "POINT EMPTY"
    
    @staticmethod
    def _validate_parquet(file_path: str) -> bool:
        """Validate Parquet file using gpq or PyArrow"""
        try:
            # Try using gpq if available
            result = subprocess.run(
                ['gpq', file_path], 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Parquet validation passed for {file_path}")
                return True
            else:
                logger.warning(f"gpq validation failed: {result.stderr}")
                # Fall back to PyArrow validation
                return ParquetExporter._validate_with_pyarrow(file_path)
                
        except FileNotFoundError:
            logger.info("gpq not found, using PyArrow validation")
            return ParquetExporter._validate_with_pyarrow(file_path)
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return ParquetExporter._validate_with_pyarrow(file_path)
    
    @staticmethod
    def _validate_with_pyarrow(file_path: str) -> bool:
        """Validate Parquet file using PyArrow"""
        try:
            # Try to read the Parquet file metadata
            pq.read_metadata(file_path)
            # Try to read a small sample
            table = pq.read_table(file_path, use_threads=True)
            logger.info(f"PyArrow validation passed for {file_path}: {table.num_rows} rows, {table.num_columns} columns")
            return True
        except Exception as e:
            logger.error(f"PyArrow validation failed for {file_path}: {e}")
            return False

# Singleton instance
parquet_exporter = ParquetExporter()