import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our PostGIS service and exporter
from .Services.PostGISService import postgis_service
from .Export.ParquetExporter import parquet_exporter

def parse_bounding_box(args) -> Dict[str, Any]:
    """Parse bounding box from command line arguments"""
    if args.start is None:
        logger.error("Please provide start coordinates (--start lng lat)")
        sys.exit(1)
    
    if len(args.start) != 2:
        logger.error("Please provide exactly two start coordinates (longitude latitude)")
        sys.exit(1)
    
    try:
        start_lng, start_lat = map(float, args.start)
    except ValueError:
        logger.error("Start coordinates must be numbers")
        sys.exit(1)
    
    # Determine end coordinates
    if args.end is not None:
        if len(args.end) != 2:
            logger.error("Please provide exactly two end coordinates (longitude latitude)")
            sys.exit(1)
        try:
            end_lng, end_lat = map(float, args.end)
        except ValueError:
            logger.error("End coordinates must be numbers")
            sys.exit(1)
    elif args.width is not None and args.height is not None:
        # Calculate end coordinates from start, width, height, and growth
        # This is a simplified calculation - in reality you'd need to use the growth factors
        # For now, we'll use a simple approximation
        width_deg = args.width * 0.01  # Approximate degrees per width unit
        height_deg = args.height * 0.01  # Approximate degrees per height unit
        end_lng = start_lng + width_deg
        end_lat = start_lat - height_deg  # Subtract because latitude decreases southward
    else:
        logger.error("Please provide either end coordinates or width and height")
        sys.exit(1)
    
    return {
        'southWest': {'lng': min(start_lng, end_lng), 'lat': min(start_lat, end_lat)},
        'northEast': {'lng': max(start_lng, end_lng), 'lat': max(start_lat, end_lat)}
    }

def setup_database_connection(args):
    """Setup database connection from arguments or environment"""
    # Initialize connection pool with parameters from args or environment
    from .Database.PostGISPool import postgis_pool
    
    postgis_pool.initialize(
        host=getattr(args, 'db_host', None),
        port=getattr(args, 'db_port', None),
        database=getattr(args, 'db_name', None),
        user=getattr(args, 'db_user', None),
        password=getattr(args, 'db_password', None),
        minconn=getattr(args, 'db_minconn', 1),
        maxconn=getattr(args, 'db_maxconn', 20)
    )
    
    # Test connection
    if not postgis_pool.health_check():
        logger.error("Failed to connect to PostGIS database")
        sys.exit(1)
    
    logger.info("Successfully connected to PostGIS database")

def main():
    parser = argparse.ArgumentParser(
        prog="HiMap",
        description="Extract spatial data from PostGIS and save as Parquet files",
        epilog="Example: himap output/ -start 13.0 52.0 -end 14.0 53.0 --query nodes"
    )
    
    # Required positional argument: output directory
    parser.add_argument("output", help="Directory to store the Parquet files")
    
    # Bounding box parameters
    parser.add_argument("--start", nargs="+", help="Start coordinates (longitude latitude)", default=None)
    parser.add_argument("--end", nargs="+", help="End coordinates (longitude latitude)", default=None)
    parser.add_argument("--width", help="Width in parts (approximate)", default=None, type=int)
    parser.add_argument("--height", help="Height in parts (approximate)", default=None, type=int)
    
    # Query type selection
    parser.add_argument("--query", choices=['nodes', 'corridors', 'vehicles', 'h3', 'all'], 
                       default='nodes', help="Type of data to query")
    
    # Optional filters
    parser.add_argument("--node-types", nargs="+", help="Filter by node types")
    parser.add_argument("--min-saturation", type=float, help="Minimum saturation level (0-100)")
    parser.add_argument("--h3-resolution", type=int, default=9, help="H3 resolution (default: 9)")
    parser.add_argument("--vehicle-limit", type=int, default=1000, help="Limit for vehicle queries")
    parser.add_argument("--nearest-point", nargs=2, type=float, metavar=('LNG', 'LAT'),
                       help="Find nearest vehicles to this point (longitude latitude)")
    
    # Database connection parameters
    parser.add_argument("--db-host", help="PostGIS host")
    parser.add_argument("--db-port", type=int, help="PostGIS port")
    parser.add_argument("--db-name", help="PostGIS database name")
    parser.add_argument("--db-user", help="PostGIS username")
    parser.add_argument("--db-password", help="PostGIS password")
    parser.add_argument("--db-minconn", type=int, default=1, help="Minimum connections in pool")
    parser.add_argument("--db-maxconn", type=int, default=20, help="Maximum connections in pool")
    
    # Output options
    parser.add_argument("--no-validate", action="store_true", help="Skip Parquet validation")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--geojson", action="store_true", help="Also export as GeoJSON")
    
    args = parser.parse_args()
    
    # Setup logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse bounding box
    bounds = parse_bounding_box(args)
    logger.info(f"Querying bounds: {bounds}")
    
    # Setup database connection
    setup_database_connection(args)
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Execute query based on type
    try:
        if args.query == 'nodes':
            logger.info("Fetching traffic nodes...")
            nodes = postgis_service.get_nodes_in_bounds(
                bounds, 
                node_types=args.node_types,
                min_saturation=args.min_saturation
            )
            logger.info(f"Found {len(nodes)} nodes")
            
            # Export to Parquet
            nodes_path = output_dir / "traffic_nodes.parquet"
            parquet_exporter.export_nodes(
                nodes, 
                str(nodes_path), 
                validate=not args.no_validate
            )
            
            # Optional GeoJSON export
            if args.geojson:
                geojson = postgis_service.get_nodes_as_geojson(bounds)
                geojson_path = output_dir / "traffic_nodes.geojson"
                with open(geojson_path, 'w') as f:
                    json.dump(geojson, f, indent=2)
                logger.info(f"GeoJSON exported to {geojson_path}")
        
        elif args.query == 'corridors':
            logger.info("Fetching corridors...")
            corridors = postgis_service.get_corridors_in_bounds(bounds)
            logger.info(f"Found {len(corridors)} corridors")
            
            corridors_path = output_dir / "corridors.parquet"
            parquet_exporter.export_corridors(
                corridors, 
                str(corridors_path), 
                validate=not args.no_validate
            )
            
            if args.geojson:
                geojson = postgis_service.get_full_map_as_geojson(bounds)
                geojson_path = output_dir / "map_features.geojson"
                with open(geojson_path, 'w') as f:
                    json.dump(geojson, f, indent=2)
                logger.info(f"GeoJSON exported to {geojson_path}")
        
        elif args.query == 'vehicles':
            if args.nearest_point:
                logger.info(f"Fetching nearest vehicles to point {args.nearest_point}...")
                point = {'lng': args.nearest_point[0], 'lat': args.nearest_point[1]}
                vehicles = postgis_service.get_nearest_vehicles(
                    point, 
                    limit=args.vehicle_limit
                )
            else:
                logger.info("Fetching vehicles in bounds...")
                vehicles = postgis_service.get_vehicles_in_bounds(bounds)
            
            logger.info(f"Found {len(vehicles)} vehicles")
            
            vehicles_path = output_dir / "vehicles.parquet"
            parquet_exporter.export_vehicles(
                vehicles, 
                str(vehicles_path), 
                validate=not args.no_validate
            )
        
        elif args.query == 'h3':
            logger.info(f"Fetching H3 cells (resolution {args.h3_resolution})...")
            h3_cells = postgis_service.get_h3_cells_in_bounds(
                bounds, 
                resolution=args.h3_resolution
            )
            logger.info(f"Found {len(h3_cells)} H3 cells")
            
            h3_path = output_dir / "h3_cells.parquet"
            parquet_exporter.export_h3_cells(
                h3_cells, 
                str(h3_path), 
                validate=not args.no_validate
            )
        
        elif args.query == 'all':
            logger.info("Fetching all data types...")
            
            # Nodes
            nodes = postgis_service.get_nodes_in_bounds(
                bounds, 
                node_types=args.node_types,
                min_saturation=args.min_saturation
            )
            nodes_path = output_dir / "traffic_nodes.parquet"
            parquet_exporter.export_nodes(
                nodes, 
                str(nodes_path), 
                validate=not args.no_validate
            )
            
            # Corridors
            corridors = postgis_service.get_corridors_in_bounds(bounds)
            corridors_path = output_dir / "corridors.parquet"
            parquet_exporter.export_corridors(
                corridors, 
                str(corridors_path), 
                validate=not args.no_validate
            )
            
            # Vehicles
            vehicles = postgis_service.get_vehicles_in_bounds(bounds)
            vehicles_path = output_dir / "vehicles.parquet"
            parquet_exporter.export_vehicles(
                vehicles, 
                str(vehicles_path), 
                validate=not args.no_validate
            )
            
            # H3 cells
            h3_cells = postgis_service.get_h3_cells_in_bounds(
                bounds, 
                resolution=args.h3_resolution
            )
            h3_path = output_dir / "h3_cells.parquet"
            parquet_exporter.export_h3_cells(
                h3_cells, 
                str(h3_path), 
                validate=not args.no_validate
            )
            
            # Combined GeoJSON
            if args.geojson:
                geojson = postgis_service.get_full_map_as_geojson(bounds)
                geojson_path = output_dir / "map_features.geojson"
                with open(geojson_path, 'w') as f:
                    json.dump(geojson, f, indent=2)
                logger.info(f"GeoJSON exported to {geojson_path}")
        
        logger.info(f"Export completed successfully. Files saved to: {output_dir.absolute()}")
        
    except Exception as e:
        logger.error(f"Error during execution: {e}", exc_info=args.verbose)
        sys.exit(1)
    finally:
        # Clean up connections
        from .Database.PostGISPool import postgis_pool
        postgis_pool.close_all()

if __name__ == "__main__":
    main()