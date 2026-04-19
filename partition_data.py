#!/usr/bin/env python3
"""
Partition OSM data into tiles for CDN distribution.
Runs the three-layer partitioning pipeline:
- Layer 1: Quadtree tiles (z/x/y)
- Layer 2: H3 indices (resolutions 7-10)
- Layer 3: Z-order keys (Morton encoding)

Usage:
    python partition_data.py --db-path ./data/osm.duckdb --city nairobi --country KE
    python partition_data.py --db-path ./data/osm.duckdb --city mombasa --country KE --tables osm_raw,osm_pois
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from himap.Export.TilePartitioner import TilePartitioner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Partition OSM data into tiles with three-layer indexing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s --db-path ./data/osm.duckdb --city nairobi --country KE
    %(prog)s --db-path ./data/osm.duckdb --city mombasa --country KE --tables osm_raw
    %(prog)s --db-path ./data/osm.duckdb --city lagos --country NG --output ./tiles
        """
    )
    
    parser.add_argument(
        '--db-path',
        required=True,
        help='Path to DuckDB database file'
    )
    
    parser.add_argument(
        '--city',
        default='nairobi',
        help='City identifier (default: nairobi)'
    )
    
    parser.add_argument(
        '--country',
        default='KE',
        help='ISO country code (default: KE)'
    )
    
    parser.add_argument(
        '--tables',
        default='osm_raw',
        help='Comma-separated list of tables to partition (default: osm_raw)'
    )
    
    parser.add_argument(
        '--output',
        default='./tiles',
        help='Output directory for tiles (default: ./tiles)'
    )
    
    parser.add_argument(
        '--zoom',
        type=int,
        default=10,
        help='Tile zoom level (default: 10)'
    )
    
    parser.add_argument(
        '--target-features',
        type=int,
        default=50000,
        help='Target features per tile before subdivision (default: 50000)'
    )
    
    parser.add_argument(
        '--h3-resolutions',
        default='7,8,9,10',
        help='Comma-separated H3 resolutions (default: 7,8,9,10)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually exporting'
    )
    
    args = parser.parse_args()
    
    try:
        # Parse parameters
        source_tables = [t.strip() for t in args.tables.split(',')]
        h3_resolutions = [int(r.strip()) for r in args.h3_resolutions.split(',')]
        
        logger.info(f"Starting partitioning pipeline")
        logger.info(f"  Database: {args.db_path}")
        logger.info(f"  City: {args.city}")
        logger.info(f"  Country: {args.country}")
        logger.info(f"  Tables: {source_tables}")
        logger.info(f"  Output: {args.output}")
        logger.info(f"  Zoom: {args.zoom}")
        logger.info(f"  H3 Resolutions: {h3_resolutions}")
        
        if args.dry_run:
            logger.info("DRY RUN: No files will be written")
            return
        
        # Initialize partitioner
        partitioner = TilePartitioner(
            db_path=args.db_path,
            output_dir=args.output,
            tile_zoom=args.zoom,
            target_features_per_tile=args.target_features,
            h3_resolutions=h3_resolutions
        )
        
        # Run pipeline
        manifest = partitioner.run_pipeline(
            source_tables=source_tables,
            city_id=args.city,
            country_code=args.country
        )
        
        # Print summary
        logger.info("=" * 60)
        logger.info("PARTITIONING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Tiles exported: {manifest['tile_count']}")
        logger.info(f"Total features: {manifest['total_features']:,}")
        logger.info(f"Z-order range: [{manifest['zorderRange'][0]}, {manifest['zorderRange'][1]}]")
        logger.info(f"H3-7 cells: {len(manifest['h3_7_cells'])}")
        logger.info(f"Output directory: {args.output}/{args.country.lower()}/{args.city.lower()}")
        logger.info(f"Manifest: {args.output}/{args.country.lower()}/{args.city.lower()}/manifest.json")
        
        # Print top tiles by feature count
        logger.info("")
        logger.info("Top 10 tiles by feature count:")
        for i, tile in enumerate(sorted(manifest['tileKeys'], key=lambda x: -x['featureCount'])[:10], 1):
            logger.info(f"  {i}. z={tile['z']}, x={tile['x']}, y={tile['y']}: "
                       f"{tile['featureCount']:,} features")
        
        # Save manifest summary
        summary_path = Path(args.output) / f"{args.country.lower()}_{args.city.lower()}_summary.json"
        with open(summary_path, 'w') as f:
            json.dump({
                'city': args.city,
                'country': args.country,
                'db_path': args.db_path,
                'output_dir': args.output,
                'tile_zoom': args.zoom,
                'h3_resolutions': h3_resolutions,
                'manifest': manifest
            }, f, indent=2)
        
        logger.info("")
        logger.info(f"Summary saved to: {summary_path}")
        logger.info("")
        logger.info("Next steps:")
        logger.info(f"  1. Upload tiles to CDN: rclone copy {args.output}/{args.country.lower()}/{args.city.lower()} r2:map-tiles-prod/{args.country.lower()}/{args.city.lower()}")
        logger.info(f"  2. Update BootstrapManifestService with manifest.json values")
        logger.info(f"  3. Update CITY_INDEX in bootstrap-manifest.service.ts")
        
        return 0
        
    except Exception as e:
        logger.error(f"Partitioning failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())