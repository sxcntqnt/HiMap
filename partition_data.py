#!/usr/bin/env python3
"""
Partition OSM data into spatially-organized Parquet files.

Runs the HiMap v3.0 eleven-stage pipeline:
    1.  load_source
    2.  compute_quadtree      (z/x/y tile address)
    3.  compute_h3            (resolutions 7–10)
    4.  compute_zorder        (Morton encoding — row micro-ordering)
    5.  compute_entropy       (Shannon entropy per H3 cell)
    6.  compute_entropy_bucket (macro partition assignment)
    7.  compute_importance    (I(S) → importance_byte 0–255)
    8.  assign_resolution     (adaptive H3 resolution)
    9.  apply_hysteresis      (stability filter)
    10. export                (Parquet, sorted by entropy_bucket + zorder + importance)
    11. write_catalog         (manifest.json with SSE manager fields)

Output: spatially-partitioned Parquet files for analytical queries.
NOT map tiles. z/x/y addresses geographic organization, not rendering zoom.

Usage:
    python partition_data.py --db-path ./data/osm.duckdb --city nairobi --country KE
    python partition_data.py --db-path ./data/osm.duckdb --city mombasa --country KE
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from himap.Export.Partitioner import Partitioner   # v3.0 — no TilePartitioner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Partition OSM data — HiMap v3.0 pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s --db-path ./data/osm.duckdb --city nairobi --country KE
    %(prog)s --db-path ./data/osm.duckdb --city lagos   --country NG --output ./lake
    %(prog)s --db-path ./data/osm.duckdb --city nairobi --country KE --dry-run
        """,
    )

    parser.add_argument("--db-path",   required=True,  help="Path to DuckDB database file")
    parser.add_argument("--city",      default="nairobi", help="City identifier (default: nairobi)")
    parser.add_argument("--country",   default="KE",      help="ISO-2 country code (default: KE)")
    parser.add_argument("--table",     default="osm_raw",
                        help="Source table in DuckDB (default: osm_raw)")
    parser.add_argument("--output",    default="./lake",
                        help="Output directory for partitioned Parquet lake (default: ./lake)")
    parser.add_argument("--zoom",      type=int, default=10,
                        help="Quadtree zoom level (default: 10)")
    parser.add_argument("--target-features", type=int, default=50000,
                        help="Target features per partition (default: 50000)")
    parser.add_argument("--h3-resolutions", default="7,8,9,10",
                        help="Comma-separated H3 resolutions (default: 7,8,9,10)")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Validate config and report without writing files")

    args = parser.parse_args()

    h3_resolutions = [int(r.strip()) for r in args.h3_resolutions.split(",")]

    logger.info("HiMap Partitioner v3.0")
    logger.info(f"  Database : {args.db_path}")
    logger.info(f"  City     : {args.city}")
    logger.info(f"  Country  : {args.country}")
    logger.info(f"  Table    : {args.table}")
    logger.info(f"  Output   : {args.output}")
    logger.info(f"  Zoom     : {args.zoom}")
    logger.info(f"  H3 res   : {h3_resolutions}")

    if args.dry_run:
        logger.info("DRY RUN — no files will be written")
        return 0

    try:
        partitioner = Partitioner(
            db_path=args.db_path,
            output_dir=args.output,
            partition_zoom=args.zoom,
            target_features_per_partition=args.target_features,
            h3_resolutions=h3_resolutions,
        )

        manifest = partitioner.run_pipeline(
            source_table=args.table,
            city_id=args.city,
            country_code=args.country,
        )

        # Summary
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Tiles exported   : {manifest['tile_count']}")
        logger.info(f"Total features   : {manifest['total_features']:,}")
        logger.info(f"Output           : {args.output}/{args.country.lower()}/{args.city.lower()}")
        logger.info(f"Manifest         : {args.output}/{args.country.lower()}/{args.city.lower()}/manifest.json")

        # Top 10 tiles by feature count
        logger.info("")
        logger.info("Top 10 tiles by feature count:")
        top = sorted(manifest["tileKeys"], key=lambda t: -t["featureCount"])[:10]
        for i, t in enumerate(top, 1):
            logger.info(
                f"  {i:2}. z={t['z']} x={t['x']} y={t['y']} — "
                f"{t['featureCount']:,} features  "
                f"entropy={t['entropyScore']:.3f}  "
                f"priority={t['fetchPriority']}"
            )

        # Next steps
        country = args.country.lower()
        city    = args.city.lower()
        logger.info("")
        logger.info("Next steps:")
        logger.info(f"  1. Register dataset:")
        logger.info(f"       registry.register('{city}', DatasetConfig(")
        logger.info(f"           country='{args.country}',")
        logger.info(f"           base_path='{args.output}/{country}/{city}/'")
        logger.info(f"       ))")
        logger.info(f"  2. Build DuckDB view:")
        logger.info(f"       view_generator.create_enriched_view('{city}', config)")
        logger.info(f"  3. Upload to storage:")
        logger.info(f"       rclone copy {args.output}/{country}/{city} r2:himap-lake/{country}/{city}")

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
