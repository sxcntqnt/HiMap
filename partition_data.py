#!/usr/bin/env python3
"""
Partition OSM and building data into spatially-organized Parquet files.

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

Dataset filtering (country, bbox) is driven by the dataset registry.
No --city argument — the pipeline is country-scoped.

Datasets can come from either registry:
    - himap.Ingestion.DataRegistry (OSM)      — reads an existing table
      in --db-path.
    - himap.Ingestion.BuildingRegistry        — reads raw parquet from
      each dataset's base_path/S3 location. Since Partitioner expects a
      DuckDB table to read from (same as OSM), building datasets are
      first materialized into --db-path as a table before the same,
      unmodified Partitioner.run_pipeline() call is made. This keeps
      the 11-stage pipeline dataset-agnostic — the only building-
      specific step is getting the parquet into a table.

Usage:
    python partition_data.py --db-path ./data/osm.duckdb --dataset canary
    python partition_data.py --db-path ./data/osm.duckdb --dataset kenya
    python partition_data.py --db-path ./data/buildings.duckdb --dataset kenya-buildings
    python partition_data.py --db-path ./data/osm.duckdb --dataset canary --dry-run

To add a new OSM dataset, register it in himap/dataset_registry.py.
To add a new building dataset, register it in himap/BuildingRegistry.py.
"""

import argparse
import logging
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))

from himap.Export.Partitioner import Partitioner
from himap.Ingestion.DataRegistry import registry
from himap.Ingestion.BuildingRegistry import building_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def resolve_dataset(dataset_key: str):
    """
    Look up a dataset key in both registries.

    Returns (config, kind) where kind is "osm" or "buildings".
    Raises ValueError if the key isn't registered anywhere.
    """
    if dataset_key in registry:
        return registry.get(dataset_key), "osm"
    if dataset_key in building_registry:
        return building_registry.get(dataset_key), "buildings"
    raise ValueError(
        f"Unknown dataset: '{dataset_key}'. "
        f"OSM datasets: {registry.list()} | "
        f"Building datasets: {building_registry.list()}"
    )


def materialize_buildings(config, db_path: str, table_name: str) -> int:
    """
    Load a building dataset's raw parquet into `table_name` inside the
    DuckDB file at db_path, so Partitioner can read it exactly like an
    OSM table. Returns the row count loaded.
    """
    conn = duckdb.connect(db_path)
    try:
        return config.materialize_to_duckdb(conn, table_name=table_name)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Partition OSM / building data — HiMap v3.0 pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
OSM datasets are defined in himap/dataset_registry.py.
Building datasets are defined in himap/BuildingRegistry.py.

Examples:
    %(prog)s --db-path ./data/osm.duckdb --dataset canary
    %(prog)s --db-path ./data/osm.duckdb --dataset kenya --output ./lake
    %(prog)s --db-path ./data/buildings.duckdb --dataset kenya-buildings
    %(prog)s --db-path ./data/osm.duckdb --dataset canary --dry-run
        """,
    )

    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file. For OSM datasets this must "
             "already contain the source table. For building datasets, "
             "this file will be created/updated with the materialized "
             "building table before partitioning runs.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help=(
            "Dataset key. Looked up in both registries — "
            f"OSM: {registry.list()} | Buildings: {building_registry.list()}"
        ),
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Source table name. Defaults to 'osm' for OSM datasets and "
             "'buildings' for building datasets.",
    )
    parser.add_argument(
        "--output",
        default="./lake",
        help="Output root for partitioned Parquet lake (default: ./lake)",
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=10,
        help="Quadtree zoom level (default: 10)",
    )
    parser.add_argument(
        "--target-features",
        type=int,
        default=50000,
        help="Target features per partition (default: 50000)",
    )
    parser.add_argument(
        "--h3-resolutions",
        default="7,8,9,10",
        help="Comma-separated H3 resolutions (default: 7,8,9,10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate dataset config and report without writing files",
    )

    args = parser.parse_args()

    # Validate dataset key early — fail fast before any DB work
    try:
        config, kind = resolve_dataset(args.dataset)
    except ValueError as e:
        logger.error(str(e))
        return 1

    table_name = args.table or ("osm" if kind == "osm" else "buildings")
    h3_resolutions = [int(r.strip()) for r in args.h3_resolutions.split(",")]

    logger.info("HiMap Partitioner v3.0")
    logger.info(f"  Dataset    : {args.dataset} ({kind})")
    logger.info(f"  Country    : {config.country}")
    logger.info(f"  Filter     : country='{config.country_filter or 'none'}' bbox={config.bbox or 'none'}")
    logger.info(f"  Source     : {args.db_path} → table '{table_name}'")
    logger.info(f"  Output     : {args.output}/{config.country.lower()}/")
    logger.info(f"  Zoom       : {args.zoom}")
    logger.info(f"  H3 res     : {h3_resolutions}")

    if args.dry_run:
        logger.info("DRY RUN — no files will be written")
        return 0

    try:
        if kind == "buildings":
            logger.info(f"Materializing '{args.dataset}' parquet → {args.db_path}::{table_name}")
            row_count = materialize_buildings(config, args.db_path, table_name)
            logger.info(f"Loaded {row_count:,} rows into '{table_name}'")

        partitioner = Partitioner(
            db_path=args.db_path,
            output_dir=args.output,
            partition_zoom=args.zoom,
            target_features_per_partition=args.target_features,
            h3_resolutions=h3_resolutions,
        )

        manifest = partitioner.run_pipeline(
            dataset_key=args.dataset,
            source_table=table_name,
        )

        # Summary
        country = config.country.lower()
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Tiles exported   : {manifest['tile_count']}")
        logger.info(f"Total features   : {manifest['total_features']:,}")
        logger.info(f"Output           : {args.output}/{country}/")
        logger.info(f"Manifest         : {args.output}/{country}/manifest.json")

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
        logger.info("")
        logger.info("Next steps:")
        if kind == "buildings":
            logger.info(
                f"  1. Flip h3_enriched=True for '{args.dataset}' in "
                f"BuildingRegistry.py so the routes layer switches to "
                f"the h3_9 query path."
            )
            logger.info(f"  2. Build DuckDB views:")
        else:
            logger.info(f"  1. Build DuckDB views:")
        logger.info(f"       from himap.view_generator import ViewGenerator")
        logger.info(f"       vg = ViewGenerator(ducklake_service)")
        logger.info(f"       vg.build('{args.dataset}')")
        logger.info(f"  {'3' if kind == 'buildings' else '2'}. Upload to storage:")
        logger.info(f"       rclone copy {args.output}/{country} r2:himap-lake/{country}")

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
