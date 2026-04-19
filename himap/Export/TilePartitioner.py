"""
Tile Partitioning Service
Implements the three-layer contract:
- Layer 1 (Quadtree): WHERE data lives
- Layer 2 (H3): WHAT data means (semantic clustering)
- Layer 3 (Z-order): HOW data is stored (sequential I/O)

For Africa-wide datasets, optimized for Kenya/Nairobi as reference implementation.
"""

import json
import logging
import math
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb

logger = logging.getLogger(__name__)


class TilePartitioner:
    """
    Three-layer partitioning for spatial data export.
    
    Responsibilities:
    - Assign features to quadtree tiles (z/x/y)
    - Compute H3 indices at multiple resolutions
    - Generate z-order keys for sequential reads
    - Export partitioned Parquet files for CDN distribution
    """
    
    def __init__(
        self,
        db_path: str = ":memory:",
        output_dir: str = "./tiles",
        tile_zoom: int = 10,
        target_features_per_tile: int = 50000,
        h3_resolutions: List[int] = None
    ):
        """
        Initialize tile partitioner with configuration.
        
        Args:
            db_path: Path to DuckDB database file
            output_dir: Base directory for exported tiles
            tile_zoom: Quadtree zoom level for leaf tiles (default 10)
            target_features_per_tile: Maximum features per tile before subdivision
            h3_resolutions: List of H3 resolutions to compute (default [7, 8, 9, 10])
        """
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.tile_zoom = tile_zoom
        self.target_features_per_tile = target_features_per_tile
        self.h3_resolutions = h3_resolutions or [7, 8, 9, 10]
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"TilePartitioner initialized: zoom={tile_zoom}, "
                   f"target={target_features_per_tile} features/tile")
    
    def _get_connection(self):
        """Get DuckDB connection with extensions loaded"""
        conn = duckdb.connect(self.db_path, read_only=True)
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        conn.execute("INSTALL h3")
        conn.execute("LOAD h3")
        return conn
    
    def compute_tile_address(self, lat: float, lng: float) -> Tuple[int, int, int]:
        """
        Layer 1: Quadtree partitioning
        Convert lat/lng to tile address (z, x, y) at configured zoom level.
        
        Uses Web Mercator projection for tile addressing.
        """
        n = 2 ** self.tile_zoom
        x = int((lng + 180.0) / 360.0 * n)
        y = int(
            (1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n
        )
        return (self.tile_zoom, max(0, min(n - 1, x)), max(0, min(n - 1, y)))
    
    def compute_h3_indices(self, lat: float, lng: float) -> Dict[str, str]:
        """
        Layer 2: H3 semantic indexing
        Compute H3 cell IDs at multiple resolutions.
        
        Resolutions for Africa context:
        - res 7: ~5 km² — city/metro scale
        - res 8: ~0.7 km² — district/neighborhood
        - res 9: ~0.1 km² — street block
        - res 10: ~15k m² — fine-grained
        """
        conn = self._get_connection()
        h3_cells = {}
        
        for res in self.h3_resolutions:
            try:
                result = conn.execute(
                    "SELECT h3_latlng_to_cell_string(?, ?, ?)",
                    [lat, lng, res]
                ).fetchone()
                h3_cells[f'h3_{res}'] = result[0] if result else None
            except Exception as e:
                logger.warning(f"Failed to compute H3 res {res}: {e}")
                h3_cells[f'h3_{res}'] = None
        
        conn.close()
        return h3_cells
    
    def compute_zorder_key(self, h3_cell: str, osm_id: int) -> int:
        """
        Layer 3: Z-order (Morton) encoding
        Encode H3 cell and OSM ID into a 64-bit sort key.
        
        This enables sequential disk reads when scanning tiles.
        Uses high 32 bits from H3 cell, low 32 bits from OSM ID.
        """
        if not h3_cell:
            # Fallback: use OSM ID only if no H3
            return osm_id & 0xFFFFFFFF
        
        conn = self._get_connection()
        try:
            # Convert H3 hex string to 64-bit integer
            h3_int = conn.execute(
                "SELECT h3_string_to_cell(?)", [h3_cell]
            ).fetchone()[0]
            
            # Morton encoding: (h3_low_32 << 32) | (osm_id_low_32)
            zorder = ((h3_int & 0xFFFFFFFF) << 32) | (osm_id & 0xFFFFFFFF)
            return zorder
        except Exception as e:
            logger.warning(f"Failed to compute zorder for {h3_cell}: {e}")
            return osm_id
        finally:
            conn.close()
    
    def create_partitioned_table(self, source_table: str) -> str:
        """
        Create a partitioned table with all three layers computed.
        
        Returns name of the partitioned table.
        """
        conn = self._get_connection()
        partition_table = f"{source_table}_partitioned"
        
        try:
            # Build H3 columns dynamically
            h3_columns = ", ".join([
                f"h3_{res} VARCHAR(15)"
                for res in self.h3_resolutions
            ])
            
            # Create partitioned table
            conn.execute(f"""
                CREATE OR REPLACE TABLE {partition_table} AS
                SELECT
                    osm_id,
                    feature_type,
                    name,
                    country_code,
                    wkb_geom,
                    ST_Y(ST_Centroid(ST_GeomFromWKB(wkb_geom))) as centroid_lat,
                    ST_X(ST_Centroid(ST_GeomFromWKB(wkb_geom))) as centroid_lng,
                    -- Layer 2: H3 indices (computed dynamically)
                    {', '.join([f'h3_latlng_to_cell_string(centroid_lat, centroid_lng, {res}) as h3_{res}' for res in self.h3_resolutions])},
                    -- Layer 1: Quadtree address
                    {self.tile_zoom} as tile_z,
                    CAST(FLOOR(((centroid_lng + 180.0) / 360.0) * POW(2, {self.tile_zoom})) AS INTEGER) as tile_x,
                    CAST(FLOOR((1.0 - LN(TAN(RADIANS(centroid_lat)) + 1.0 / COS(RADIANS(centroid_lat))) / PI()) / 2.0 * POW(2, {self.tile_zoom})) AS INTEGER) as tile_y,
                    -- Layer 3: Z-order (placeholder, computed in post-process)
                    CAST(NULL AS BIGINT) as zorder_key,
                    -- Feature metadata
                    highway,
                    building,
                    amenity,
                    landuse,
                    population
                FROM {source_table}
                WHERE wkb_geom IS NOT NULL
            """)
            
            # Compute z-order keys
            conn.execute(f"""
                UPDATE {partition_table}
                SET zorder_key = (
                    (h3_string_to_cell(h3_{self.h3_resolutions[1]}) & 0xFFFFFFFF) << 32 
                    | (osm_id & 0xFFFFFFFF)
                )
                WHERE h3_{self.h3_resolutions[1]} IS NOT NULL
            """)
            
            # Handle features without H3
            conn.execute(f"""
                UPDATE {partition_table}
                SET zorder_key = osm_id
                WHERE zorder_key IS NULL
            """)
            
            logger.info(f"Created partitioned table: {partition_table}")
            return partition_table
            
        except Exception as e:
            logger.error(f"Failed to create partitioned table: {e}")
            raise
        finally:
            conn.close()
    
    def export_tiles(
        self,
        partition_table: str,
        city_id: str = "nairobi",
        country_code: str = "KE"
    ) -> Dict[str, Any]:
        """
        Export partitioned tiles as Parquet files.
        
        Creates:
        - Individual tile files: {output_dir}/{country}/{city}/z{z}/{x}/{y}.parquet
        - Manifest JSON: {output_dir}/{country}/{city}/manifest.json
        
        Returns manifest data structure.
        """
        conn = self._get_connection()
        
        try:
            # Build H3 column list for export
            h3_cols = ", ".join([f"h3_{res}" for res in self.h3_resolutions])
            
            # Export tiles with partition by
            output_path = self.output_dir / country_code.lower() / city_id.lower()
            output_path.mkdir(parents=True, exist_ok=True)
            
            conn.execute(f"""
                COPY (
                    SELECT
                        osm_id,
                        feature_type,
                        name,
                        country_code,
                        wkb_geom,
                        centroid_lat,
                        centroid_lng,
                        {h3_cols},
                        zorder_key,
                        tile_z,
                        tile_x,
                        tile_y,
                        highway,
                        building,
                        amenity,
                        landuse,
                        population,
                        CASE highway
                            WHEN 'motorway' THEN 1
                            WHEN 'trunk' THEN 2
                            WHEN 'primary' THEN 3
                            WHEN 'secondary' THEN 4
                            WHEN 'tertiary' THEN 5
                            ELSE 6
                        END::TINYINT as road_class
                    FROM {partition_table}
                    ORDER BY tile_z, tile_x, tile_y, zorder_key
                ) TO '{output_path}' (
                    FORMAT PARQUET,
                    PARTITION_BY (tile_z, tile_x, tile_y),
                    COMPRESSION 'ZSTD',
                    COMPRESSION_LEVEL 3,
                    ROW_GROUP_SIZE 16000000,
                    OVERWRITE_OR_IGNORE TRUE
                )
            """)
            
            # Generate tile manifest
            manifest = self._create_manifest(conn, partition_table, city_id, country_code)
            
            logger.info(f"Exported {manifest['tile_count']} tiles to {output_path}")
            return manifest
            
        except Exception as e:
            logger.error(f"Failed to export tiles: {e}")
            raise
        finally:
            conn.close()
    
    def _create_manifest(
        self,
        conn,
        partition_table: str,
        city_id: str,
        country_code: str
    ) -> Dict[str, Any]:
        """Create manifest JSON for BootstrapManifestService"""
        
        # Get z-order range
        zrange = conn.execute(f"""
            SELECT MIN(zorder_key), MAX(zorder_key)
            FROM {partition_table}
        """).fetchone()
        
        # Get tile statistics
        tiles = conn.execute(f"""
            SELECT
                tile_z as z,
                tile_x as x,
                tile_y as y,
                COUNT(*) as feature_count,
                COUNT(*) FILTER (WHERE feature_type = 'road') as road_count,
                COUNT(*) FILTER (WHERE feature_type = 'building') as building_count
            FROM {partition_table}
            GROUP BY tile_z, tile_x, tile_y
            ORDER BY feature_count DESC
        """).fetchall()
        
        # Build manifest structure
        tile_keys = []
        for row in tiles:
            cdn_path = f"{country_code.lower()}/{city_id.lower()}/z{row[0]}/{row[1]}/{row[2]}.parquet"
            tile_keys.append({
                "z": row[0],
                "x": row[1],
                "y": row[2],
                "featureCount": row[3],
                "roadCount": row[4],
                "buildingCount": row[5],
                "parquetUrl": f"https://cdn.yourdomain.com/{cdn_path}"
            })
        
        # Get H3 cells at resolution 7 (city/metro level)
        h3_cells = conn.execute(f"""
            SELECT DISTINCT h3_7
            FROM {partition_table}
            WHERE h3_7 IS NOT NULL
        """).fetchall()
        
        manifest = {
            "cityId": city_id,
            "countryCode": country_code,
            "tileZoom": self.tile_zoom,
            "zorderRange": [zrange[0], zrange[1]] if zrange else [0, 0],
            "h3_7_cells": [row[0] for row in h3_cells],
            "tile_count": len(tile_keys),
            "total_features": sum(t["featureCount"] for t in tile_keys),
            "tileKeys": tile_keys,
            "generatedAt": datetime.now().isoformat()
        }
        
        # Write manifest JSON
        manifest_path = self.output_dir / country_code.lower() / city_id.lower() / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Created manifest: {manifest_path}")
        return manifest
    
    def run_pipeline(
        self,
        source_tables: List[str],
        city_id: str = "nairobi",
        country_code: str = "KE"
    ) -> Dict[str, Any]:
        """
        Run full partitioning pipeline on source tables.
        
        Args:
            source_tables: List of table names to partition (e.g., ['osm_raw'])
            city_id: City identifier for output path
            country_code: ISO country code
            
        Returns:
            Manifest dictionary with all tile metadata
        """
        logger.info(f"Starting partitioning pipeline for {city_id}, {country_code}")
        
        all_manifests = []
        for table in source_tables:
            logger.info(f"Processing table: {table}")
            partition_table = self.create_partitioned_table(table)
            manifest = self.export_tiles(partition_table, city_id, country_code)
            all_manifests.append(manifest)
        
        # Merge manifests if multiple tables
        merged_manifest = self._merge_manifests(all_manifests, city_id, country_code)
        
        logger.info(f"Pipeline complete: {merged_manifest['tile_count']} tiles, "
                   f"{merged_manifest['total_features']} features")
        
        return merged_manifest
    
    def _merge_manifests(
        self,
        manifests: List[Dict],
        city_id: str,
        country_code: str
    ) -> Dict:
        """Merge multiple table manifests into single manifest"""
        if len(manifests) == 1:
            return manifests[0]
        
        # Combine tile keys, handling duplicates
        tile_key_map = {}
        for m in manifests:
            for tk in m.get("tileKeys", []):
                key = (tk["z"], tk["x"], tk["y"])
                if key not in tile_key_map:
                    tile_key_map[key] = tk
                else:
                    # Merge feature counts
                    tile_key_map[key]["featureCount"] += tk["featureCount"]
        
        # Combine H3 cells
        all_h3_cells = set()
        z_min = float('inf')
        z_max = float('-inf')
        
        for m in manifests:
            all_h3_cells.update(m.get("h3_7_cells", []))
            z_range = m.get("zorderRange", [0, 0])
            z_min = min(z_min, z_range[0])
            z_max = max(z_max, z_range[1])
        
        return {
            "cityId": city_id,
            "countryCode": country_code,
            "tileZoom": self.tile_zoom,
            "zorderRange": [z_min, z_max],
            "h3_7_cells": sorted(list(all_h3_cells)),
            "tile_count": len(tile_key_map),
            "total_features": sum(t["featureCount"] for t in tile_key_map.values()),
            "tileKeys": list(tile_key_map.values()),
            "generatedAt": datetime.now().isoformat()
        }


# Convenience function for CLI usage
def partition_osm_data(
    db_path: str = ":memory:",
    output_dir: str = "./tiles",
    city_id: str = "nairobi",
    country_code: str = "KE",
    source_tables: List[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to partition OSM data for export.
    
    Example:
        manifest = partition_osm_data(
            db_path="./data/osm.duckdb",
            output_dir="./tiles",
            city_id="nairobi",
            country_code="KE",
            source_tables=["osm_raw"]
        )
    """
    partitioner = TilePartitioner(
        db_path=db_path,
        output_dir=output_dir,
        tile_zoom=10,
        target_features_per_tile=50000,
        h3_resolutions=[7, 8, 9, 10]
    )
    
    return partitioner.run_pipeline(
        source_tables=source_tables or ["osm_raw"],
        city_id=city_id,
        country_code=country_code
    )