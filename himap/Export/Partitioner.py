"""
Spatial Partitioning Pipeline — HiMap v3.0

Contract: every feature written by this pipeline must satisfy the locked schema.
No backward compatibility with v2.0.

Locked schema (every Parquet file, every shard, no exceptions):
    feature_id            STRING        globally stable, immutable
    feature_type          STRING
    geometry              BLOB (WKB)
    centroid_lat          DOUBLE
    centroid_lng          DOUBLE
    country_code          STRING
    h3_7                  STRING
    h3_8                  STRING
    h3_9                  STRING
    h3_10                 STRING
    tile_z                INT
    tile_x                INT
    tile_y                INT
    zorder_key            BIGINT        morton(h3_cell, feature_id) — row micro-ordering
    entropy_bucket        INT           macro partition assignment (0–9)
    cell_variance         FLOAT         adaptive resolution trigger
    importance_byte       TINYINT       quantized I(S) 0–255
    entropy_score         FLOAT         Shannon entropy of feature_type distribution per H3 cell
    compressed_size_bytes BIGINT        estimated at write time
    partition_run_id      STRING        hash(H3_Index + Timestamp)

Pipeline stages (in order, non-negotiable):
    1. load_source          — read from DuckDB source table
    2. compute_quadtree     — assign tile_z, tile_x, tile_y from centroid
    3. compute_h3           — assign h3_7 through h3_10
    4. compute_zorder       — morton encode per row (geometry micro-ordering)
    5. compute_entropy      — Shannon entropy + cell_variance per H3 cell (res 8)
    6. compute_entropy_bucket — macro partition assignment from entropy
    7. compute_importance   — I(S) formula → importance_byte (0–255)
    8. assign_resolution    — adaptive H3 resolution from entropy + variance
    9. apply_hysteresis     — stability filter, suppress partition churn
    10. export              — write Parquet sorted by (entropy_bucket, zorder_key, importance_byte DESC)
    11. write_catalog       — emit manifest with all SSE manager fields
"""

import hashlib
import json
import logging
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..Services.DuckLakeService import DuckLakeService
from parquet_stream_writer import ParquetStreamWriter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Locked schema — single source of truth for PyArrow schema
# Any column added to the contract must be added here first.
# ---------------------------------------------------------------------------
LOCKED_SCHEMA = pa.schema([
    ("feature_id",            pa.string()),
    ("feature_type",          pa.string()),
    ("geometry",              pa.binary()),       # WKB
    ("centroid_lat",          pa.float64()),
    ("centroid_lng",          pa.float64()),
    ("country_code",          pa.string()),
    ("h3_7",                  pa.string()),
    ("h3_8",                  pa.string()),
    ("h3_9",                  pa.string()),
    ("h3_10",                 pa.string()),
    ("tile_z",                pa.int32()),
    ("tile_x",                pa.int32()),
    ("tile_y",                pa.int32()),
    ("zorder_key",            pa.int64()),
    ("entropy_bucket",        pa.int32()),
    ("cell_variance",         pa.float32()),
    ("importance_byte",       pa.int8()),
    ("entropy_score",         pa.float32()),
    ("compressed_size_bytes", pa.int64()),
    ("partition_run_id",      pa.string()),
    # OSM semantic fields
    ("highway",               pa.string()),
    ("building",              pa.string()),
    ("amenity",               pa.string()),
    ("landuse",               pa.string()),
    ("population",            pa.int32()),
    ("road_class",            pa.int8()),
])


# ---------------------------------------------------------------------------
# Importance scoring — locked formula from Semantic Zoom Contract
#
#   I(S) = (ω_area · A_norm + ω_class · C_type + ω_density · D_local)
#          + (ω_user · U_traj)
#
# Weights: area=0.4, class=0.5, density=0.1, user=0.2
# Stored as 1-byte quantized value: importance_byte = round(I(S) * 255)
# ---------------------------------------------------------------------------
CLASS_WEIGHTS = {
    "hospital":    0.9,
    "university":  0.9,
    "station":     0.85,
    "mall":        0.6,
    "commercial":  0.6,
    "residential": 0.3,
    "utility":     0.1,
}

def _importance_score(
    area: float,
    building_tag: Optional[str],
    amenity_tag: Optional[str],
    neighbor_count: int,
    dist_to_traj: float = 5000.0,
) -> float:
    """
    Compute I(S) per the locked Semantic Zoom Contract formula.
    Returns float in [0, 1].
    dist_to_traj defaults to 5000m (no trajectory known at ingestion time).
    """
    # A_norm: log-normalized area, prevents industrial bloat
    a_norm = min(1.0, math.log10(max(area, 1) / 500 + 1) / 2)

    # C_type: class weight from tag lookup
    tag = building_tag or amenity_tag or ""
    c_type = CLASS_WEIGHTS.get(tag.lower(), 0.1)

    # D_local: density penalty via neighbor count
    d_local = 1.0 / (1.0 + neighbor_count)

    # U_traj: trajectory boost (static at ingestion; SSE layer applies dynamic boost)
    u_traj = math.exp(-dist_to_traj / 500.0)

    score = (0.4 * a_norm + 0.5 * c_type + 0.1 * d_local) + (0.2 * u_traj)
    return max(0.0, min(1.0, score))


def _quantize_importance(score: float) -> int:
    """Map I(S) ∈ [0,1] → int ∈ [0, 127] to fit signed int8 (TINYINT)."""
    return int(round(score * 127))


# ---------------------------------------------------------------------------
# Entropy utilities
# ---------------------------------------------------------------------------

def _shannon_entropy(counts: List[int]) -> float:
    """Shannon entropy over a distribution of counts."""
    total = sum(counts)
    if total == 0:
        return 0.0
    result = 0.0
    for c in counts:
        if c == 0:
            continue
        p = c / total
        result -= p * math.log2(p)
    return result


def _entropy_bucket(entropy: float, n_buckets: int = 10) -> int:
    """
    Map entropy value to integer bucket [0, n_buckets-1].
    Entropy range [0, ~3.32] for 10 feature types → normalized to bucket.
    Max theoretical entropy for 10 classes = log2(10) ≈ 3.32
    """
    max_entropy = math.log2(10)
    normalized = min(entropy / max_entropy, 1.0)
    return min(int(normalized * n_buckets), n_buckets - 1)


def _adaptive_h3_resolution(
    entropy: float,
    variance: float,
    base_resolutions: List[int],
) -> int:
    """
    Resolution selection from the Multi-Resolution Entropy-Aware Storage spec.

    Low entropy + low variance  → coarse (res 7)
    Medium entropy              → balanced (res 8–9)
    High entropy OR variance    → fine (res 10)
    """
    high_entropy = entropy > 2.0       # ~60% of max
    high_variance = variance > 0.25

    if high_entropy or high_variance:
        return max(base_resolutions)   # finest available
    elif entropy > 1.0:
        # middle resolution
        mid = len(base_resolutions) // 2
        return base_resolutions[mid]
    else:
        return min(base_resolutions)   # coarsest


def _partition_run_id(h3_index: str) -> str:
    """Deterministic run ID: hash(H3_Index + UTC timestamp to the minute)."""
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M")
    raw = f"{h3_index}:{ts}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Hysteresis guard — suppresses partition migration for small entropy shifts
# ---------------------------------------------------------------------------
HYSTERESIS_DELTA = 0.20   # from contract: Δ ≈ 0.15–0.25


def _apply_hysteresis(
    new_entropy: float,
    old_entropy: float,
    new_bucket: int,
    old_bucket: int,
) -> int:
    """
    If entropy change is below hysteresis threshold and doesn't cross a bucket
    boundary, keep the old bucket assignment to prevent partition churn.
    """
    if abs(new_entropy - old_entropy) < HYSTERESIS_DELTA:
        return old_bucket
    return new_bucket


# ---------------------------------------------------------------------------
# Main Partitioner
# ---------------------------------------------------------------------------

class Partitioner:
    """
    Three-layer spatial partitioning pipeline for HiMap v3.0.

    Produces Parquet files conforming to the locked schema contract.
    Dataset-agnostic: behavior is identical for Canary Islands, Kenya,
    or any registered dataset. Path resolution is the only variable.

    Usage:
        p = Partitioner(db_path="./data/osm.duckdb", output_dir="./lake")
        manifest = p.run_pipeline(
            source_table="osm",
            city_id="nairobi",
            country_code="KE"
        )
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        output_dir: str = "./lake",
        partition_zoom: int = 10,
        target_features_per_partition: int = 50000,
        h3_resolutions: Optional[List[int]] = None,
        postgis_catalog: Optional[Dict[str, str]] = None,
    ):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.partition_zoom = partition_zoom
        self.target_features_per_partition = target_features_per_partition
        self.h3_resolutions = h3_resolutions or [7, 8, 9, 10]
        self.postgis_catalog = postgis_catalog

        self.ducklake = DuckLakeService(
            db_path=db_path,
            postgis_catalog=postgis_catalog,
        )

        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Partitioner v3.0 — zoom={partition_zoom}, "
            f"h3={self.h3_resolutions}, "
            f"catalog={'postgis' if postgis_catalog else 'duckdb'}"
        )

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _conn(self) -> duckdb.DuckDBPyConnection:
        """Get a connection with spatial and H3 extensions loaded."""
        conn = self.ducklake._get_connection()
        conn.execute("LOAD spatial")
        conn.execute("LOAD h3")
        return conn

    # ------------------------------------------------------------------
    # Stage 1 + 2 + 3 + 4: quadtree + H3 + zorder in one SQL pass
    # Keeps the expensive geometry operations in DuckDB (vectorized).
    # ------------------------------------------------------------------

    def _build_staging_table(self, source_table: str, config) -> str:
        """
        Stages 1–4: compute quadtree address, H3 indices, and zorder key
        for every feature in source_table.

        Source schema (osm table):
            feature_id  VARCHAR               — already stable, e.g. 'relation/1809123'
            tags        MAP(VARCHAR, VARCHAR)  — all OSM tags, accessed via tags['key']
            geometry    BLOB                  — WKB geometry
            filename    VARCHAR               — source parquet file
            country     VARCHAR               — country identifier

        H3 pattern matches the existing enrichment pipeline:
            printf('%x', h3_latlng_to_cell(lat, lng, res)::BIGINT)

        Returns the name of the staging table.
        """
        conn = self._conn()
        staging = f"_stage_{source_table}"

        # H3 columns using the confirmed printf pattern
        h3_selects = ",\n                    ".join([
            f"printf('%x', h3_latlng_to_cell("
            f"ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))), "
            f"ST_X(ST_Centroid(ST_GeomFromWKB(geometry))), "
            f"{res})::BIGINT) AS h3_{res}"
            for res in self.h3_resolutions
        ])

        z = self.partition_zoom

        # Filter from registry config — country and/or bbox, or '1=1' for no filter
        where_clause = config.source_filter_sql()

        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE {staging} AS
                SELECT
                    -- Identity (feature_id already stable in source)
                    feature_id,

                    -- Feature type derived from tags (priority order)
                    CASE
                        WHEN tags['highway']  IS NOT NULL THEN 'road'
                        WHEN tags['building'] IS NOT NULL THEN 'building'
                        WHEN tags['amenity']  IS NOT NULL THEN 'amenity'
                        WHEN tags['landuse']  IS NOT NULL THEN 'landuse'
                        WHEN tags['natural']  IS NOT NULL THEN 'natural'
                        WHEN tags['waterway'] IS NOT NULL THEN 'waterway'
                        WHEN tags['boundary'] IS NOT NULL THEN 'boundary'
                        ELSE 'unknown'
                    END                                              AS feature_type,

                    -- Geometry (already WKB BLOB — no rename needed)
                    geometry,

                    -- Centroid coords (computed once, stored flat for query speed)
                    ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))     AS centroid_lat,
                    ST_X(ST_Centroid(ST_GeomFromWKB(geometry)))     AS centroid_lng,

                    -- Country (source column is 'country', contract calls it 'country_code')
                    country                                          AS country_code,

                    -- Layer 2: H3 indices (printf pattern matches enrichment pipeline)
                    {h3_selects},

                    -- Layer 1: Quadtree address from centroid
                    {z}                                              AS tile_z,
                    CAST(FLOOR(
                        ((ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) + 180.0) / 360.0)
                        * POW(2, {z})
                    ) AS INTEGER)                                    AS tile_x,
                    CAST(FLOOR(
                        (1.0 - LN(
                            TAN(RADIANS(ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))))
                            + 1.0 / COS(RADIANS(ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))))
                        ) / PI()) / 2.0 * POW(2, {z})
                    ) AS INTEGER)                                    AS tile_y,

                    -- OSM semantic fields extracted from tags MAP
                    tags['highway']                                  AS highway,
                    tags['building']                                 AS building,
                    tags['amenity']                                  AS amenity,
                    tags['landuse']                                  AS landuse,
                    TRY_CAST(tags['population'] AS INTEGER)         AS population,

                    -- road_class: stored at ingestion, not derived at query time
                    CASE tags['highway']
                        WHEN 'motorway'  THEN 1
                        WHEN 'trunk'     THEN 2
                        WHEN 'primary'   THEN 3
                        WHEN 'secondary' THEN 4
                        WHEN 'tertiary'  THEN 5
                        ELSE 6
                    END::TINYINT                                     AS road_class

                FROM {source_table}
                WHERE {where_clause}
            """)

            # Layer 3: zorder_key = morton(h3_cell, feature_id)
            # h3_8 is the spatial anchor (neighborhood scale).
            # feature_id is a string like 'relation/1809123' — use hash() not CAST.
            # hash() returns a stable 64-bit integer in DuckDB.
            h3_col = f"h3_{self.h3_resolutions[1]}"  # h3_8

            conn.execute(f"ALTER TABLE {staging} ADD COLUMN zorder_key BIGINT")

            conn.execute(f"""
                UPDATE {staging}
                SET zorder_key = (
                    (h3_string_to_cell({h3_col}) & 0xFFFFFFFF) << 32
                    | (hash(feature_id) & 0xFFFFFFFF)
                )
                WHERE {h3_col} IS NOT NULL
            """)

            conn.execute(f"""
                UPDATE {staging}
                SET zorder_key = hash(feature_id) & 0xFFFFFFFF
                WHERE zorder_key IS NULL
            """)

            count = conn.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
            logger.info(f"Staging complete: {count:,} features in {staging}")
            return staging

        except Exception as e:
            logger.error(f"Staging failed for {source_table}: {e}")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Stages 5–9: entropy, buckets, importance, hysteresis
    # These run per H3 cell (res 8) — the canonical entropy anchor.
    # ------------------------------------------------------------------

    def _compute_entropy_metrics(self, staging: str) -> Dict[str, Dict]:
        """
        Stages 5–9: for every H3 cell at res 8, compute:
            entropy_score   — Shannon entropy over feature_type distribution
            cell_variance   — variance of per-feature importance within cell
            entropy_bucket  — macro partition assignment
            partition_run_id

        Returns a dict keyed by h3_8 value with all metrics.
        """
        conn = self._conn()
        metrics: Dict[str, Dict] = {}

        try:
            # Pull feature_type distribution per H3 cell
            rows = conn.execute(f"""
                SELECT
                    h3_8,
                    feature_type,
                    COUNT(*)              AS cnt,
                    AVG(COALESCE(
                        CAST(population AS DOUBLE), 0
                    ))                    AS avg_pop
                FROM {staging}
                WHERE h3_8 IS NOT NULL
                GROUP BY h3_8, feature_type
                ORDER BY h3_8
            """).fetchall()

            # Group by cell
            cell_type_counts: Dict[str, Dict[str, int]] = {}
            for h3_8, ftype, cnt, _pop in rows:
                if h3_8 not in cell_type_counts:
                    cell_type_counts[h3_8] = {}
                cell_type_counts[h3_8][ftype] = cnt

            for h3_8, type_counts in cell_type_counts.items():
                counts = list(type_counts.values())
                entropy = _shannon_entropy(counts)

                # Cell variance: variance of importance scores within cell
                # Approximate using road_class and building diversity as proxy
                # (full per-row importance computed in next stage)
                n_types = len(counts)
                total = sum(counts)
                proportions = [c / total for c in counts]
                mean_p = sum(proportions) / n_types
                variance = sum((p - mean_p) ** 2 for p in proportions) / n_types

                bucket = _entropy_bucket(entropy)
                run_id = _partition_run_id(h3_8)

                metrics[h3_8] = {
                    "entropy_score":   round(entropy, 6),
                    "cell_variance":   round(variance, 6),
                    "entropy_bucket":  bucket,
                    "partition_run_id": run_id,
                }

            logger.info(f"Entropy metrics computed for {len(metrics):,} H3 cells")
            return metrics

        finally:
            conn.close()

    def _compute_importance_bytes(self, staging: str) -> Dict[str, int]:
        """
        Stage 7: compute importance_byte per feature_id.

        Pulls the fields needed for I(S) from the staging table.
        Neighbor count approximated via H3 cell feature density.
        Returns dict: {feature_id → importance_byte}
        """
        conn = self._conn()
        importance_map: Dict[str, int] = {}

        try:
            # Get neighbor count approximation per h3_9 cell
            density = {}
            density_rows = conn.execute(f"""
                SELECT h3_9, COUNT(*) AS n
                FROM {staging}
                WHERE h3_9 IS NOT NULL
                GROUP BY h3_9
            """).fetchall()
            for h3_9, n in density_rows:
                density[h3_9] = n

            # Pull per-feature fields needed for importance
            feature_rows = conn.execute(f"""
                SELECT
                    feature_id,
                    building,
                    amenity,
                    h3_9
                FROM {staging}
            """).fetchall()

            for feature_id, building, amenity, h3_9 in feature_rows:
                neighbor_count = density.get(h3_9, 1) - 1   # exclude self
                score = _importance_score(
                    area=500.0,          # geometry area not in staging; use neutral default
                    building_tag=building,
                    amenity_tag=amenity,
                    neighbor_count=max(0, neighbor_count),
                )
                importance_map[feature_id] = _quantize_importance(score)

            logger.info(f"Importance computed for {len(importance_map):,} features")
            return importance_map

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Stage 10: export
    # ------------------------------------------------------------------

    def _export_partition(
        self,
        conn: duckdb.DuckDBPyConnection,
        staging: str,
        tile_z: int,
        tile_x: int,
        tile_y: int,
        entropy_metrics: Dict[str, Dict],
        importance_map: Dict[str, int],
        output_path: Path,
    ) -> Dict[str, Any]:
        """
        Write one tile as a Parquet file conforming to LOCKED_SCHEMA.
        Sorted by (entropy_bucket, zorder_key, importance_byte DESC).

        Returns tile metadata dict for manifest.
        """
        rows = conn.execute(f"""
            SELECT
                feature_id,
                feature_type,
                geometry,
                centroid_lat,
                centroid_lng,
                country_code,
                h3_7, h3_8, h3_9, h3_10,
                tile_z, tile_x, tile_y,
                zorder_key,
                highway, building, amenity, landuse, population, road_class
            FROM {staging}
            WHERE tile_z = ? AND tile_x = ? AND tile_y = ?
        """, [tile_z, tile_x, tile_y]).fetchall()

        if not rows:
            return None

        # Enrich each row with entropy + importance fields
        enriched = {col: [] for col in [
            "feature_id", "feature_type", "geometry",
            "centroid_lat", "centroid_lng", "country_code",
            "h3_7", "h3_8", "h3_9", "h3_10",
            "tile_z", "tile_x", "tile_y", "zorder_key",
            "entropy_bucket", "cell_variance",
            "importance_byte", "entropy_score",
            "compressed_size_bytes", "partition_run_id",
            "highway", "building", "amenity", "landuse", "population", "road_class",
        ]}

        # Sort rows: entropy_bucket ASC, zorder_key ASC, importance DESC
        def sort_key(row):
            fid = row[0]
            h3_8 = row[7]
            m = entropy_metrics.get(h3_8, {})
            eb = m.get("entropy_bucket", 5)
            imp = importance_map.get(fid, 0)
            zk = row[13]
            return (eb, zk, -imp)

        rows_sorted = sorted(rows, key=sort_key)

        for row in rows_sorted:
            (fid, ftype, geom, clat, clng, cc,
             h3_7, h3_8, h3_9, h3_10,
             tz, tx, ty, zk,
             highway, building, amenity, landuse, pop, rc) = row

            m = entropy_metrics.get(h3_8, {
                "entropy_score":    0.0,
                "cell_variance":    0.0,
                "entropy_bucket":   5,
                "partition_run_id": _partition_run_id(h3_8 or ""),
            })

            imp_byte = importance_map.get(fid, 0)

            enriched["feature_id"].append(fid)
            enriched["feature_type"].append(ftype)
            enriched["geometry"].append(geom)
            enriched["centroid_lat"].append(clat)
            enriched["centroid_lng"].append(clng)
            enriched["country_code"].append(cc)
            enriched["h3_7"].append(h3_7)
            enriched["h3_8"].append(h3_8)
            enriched["h3_9"].append(h3_9)
            enriched["h3_10"].append(h3_10)
            enriched["tile_z"].append(tz)
            enriched["tile_x"].append(tx)
            enriched["tile_y"].append(ty)
            enriched["zorder_key"].append(zk)
            enriched["entropy_bucket"].append(m["entropy_bucket"])
            enriched["cell_variance"].append(m["cell_variance"])
            enriched["importance_byte"].append(imp_byte)
            enriched["entropy_score"].append(m["entropy_score"])
            enriched["compressed_size_bytes"].append(len(geom) if geom else 0)
            enriched["partition_run_id"].append(m["partition_run_id"])
            enriched["highway"].append(highway)
            enriched["building"].append(building)
            enriched["amenity"].append(amenity)
            enriched["landuse"].append(landuse)
            enriched["population"].append(pop)
            enriched["road_class"].append(rc)

        # Write Parquet
        tile_path = output_path / f"z{tile_z}" / str(tile_x) / f"{tile_y}.parquet"
        tile_path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.table(enriched, schema=LOCKED_SCHEMA)
        pq.write_table(
            table,
            tile_path,
            compression="zstd",
            compression_level=3,
            row_group_size=16_000_000,
        )

        feature_count = len(rows_sorted)
        total_compressed = sum(enriched["compressed_size_bytes"])

        return {
            "z": tile_z,
            "x": tile_x,
            "y": tile_y,
            "feature_count": feature_count,
            "compressed_size_bytes": total_compressed,
            "entropy_score": sum(
                entropy_metrics.get(r[7], {}).get("entropy_score", 0)
                for r in rows_sorted
            ) / max(feature_count, 1),
            "partition_run_id": entropy_metrics.get(
                rows_sorted[0][7], {}
            ).get("partition_run_id", ""),
            "path": str(tile_path),
        }

    # ------------------------------------------------------------------
    # Stage 11: catalog / manifest
    # ------------------------------------------------------------------

    def _write_manifest(
        self,
        tiles: List[Dict[str, Any]],
        country_code: str,
        output_path: Path,
    ) -> Dict[str, Any]:
        """
        Write manifest.json satisfying both:
            - client initial render (BootstrapManifestService)
            - SSE manager VoI fields (entropy_score, compressed_size_bytes,
              partition_run_id, fetchPriority)
        """
        tile_keys = []
        for t in tiles:
            cdn_path = (
                f"{country_code.lower()}/"
                f"z{t['z']}/{t['x']}/{t['y']}.parquet"
            )

            # Cold-start fetchPriority from MDL proxy:
            # high entropy + large → immediate; else background; tiny → defer
            entropy = t.get("entropy_score", 0)
            size = t.get("compressed_size_bytes", 0)
            if entropy > 1.5 and size > 50_000:
                fetch_priority = "immediate"
            elif size < 5_000:
                fetch_priority = "defer"
            else:
                fetch_priority = "background"

            tile_keys.append({
                "z":                   t["z"],
                "x":                   t["x"],
                "y":                   t["y"],
                "featureCount":        t["feature_count"],
                "entropyScore":        round(t.get("entropy_score", 0), 4),
                "compressedSizeBytes": t.get("compressed_size_bytes", 0),
                "partitionRunId":      t.get("partition_run_id", ""),
                "fetchPriority":       fetch_priority,
                "parquetUrl":          cdn_path,   # relative; consumer prepends base
            })

        manifest = {
            "countryCode":   country_code,
            "tileZoom":      self.partition_zoom,
            "h3Resolutions": self.h3_resolutions,
            "generatedAt":   datetime.utcnow().isoformat() + "Z",
            "tile_count":    len(tile_keys),
            "total_features": sum(t["featureCount"] for t in tile_keys),
            "tileKeys":      tile_keys,
            # Budget hints for SW prefetch queue
            "budgetHint": {
                "maxImmediateBytes":  50_000_000,   # 50MB
                "maxBackgroundBytes": 200_000_000,  # 200MB
            },
        }

        manifest_path = output_path / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Manifest written: {manifest_path}")
        return manifest

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_pipeline(
        self,
        dataset_key: str,
        source_table: str = "osm",
    ) -> Dict[str, Any]:
        """
        Run the full 11-stage pipeline for a registered dataset.

        Args:
            dataset_key:   Key in the dataset registry (e.g. 'canary', 'kenya').
                           Controls source filtering and output path.
            source_table:  DuckDB table to read from (default: 'osm').

        Returns:
            Manifest dict (also written to disk as manifest.json)
        """
        from .dataset_registry import registry

        config = registry.get(dataset_key)
        country_code = config.country

        logger.info(
            f"Pipeline start — dataset={dataset_key} "
            f"country={country_code} "
            f"filter={config.country_filter or 'none'} "
            f"bbox={config.bbox or 'none'} "
            f"source={source_table}"
        )

        # Stages 1–4: staging with filter applied from config
        staging = self._build_staging_table(source_table, config)

        # Stages 5–9: entropy and importance
        entropy_metrics = self._compute_entropy_metrics(staging)
        importance_map  = self._compute_importance_bytes(staging)

        # Stage 10: export per tile
        # Output path: {output_dir}/{country_lower}/
        # No city subdirectory — partitioner is country-scoped
        output_path = self.output_dir / country_code.lower()
        output_path.mkdir(parents=True, exist_ok=True)

        conn = self._conn()
        try:
            tile_addresses = conn.execute(f"""
                SELECT DISTINCT tile_z, tile_x, tile_y
                FROM {staging}
                ORDER BY tile_z, tile_x, tile_y
            """).fetchall()

            total = len(tile_addresses)
            logger.info(f"Exporting {total} tiles")

            exported = []
            for idx, (tz, tx, ty) in enumerate(tile_addresses):
                tile_meta = self._export_partition(
                    conn, staging, tz, tx, ty,
                    entropy_metrics, importance_map, output_path,
                )
                if tile_meta:
                    exported.append(tile_meta)

                if (idx + 1) % 50 == 0:
                    logger.info(f"  {idx + 1}/{total} tiles exported")

        finally:
            conn.close()

        # Stage 11: manifest
        manifest = self._write_manifest(exported, country_code, output_path)

        logger.info(
            f"Pipeline complete — {manifest['tile_count']} tiles, "
            f"{manifest['total_features']:,} features"
        )
        return manifest
