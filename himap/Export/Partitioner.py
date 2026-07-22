"""
Spatial Partitioning Pipeline — HiMap v3.0 (schema v3.1)

Contract: every feature written by this pipeline must satisfy the locked schema.
No backward compatibility with v2.0.

v3.1 note: the locked schema was extended to add nullable building-specific
columns (floorspace, occupancy, height_raw/height_m/height_floors, quadkey,
relation_id, source_provider, area_m2, perimeter_m) so that OpenBuildingMap
datasets can flow through the same 11-stage pipeline as OSM. OSM rows leave
these NULL; building rows leave the OSM semantic fields (highway, amenity,
landuse, population, road_class) NULL. This is a deliberate, additive schema
change — nothing existing was renamed or repurposed.

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
    zorder_key             BIGINT        morton(h3_cell, feature_id) — row micro-ordering
    entropy_bucket        INT           macro partition assignment (0–9)
    cell_variance         FLOAT         adaptive resolution trigger
    importance_byte       TINYINT       quantized I(S) 0–255
    entropy_score         FLOAT         Shannon entropy of feature_type distribution per H3 cell
    compressed_size_bytes BIGINT        estimated at write time
    partition_run_id      STRING        hash(H3_Index + Timestamp)
    -- OSM semantic fields (NULL for building datasets)
    highway               STRING
    building              STRING
    amenity               STRING
    landuse               STRING
    population            INT
    road_class            TINYINT
    -- Building semantic fields (NULL for OSM datasets)
    floorspace            STRING
    occupancy             STRING        RES / COM / IND / CIV / UNK
    height_raw            STRING        original encoding, e.g. 'HBET:1-2'
    height_m              DOUBLE
    height_floors         DOUBLE
    quadkey               STRING        legacy Bing quadkey, kept for interop
    relation_id           STRING
    source_provider       STRING        'OSM' / 'Google' / 'Microsoft' etc.
    area_m2               DOUBLE        computed in a projected CRS, not degrees
    perimeter_m           DOUBLE

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

Dataset entry points (stage 1 has two implementations, everything downstream
is shared):
    _build_staging_table            — OSM: reads tags MAP, feature_id, geom
    _build_staging_table_buildings   — Buildings: reads discrete columns
                                        (id, geometry, occupancy, height, ...),
                                        no tags map required
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
    ("geometry",              pa.binary()),
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
    ("building",               pa.string()),
    ("amenity",               pa.string()),
    ("landuse",               pa.string()),
    ("population",            pa.int32()),
    ("road_class",            pa.int8()),
    # Building semantic fields (v3.1)
    ("floorspace",            pa.string()),
    ("occupancy",             pa.string()),
    ("height_raw",            pa.string()),
    ("height_m",              pa.float64()),
    ("height_floors",         pa.float64()),
    ("quadkey",               pa.string()),
    ("relation_id",           pa.string()),
    ("source_provider",       pa.string()),
    ("area_m2",               pa.float64()),
    ("perimeter_m",           pa.float64()),
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
    # Building occupancy codes (lowercased) — same weight scale as OSM tags
    "civ":         0.7,
    "com":         0.6,
    "ind":         0.4,
    "res":         0.3,
    "unk":         0.1,
}

def _importance_score(
    area: float,
    building_tag: Optional[str],
    amenity_tag: Optional[str],
    neighbor_count: int,
    occupancy: Optional[str] = None,
    dist_to_traj: float = 5000.0,
) -> float:
    """
    Compute I(S) per the locked Semantic Zoom Contract formula.
    Returns float in [0, 1].
    dist_to_traj defaults to 5000m (no trajectory known at ingestion time).

    `occupancy` is the building-dataset classification (RES/COM/IND/CIV/UNK).
    It's used as a fallback tag when building_tag/amenity_tag aren't present
    (which is always, for building datasets — they don't carry OSM tags).
    """
    # A_norm: log-normalized area, prevents industrial bloat
    a_norm = min(1.0, math.log10(max(area, 1) / 500 + 1) / 2)

    # C_type: class weight from tag lookup
    tag = building_tag or amenity_tag or occupancy or ""
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
    Dataset-agnostic downstream of staging: entropy, importance, export,
    and manifest stages are identical for OSM and building datasets. Only
    stage 1 (load_source/staging) differs, because the two source schemas
    genuinely differ (OSM tags MAP vs. discrete building columns) — see
    _build_staging_table vs _build_staging_table_buildings.

    Usage:
        p = Partitioner(db_path="./data/osm.duckdb", output_dir="./lake")
        manifest = p.run_pipeline(dataset_key="kenya", source_table="osm")
        manifest = p.run_pipeline(dataset_key="kenya-buildings", source_table="buildings")
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
    # Shared: zorder_key (stage 4) — identical for every dataset kind.
    # zorder_key = morton(h3_cell, feature_id)
    # h3_8 is the spatial anchor (neighborhood scale).
    # feature_id may be a string like 'relation/1809123' or
    # 'building/796743226' — use hash(), not CAST.
    # ------------------------------------------------------------------

    def _apply_zorder_key(self, conn: duckdb.DuckDBPyConnection, staging: str) -> None:
        h3_col = f"h3_{self.h3_resolutions[1]}"  # h3_8

        conn.execute(f"ALTER TABLE {staging} ADD COLUMN zorder_key BIGINT")

        MASK31 = 2147483647   # 2^31 - 1
        SHIFT31 = 2147483648  # 2^31

        conn.execute(f"""
            UPDATE {staging}
            SET zorder_key = (
                ((h3_string_to_h3({h3_col}) % {MASK31}) * {SHIFT31})
                + (hash(feature_id) % {MASK31})
            )
            WHERE {h3_col} IS NOT NULL
        """)

        conn.execute(f"""
            UPDATE {staging}
            SET zorder_key = hash(feature_id) & 4294967295
            WHERE zorder_key IS NULL
        """)

    # ------------------------------------------------------------------
    # Stage 1 + 2 + 3 + 4 (OSM): quadtree + H3 + zorder in one SQL pass
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
            f"ST_Y(ST_Centroid(ST_GeomFromWKB(geom))), "
            f"ST_X(ST_Centroid(ST_GeomFromWKB(geom))), "
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
                    geom,

                    -- Centroid coords (computed once, stored flat for query speed)
                    ST_Y(ST_Centroid(ST_GeomFromWKB(geom)))     AS centroid_lat,
                    ST_X(ST_Centroid(ST_GeomFromWKB(geom)))     AS centroid_lng,

                    -- Country (source column is 'country', contract calls it 'country_code')
                    country                                          AS country_code,

                    -- Layer 2: H3 indices (printf pattern matches enrichment pipeline)
                    {h3_selects},

                    -- Layer 1: Quadtree address from centroid
                    {z}                                              AS tile_z,
                    CAST(FLOOR(
                        ((ST_X(ST_Centroid(ST_GeomFromWKB(geom))) + 180.0) / 360.0)
                        * POW(2, {z})
                    ) AS INTEGER)                                    AS tile_x,
                    CAST(FLOOR(
                        (1.0 - LN(
                            TAN(RADIANS(ST_Y(ST_Centroid(ST_GeomFromWKB(geom)))))
                            + 1.0 / COS(RADIANS(ST_Y(ST_Centroid(ST_GeomFromWKB(geom)))))
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
                    END::TINYINT                                     AS road_class,

                    -- Building semantic fields — not applicable to OSM rows
                    NULL::VARCHAR                                    AS floorspace,
                    NULL::VARCHAR                                    AS occupancy,
                    NULL::VARCHAR                                    AS height_raw,
                    NULL::DOUBLE                                     AS height_m,
                    NULL::DOUBLE                                     AS height_floors,
                    NULL::VARCHAR                                    AS quadkey,
                    NULL::VARCHAR                                    AS relation_id,
                    NULL::VARCHAR                                    AS source_provider,
                    NULL::DOUBLE                                     AS area_m2,
                    NULL::DOUBLE                                     AS perimeter_m

                FROM {source_table}
                WHERE {where_clause}
            """)

            self._apply_zorder_key(conn, staging)

            count = conn.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
            logger.info(f"Staging complete: {count:,} features in {staging}")
            return staging

        except Exception as e:
            logger.error(f"Staging failed for {source_table}: {e}")
            raise

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Stage 1 + 2 + 3 + 4 (Buildings): same quadtree/H3/zorder logic as
    # OSM, but reading from discrete building columns instead of a tags
    # MAP. No OSM-specific concepts (highway/amenity/landuse/population)
    # apply here — those columns are left NULL.
    # ------------------------------------------------------------------

    def _build_staging_table_buildings(self, source_table: str, config) -> str:
        """
        Stages 1–4 for OpenBuildingMap sources.

        Source schema (buildings table, materialized from parquet by
        BuildingConfig.materialize_to_duckdb):
            id          BIGINT
            floorspace  VARCHAR
            occupancy   VARCHAR    — 'UNK', 'RES', 'COM', 'IND', 'CIV'
            relation_id VARCHAR
            quadkey     VARCHAR
            last_update TIMESTAMP WITH TIME ZONE
            height      VARCHAR    — 'H:n' / 'HBET:a-b' / 'HHT:x' / numeric / NULL
            geometry    BLOB       — WKB polygon/multipolygon
            bbox        STRUCT(xmin, ymin, xmax, ymax)
            source      VARCHAR    — 'OSM' / 'Google' / 'Microsoft'

        height parsing (matches the enrichment contract):
            'H:n'       -> floors=n,        height_m = n * 3.5
            'HBET:a-b'  -> floors=(a+b)/2,  height_m = floors * 3.5
            'HHT:x'     -> height_m=x,      floors = x / 3.5
            '12.5'      -> height_m=12.5,   floors = height_m / 3.5
            NULL/other  -> both NULL

        area_m2/perimeter_m are computed via ST_Transform into config's
        utm_epsg — NOT raw ST_Area on WGS84 coordinates, which would be
        degrees² and meaningless.

        Returns the name of the staging table.
        """
        conn = self._conn()
        staging = f"_stage_{source_table}"

        h3_selects = ",\n                    ".join([
            f"printf('%x', h3_latlng_to_cell("
            f"ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))), "
            f"ST_X(ST_Centroid(ST_GeomFromWKB(geometry))), "
            f"{res})::BIGINT) AS h3_{res}"
            for res in self.h3_resolutions
        ])

        z = self.partition_zoom
        where_clause = config.quadkey_filter_sql()
        utm_epsg = getattr(config, "utm_epsg", None) or 32637  # default: UTM 37N (Kenya)

        height_m_case = """
            CASE
                WHEN height ~ '^H:[0-9]+(\\.[0-9]+)?$'
                    THEN CAST(regexp_extract(height, '^H:([0-9]+(\\.[0-9]+)?)', 1) AS DOUBLE) * 3.5
                WHEN height ~ '^HBET:[0-9]+(\\.[0-9]+)?-[0-9]+(\\.[0-9]+)?$'
                    THEN (
                        CAST(regexp_extract(height, '^HBET:([0-9]+(\\.[0-9]+)?)-', 1) AS DOUBLE)
                      + CAST(regexp_extract(height, '-([0-9]+(\\.[0-9]+)?)$', 1) AS DOUBLE)
                    ) / 2.0 * 3.5
                WHEN height ~ '^HHT:[0-9]+(\\.[0-9]+)?$'
                    THEN CAST(regexp_extract(height, '^HHT:([0-9]+(\\.[0-9]+)?)', 1) AS DOUBLE)
                WHEN TRY_CAST(height AS DOUBLE) IS NOT NULL
                    THEN TRY_CAST(height AS DOUBLE)
                ELSE NULL
            END
        """

        height_floors_case = """
            CASE
                WHEN height ~ '^H:[0-9]+(\\.[0-9]+)?$'
                    THEN CAST(regexp_extract(height, '^H:([0-9]+(\\.[0-9]+)?)', 1) AS DOUBLE)
                WHEN height ~ '^HBET:[0-9]+(\\.[0-9]+)?-[0-9]+(\\.[0-9]+)?$'
                    THEN (
                        CAST(regexp_extract(height, '^HBET:([0-9]+(\\.[0-9]+)?)-', 1) AS DOUBLE)
                      + CAST(regexp_extract(height, '-([0-9]+(\\.[0-9]+)?)$', 1) AS DOUBLE)
                    ) / 2.0
                WHEN height ~ '^HHT:[0-9]+(\\.[0-9]+)?$'
                    THEN CAST(regexp_extract(height, '^HHT:([0-9]+(\\.[0-9]+)?)', 1) AS DOUBLE) / 3.5
                WHEN TRY_CAST(height AS DOUBLE) IS NOT NULL
                    THEN TRY_CAST(height AS DOUBLE) / 3.5
                ELSE NULL
            END
        """

        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE {staging} AS
                SELECT
                    'building/' || CAST(id AS VARCHAR)              AS feature_id,
                    'building'                                       AS feature_type,

                    geometry                                         AS geom,

                    ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))     AS centroid_lat,
                    ST_X(ST_Centroid(ST_GeomFromWKB(geometry)))     AS centroid_lng,

                    '{config.country}'                               AS country_code,

                    {h3_selects},

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

                    -- OSM semantic fields — not applicable to building rows
                    NULL::VARCHAR                                    AS highway,
                    NULL::VARCHAR                                    AS building,
                    NULL::VARCHAR                                    AS amenity,
                    NULL::VARCHAR                                    AS landuse,
                    NULL::INTEGER                                    AS population,
                    NULL::TINYINT                                    AS road_class,

                    -- Building semantic fields
                    CAST(floorspace AS VARCHAR)                      AS floorspace,
                    occupancy                                        AS occupancy,
                    height                                            AS height_raw,
                    {height_m_case}                                  AS height_m,
                    {height_floors_case}                             AS height_floors,
                    quadkey                                          AS quadkey,
                    relation_id                                      AS relation_id,
                    source                                           AS source_provider,
                    ST_Area(ST_Transform(
                        ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:{utm_epsg}'
                    ))                                                AS area_m2,
                    ST_Perimeter(ST_Transform(
                        ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:{utm_epsg}'
                    ))                                                AS perimeter_m

                FROM {source_table}
                WHERE {where_clause}
            """)

            self._apply_zorder_key(conn, staging)

            count = conn.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
            logger.info(f"Staging complete: {count:,} buildings in {staging}")
            return staging

        except Exception as e:
            logger.error(f"Building staging failed for {source_table}: {e}")
            raise

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Stages 5–9: entropy, buckets, importance, hysteresis
    # These run per H3 cell (res 8) — the canonical entropy anchor.
    # Identical for OSM and building staging tables, since both now
    # produce the same superset of columns.
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
        Uses real area_m2 when available (building datasets); falls back
        to the neutral default (OSM, which has no polygon area computed)
        the same way the original implementation did.
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
                    h3_9,
                    area_m2,
                    occupancy
                FROM {staging}
            """).fetchall()

            for feature_id, building, amenity, h3_9, area_m2, occupancy in feature_rows:
                neighbor_count = density.get(h3_9, 1) - 1   # exclude self
                score = _importance_score(
                    area=area_m2 if area_m2 is not None else 500.0,
                    building_tag=building,
                    amenity_tag=amenity,
                    neighbor_count=max(0, neighbor_count),
                    occupancy=occupancy,
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
                geom AS geometry,
                centroid_lat,
                centroid_lng,
                country_code,
                h3_7, h3_8, h3_9, h3_10,
                tile_z, tile_x, tile_y,
                zorder_key,
                highway, building, amenity, landuse, population, road_class,
                floorspace, occupancy, height_raw, height_m, height_floors,
                quadkey, relation_id, source_provider, area_m2, perimeter_m
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
            "floorspace", "occupancy", "height_raw", "height_m", "height_floors",
            "quadkey", "relation_id", "source_provider", "area_m2", "perimeter_m",
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
             highway, building, amenity, landuse, pop, rc,
             floorspace, occupancy, height_raw, height_m, height_floors,
             quadkey, relation_id, source_provider, area_m2, perimeter_m) = row

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
            enriched["floorspace"].append(floorspace)
            enriched["occupancy"].append(occupancy)
            enriched["height_raw"].append(height_raw)
            enriched["height_m"].append(height_m)
            enriched["height_floors"].append(height_floors)
            enriched["quadkey"].append(quadkey)
            enriched["relation_id"].append(relation_id)
            enriched["source_provider"].append(source_provider)
            enriched["area_m2"].append(area_m2)
            enriched["perimeter_m"].append(perimeter_m)

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
            dataset_key:   Key in either registry — OSM DataRegistry
                           (e.g. 'canary', 'kenya') or BuildingRegistry
                           (e.g. 'kenya-buildings'). Controls source
                           filtering and output path.
            source_table:  DuckDB table to read from (default: 'osm').
                           For building datasets this should be the table
                           name BuildingConfig.materialize_to_duckdb() was
                           called with (default 'buildings').

        Returns:
            Manifest dict (also written to disk as manifest.json)
        """
        from ..Ingestion.DataRegistry import registry as osm_registry
        from ..Ingestion.BuildingRegistry import building_registry

        if dataset_key in osm_registry:
            config = osm_registry.get(dataset_key)
            dataset_kind = "osm"
        elif dataset_key in building_registry:
            config = building_registry.get(dataset_key)
            dataset_kind = "buildings"
        else:
            raise ValueError(
                f"Unknown dataset: '{dataset_key}'. "
                f"OSM datasets: {osm_registry.list()} | "
                f"Building datasets: {building_registry.list()}"
            )

        country_code = config.country

        logger.info(
            f"Pipeline start — dataset={dataset_key} ({dataset_kind}) "
            f"country={country_code} "
            f"filter={config.country_filter or 'none'} "
            f"bbox={config.bbox or 'none'} "
            f"source={source_table}"
        )

        # Stages 1–4: staging with filter applied from config
        staging = (
            self._build_staging_table(source_table, config)
            if dataset_kind == "osm"
            else self._build_staging_table_buildings(source_table, config)
        )

        # Stages 5–9: entropy and importance
        entropy_metrics = self._compute_entropy_metrics(staging)
        importance_map  = self._compute_importance_bytes(staging)

        # Stage 10: export per tile
        # Output path: {output_dir}/{country_lower}/            (OSM)
        #              {output_dir}/{country_lower}/buildings/  (buildings)
        # The 'buildings' subdirectory is mandatory, not cosmetic — an OSM
        # dataset and a building dataset can share the same country code
        # (e.g. 'kenya' and 'kenya-buildings' are both country='KE'), and
        # without this split they would write into the identical tile
        # paths and silently clobber each other's Parquet files.
        # No city subdirectory otherwise — partitioner is country-scoped.
        output_path = (
            self.output_dir / country_code.lower() / "buildings"
            if dataset_kind == "buildings"
            else self.output_dir / country_code.lower()
        )
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
