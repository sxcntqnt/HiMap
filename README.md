# HiMap v3.0 — Spatial Data Lake API

HiMap is a FastAPI-based spatial query engine over a partitioned Parquet data lake. It serves OSM features and building footprints indexed by H3, quadtree, and entropy-based layout — optimized for progressive rendering in map clients.

## What changed from v2.0

v2.0 hardcoded table names and city identifiers across every layer. Adding a second dataset required changes in six files. v3.0 eliminates that entirely:

- **Dataset Registry** — one `registry.register()` call adds a new country
- **Town Registry** — named viewports with center + extent, linked to datasets
- **Building Registry** — separate OpenBuildingMap datasets with zoom gate
- **View Generator** — builds DuckDB views over Parquet at startup
- **Locked Schema** — 26-column PyArrow contract enforced at write time
- **Zoom-aware queries** — limit and importance threshold scale with zoom level

Dead endpoints removed: `/query/nodes`, `/query/corridors`, `/query/vehicles`, `/set-catalog`.

---

## Quick Start

```bash
pip install -r requirements.txt

# Partition OSM data for a registered dataset
python partition_data.py --db-path ./data/osm.duckdb --dataset canary

# Start the API
python run_server.py

# Docs
open http://localhost:8000/docs
```

---

## API Endpoints

### Info & Health

| Endpoint | Description |
|----------|-------------|
| `GET /` | API info, version, registered datasets |
| `GET /health` | DuckLake connectivity and latency |
| `GET /zoom/{zoom}?lat=` | Ground resolution, H3 column, semantic level at a latitude |

### Datasets

| Endpoint | Description |
|----------|-------------|
| `GET /datasets` | All registered datasets with country filter and paths |
| `GET /datasets/{key}` | Single dataset config |
| `GET /datasets/{key}/views` | DuckDB view state — debug |
| `GET /datasets/{key}/towns` | Towns linked to a dataset |

### Towns

| Endpoint | Description |
|----------|-------------|
| `GET /towns` | All registered towns |
| `GET /towns/{key}` | Town config — center, extent, bbox, area |
| `GET /towns/{key}/bbox-params` | Ready-to-use params for `/query/all` |
| `GET /towns/{key}/h3-params` | Ready-to-use params for `/query/h3` |

### Queries

| Endpoint | Description |
|----------|-------------|
| `GET /query/all?dataset=&sw_lng=...&zoom=` | Bounding box query — zoom scales limit and importance threshold |
| `GET /query/h3?dataset=&h3_index=...&zoom=` | H3 index query — zoom or resolution |

### Partitions

| Endpoint | Description |
|----------|-------------|
| `GET /partitions/{dataset}/{z}/{x}/{y}.parquet` | Serve Parquet partition (immutable, 7-day cache) |
| `GET /partitions/{dataset}/manifest` | Manifest with entropy, size, fetchPriority per tile |

### Buildings

| Endpoint | Description |
|----------|-------------|
| `GET /buildings/{dataset}?zoom=&sw_lng=...` | Building footprints — **zoom >= 14 required** |
| `GET /buildings/{dataset}/stats?zoom=&sw_lng=...` | Occupancy counts for a viewport |

---

## Zoom-Aware Queries

Every query endpoint accepts an optional `zoom` parameter. Passing it changes what you get back:

| Zoom range | Semantic level | Feature limit | Min importance |
|------------|----------------|---------------|----------------|
| 0–9 | metro | 500 | 80 (landmarks, motorways only) |
| 10–12 | city | 2000 | 40 (main roads, commercial) |
| 13–15 | neighborhood | 5000 | 0 (all features) |
| 16+ | street | 5000 | 0 (all features) |

The `/zoom/{zoom}?lat=` endpoint shows the exact ground resolution, H3 column, and semantic level at any location:

```bash
GET /zoom/12?lat=-1.2921   # Nairobi — 38.21 m/px, h3_8, city scale
GET /zoom/12?lat=28.1235   # Las Palmas — 33.69 m/px, h3_8, city scale (12% smaller)
GET /zoom/12?lat=50.0755   # Prague — 24.52 m/px, h3_8, city scale (36% smaller)
```

Ground resolution is always latitude-corrected: `equatorial_res × cos(lat)`.

### H3 resolution ↔ zoom mapping

| H3 resolution | Zoom range | Scale | Parquet column |
|---------------|------------|-------|----------------|
| 7 | 0–10 | ~5 km² metro | `h3_7` |
| 8 | 11–12 | ~0.7 km² neighborhood | `h3_8` |
| 9 | 13–14 | ~0.1 km² street block | `h3_9` |
| 10 | 15+ | ~15k m² fine-grained | `h3_10` |

---

## Buildings

Building data comes from [OpenBuildingMap](https://source.coop/tge-labs/openbuildingmap) — a separate dataset from OSM with its own schema and zoom constraints.

**Zoom gate**: buildings are only queryable at zoom >= 14. Below this level the API returns HTTP 400. Individual building footprints are invisible and meaningless at city or country scale.

**Pre-H3 filtering**: the building dataset uses bbox STRUCT columns for spatial filtering — no geometry parsing. After H3 enrichment this switches to `WHERE h3_9 = ?`.

```bash
# Occupancy breakdown first — cheap aggregate
GET /buildings/kenya-buildings/stats?zoom=15&sw_lng=36.80&sw_lat=-1.30&ne_lng=36.85&ne_lat=-1.28

# Then fetch footprints
GET /buildings/kenya-buildings?zoom=15&sw_lng=36.80&sw_lat=-1.30&ne_lng=36.85&ne_lat=-1.28&limit=500
```

---

## Adding a New Dataset

Register in `himap/dataset_registry.py`. The `country_filter` must exactly match the `country` column in the OSM source table — verify with `SELECT DISTINCT country FROM osm`.

```python
# 1. Verify the country value
# SELECT DISTINCT country FROM osm WHERE country LIKE '%tanzania%';
# → 'tanzania'

# 2. Register
registry.register(
    "tanzania",
    DatasetConfig(
        country="TZ",
        base_path="./lake/tz/",
        country_filter="tanzania",   # exact match from osm.country
    )
)
```

```bash
# 3. Run the pipeline
python partition_data.py --db-path ./data/africa.duckdb --dataset tanzania

# 4. Restart server — startup builds DuckDB views automatically
# 5. Query immediately
GET /query/all?dataset=tanzania&sw_lng=...
```

No other files change.

---

## Adding a New Town

Register in `himap/Towns/TownRegistry.py`:

```python
town_registry.register(
    "kampala",
    TownBase(
        name="Kampala",
        lat=0.3476,
        lng=32.5825,
        lat_extent=0.12,     # ~26 km north-south
        lng_extent=0.15,     # ~18 km east-west at this latitude
        dataset_key="uganda", # must be registered in dataset_registry.py
        country_code="UG",
    )
)
```

Then use the town viewport directly:

```bash
GET /towns/kampala/bbox-params?zoom=12
# → {"dataset": "uganda", "sw_lng": 32.4325, "sw_lat": 0.2276, ...}
```

---

## Partitioner Pipeline

The partitioner runs 11 stages on the `osm` source table and writes Parquet files conforming to the locked schema.

```bash
python partition_data.py --db-path ./data/osm.duckdb --dataset canary [--dry-run]
```

| Stage | Name | What it does |
|-------|------|-------------|
| 1 | load_source | Read from `osm` table — `feature_id`, `tags MAP`, `geometry BLOB`, `country` |
| 2 | compute_quadtree | Assign `tile_z/x/y` from centroid via Web Mercator |
| 3 | compute_h3 | Assign `h3_7` through `h3_10` using `printf('%x', h3_latlng_to_cell(...)::BIGINT)` |
| 4 | compute_zorder | Morton key: `(h3_8_int & 0xFFFFFFFF) << 32 \| (hash(feature_id) & 0xFFFFFFFF)` |
| 5 | compute_entropy | Shannon entropy over `feature_type` distribution per H3 cell |
| 6 | compute_entropy_bucket | Map entropy to macro partition bucket 0–9 |
| 7 | compute_importance | `I(S) = (0.4·A_norm + 0.5·C_type + 0.1·D_local) + 0.2·U_traj` → `importance_byte` 0–127 |
| 8 | assign_resolution | Adaptive H3 resolution from entropy + cell variance |
| 9 | apply_hysteresis | Suppress partition migration when entropy shift < 0.20 |
| 10 | export | Write Parquet sorted by `entropy_bucket ASC, zorder_key ASC, importance_byte DESC` |
| 11 | write_catalog | Write `manifest.json` with entropy, size, fetchPriority, and budgetHint per tile |

### Locked Schema (all 26 columns, every file)

| Column | Type | Purpose |
|--------|------|---------|
| `feature_id` | STRING | Stable OSM ID e.g. `relation/1809123` |
| `feature_type` | STRING | Derived from tags: road, building, amenity, landuse… |
| `geometry` | BLOB | WKB polygon/linestring |
| `centroid_lat` / `centroid_lng` | DOUBLE | Pre-computed centroid — avoids WKB parse at query time |
| `country_code` | STRING | ISO-2 code |
| `h3_7` … `h3_10` | STRING | H3 cell at each resolution |
| `tile_z/x/y` | INT32 | Quadtree partition address |
| `zorder_key` | INT64 | Morton encoding for sequential I/O |
| `entropy_bucket` | INT32 | Macro partition group 0–9 |
| `cell_variance` | FLOAT32 | Feature type variance within H3 cell |
| `importance_byte` | INT8 | Quantized I(S) 0–127 |
| `entropy_score` | FLOAT32 | Shannon entropy per H3 cell |
| `compressed_size_bytes` | INT64 | Estimated compressed size |
| `partition_run_id` | STRING | `hash(H3_Index + timestamp)` |
| `highway/building/amenity/landuse` | STRING | Extracted from `tags` MAP at ingestion |
| `population` | INT32 | `TRY_CAST(tags['population'] AS INT)` |
| `road_class` | INT8 | 1=motorway … 6=other |

---

## Project Structure

```
himap/
├── API/
│   ├── main.py                  # App factory — middleware, routers, startup
│   ├── Models/
│   │   ├── requests.py          # BBoxQueryParams, H3QueryParams, PartitionParams
│   │   └── responses.py         # FeaturesResponse, PartitionManifest, HealthStatus
│   └── routes/
│       ├── query.py             # /query/all, /query/h3
│       ├── partitions.py        # /partitions/{dataset}/{z}/{x}/{y}.parquet
│       ├── catalog.py           # /health, /datasets, /towns, /zoom
│       └── buildings.py         # /buildings/{dataset}
├── Services/
│   └── DuckLakeService.py       # Connection layer only — no domain queries
├── Export/
│   ├── Partitioner.py           # 11-stage pipeline, locked schema
│   └── Writer/                  # ParquetStreamWriter
├── Towns/
│   ├── TownBase.py              # Named viewport with bbox/H3 factories
│   └── TownRegistry.py          # Global town registry
├── Utils/
│   ├── Utils.py                 # Lat-corrected spatial math
│   └── Zoom.py                  # Ground resolution, H3↔zoom mapping
├── dataset_registry.py          # DatasetConfig, global registry
├── building_registry.py         # BuildingConfig, global building registry
├── view_generator.py            # DuckDB view builder + SQL factories
└── partition_data.py            # CLI entry point
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `HIMAP_LAKE_ROOT` | Root directory for partitioned Parquet lake | `./lake` |
| `HIMAP_DATASET_CANARY` | Override path for canary dataset | `./lake/es/` |
| `HIMAP_DATASET_KENYA` | Override path for kenya dataset | `./lake/ke/` |
| `HIMAP_OBM_ROOT` | OpenBuildingMap Parquet source | S3 path |
| `HOST` | API server host | `0.0.0.0` |
| `PORT` | API server port | `8000` |

S3 paths work natively via DuckDB — set `HIMAP_DATASET_*` to `s3://your-bucket/path/` and no other code changes are needed.

---

## Filtering Model

Two spatial filters operate at different layers and stack independently:

**Dataset filter** — applied at ingestion time by the Partitioner:
```sql
WHERE country = 'canary-islands'   -- exact match against osm.country column
```
This determines what goes into the Parquet files. The DuckDB view reads only those files.

**Town/viewport filter** — applied at query time by `/query/all`:
```sql
WHERE ST_Intersects(centroid_geom, ST_MakeEnvelope(sw_lng, sw_lat, ne_lng, ne_lat))
```
This scopes results to the viewport the user is looking at.

A query for Nairobi applies both: the Kenya dataset filter ensures only Kenyan OSM data is in scope, and the Nairobi town bbox narrows to the city viewport.
