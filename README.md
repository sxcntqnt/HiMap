# HiMap v3.0 — Spatial Data Lake API

HiMap is a FastAPI-based spatial query engine over a partitioned Parquet data lake. It serves OSM features and building footprints indexed by H3, quadtree, and entropy-based layout — optimized for progressive rendering in map clients.

## What changed from v2.0

v2.0 hardcoded table names and city identifiers across every layer. Adding a second dataset required changes in six files. v3.0 eliminates that entirely:

- **Dataset Registry** — one `registry.register()` call adds a new country
- **Town Registry** — named viewports with center + extent, linked to datasets
- **Building Registry** — separate OpenBuildingMap datasets with zoom gate, now sharing the same H3 partitioner pipeline as OSM
- **Registry package** — `Registry/` (top-level) holds curated + auto-discovered town registrations, separate from the `TownRegistry` class itself
- **View Generator** — builds DuckDB views over Parquet at startup, for both OSM (`ViewGenerator`) and buildings (`BuildingViewGenerator`)
- **Locked Schema** — 36-column PyArrow contract enforced at write time (26 OSM/shared columns + 10 nullable building-specific columns), shared by both dataset kinds
- **Zoom-aware queries** — limit and importance threshold scale with zoom level

Dead endpoints removed: `/query/nodes`, `/query/corridors`, `/query/vehicles`, `/set-catalog`.

---

## Quick Start

```bash
pip install -r requirements.txt

# Partition OSM data for a registered dataset
python partition_data.py --db-path ./data/osm.duckdb --dataset canary

# Partition a building dataset — materializes its parquet into --db-path first,
# then runs the same 11-stage pipeline
python partition_data.py --db-path ./data/buildings.duckdb --dataset kenya-buildings

# Start the API
python run_server.py

# Docs
open http://localhost:9910/docs
```

---

## API Endpoints

### Info & Health

| Endpoint | Description |
|----------|-------------|
| `GET /` | API info, version, registered datasets |
| `GET /health` | DuckLake connectivity and latency |
| `GET /zoom/{zoom}?lat=` | Ground resolution, H3 column, semantic level at a latitude |

### Datasets (OSM)

| Endpoint | Description |
|----------|-------------|
| `GET /datasets` | All registered datasets with country filter and paths |
| `GET /datasets/{key}` | Single dataset config |
| `GET /datasets/{key}/views` | DuckDB view state — debug |
| `GET /datasets/{key}/towns` | Towns linked to a dataset |

### Building datasets

| Endpoint | Description |
|----------|-------------|
| `GET /buildings-datasets` | All registered building datasets |
| `GET /buildings-datasets/{key}` | Single building dataset config |
| `GET /buildings-datasets/{key}/views` | DuckDB view state (raw/partitioned/enriched tiers) — debug |
| `GET /buildings-datasets/{key}/towns` | Towns linked to a building dataset (via its `osm_dataset_key` back-reference) |

### Towns

| Endpoint | Description |
|----------|-------------|
| `GET /towns` | All registered towns (curated + auto-generated) |
| `GET /towns/{key}` | Town config — center, extent, bbox, area, `has_buildings` |
| `GET /towns/{key}/bbox-params` | Ready-to-use params for `/query/all` |
| `GET /towns/{key}/buildings-bbox-params` | Ready-to-use params for `/buildings/{dataset}` — enforces the buildings zoom gate |
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
| `GET /buildings/{dataset}?zoom=&sw_lng=...` | Building footprints — **zoom >= 14 required**. Reads the pre-H3 (bbox/quadkey) or post-H3 (`h3_9`) view depending on `h3_enriched` |
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

Building data comes from [OpenBuildingMap](https://source.coop/tge-labs/openbuildingmap) — a separate dataset from OSM with its own schema, and now with two distinct lifecycle stages:

**Zoom gate**: buildings are only queryable at zoom >= 14 (`BuildingConfig.zoom_gate`, per dataset). Below this level the API returns HTTP 400. Individual building footprints are invisible and meaningless at city or country scale.

**Pre-H3 (`h3_enriched=False`, the default for a newly-registered dataset)**: queries read `{dataset}_buildings_view`, a pass-through over the raw OpenBuildingMap parquet, filtered by the `bbox` STRUCT columns plus a `quadkey` prefix guard — no geometry parsing. Geometry returned is a Point approximated from the bbox center.

**Post-H3 (`h3_enriched=True`, set once `partition_data.py` has run for the dataset and `BuildingViewGenerator.build()` has been called again)**: queries read `{dataset}_buildings_partitioned`, the enriched, H3-partitioned lake output — filtered by `WHERE h3_9 IN (...)`. Geometry returned is the true polygon, plus `area_m2`, `perimeter_m`, `height_m`, `height_floors`.

```bash
# Occupancy breakdown first — cheap aggregate
GET /buildings/kenya-buildings/stats?zoom=15&sw_lng=36.80&sw_lat=-1.30&ne_lng=36.85&ne_lat=-1.28

# Then fetch footprints
GET /buildings/kenya-buildings?zoom=15&sw_lng=36.80&sw_lat=-1.30&ne_lng=36.85&ne_lat=-1.28&limit=500
```

### Adding a New Building Dataset

Register in `himap/Ingestion/BuildingRegistry.py`:

```python
building_registry.register(
    "tanzania-buildings",
    BuildingConfig(
        country="TZ",
        base_path=os.getenv("HIMAP_BUILDINGS_TANZANIA", f"{_OBM_ROOT}/"),
        osm_dataset_key="tanzania",           # links towns via for_osm_dataset()
        quadkey_prefixes=["1213"],              # verify against your actual data
        utm_epsg=32736,                        # UTM zone covering Tanzania — needed
                                                 # for true area_m2/perimeter_m
    )
)
```

```bash
# 1. Query immediately via the pre-H3 bbox/quadkey path — no partitioning needed yet
GET /buildings/tanzania-buildings?zoom=15&sw_lng=...

# 2. When ready to enrich: materializes parquet into --db-path, then runs the
#    same 11-stage pipeline used for OSM
python partition_data.py --db-path ./data/buildings.duckdb --dataset tanzania-buildings

# 3. Flip h3_enriched=True for this key in BuildingRegistry.py

# 4. Rebuild its views (or restart the server — startup calls
#    BuildingViewGenerator.build_all(), but only picks up datasets that were
#    already h3_enriched at boot time; a dataset enriched mid-lifetime needs
#    build() called again explicitly)
```

---

## Adding a New Dataset (OSM)

Register in `himap/Ingestion/DataRegistry.py`. The `country_filter` must exactly match the `country` column in the OSM source table — verify with `SELECT DISTINCT country FROM osm`.

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

## Towns

Two ways a town ends up registered, in priority order:

1. **Curated** (`himap/Registry/curated_towns.py`) — hand-picked entries with tuned extents (Nairobi, Mombasa, Kisumu, Las Palmas, Santa Cruz, Prague). These register first and always win on a key collision.
2. **Auto-generated** (`himap/Registry/auto_registry.py`) — discovers every `TownBase` subclass under `himap/Towns/towns/` (produced by `generate_towns.py`), instantiates it, derives a lowercase-hyphenated key from its `name` field, and registers whatever the curated set didn't already claim. Nairobi/Mombasa/Kisumu deliberately exist in both sets — `generate_towns.py`'s `DEFAULT_EXTENTS` matches the curated values for those three, and the auto-registration step just skips them rather than raising.

Both run automatically at import time via `himap/Towns/__init__.py` — you don't call them yourself in normal use.

### Adding towns in bulk

```bash
cd himap/Towns
python generate_towns.py towns.txt ./towns --country Kenya --dataset kenya --country-code KE
```

This writes one `TownBase` subclass per line in `towns.txt` into `Towns/towns/`, plus a `towns/__init__.py` if one doesn't exist. They're picked up automatically the next time `himap.Towns` is imported — no manual registration step.

### Adding one town by hand

For anything that needs a hand-tuned extent, or isn't in `towns.txt`, add it to `himap/Registry/curated_towns.py` instead:

```python
registry.register(
    "kampala",
    TownBase(
        name="Kampala",
        lat=0.3476,
        lng=32.5825,
        lat_extent=0.12,     # ~26 km north-south
        lng_extent=0.15,     # ~18 km east-west at this latitude
        dataset_key="uganda", # must be registered in DataRegistry.py
        country_code="UG",
    )
)
```

Then use the town viewport directly:

```bash
GET /towns/kampala/bbox-params?zoom=12
# → {"dataset": "uganda", "sw_lng": 32.4325, "sw_lat": 0.2276, ...}
```

If a building dataset is later registered with `osm_dataset_key="uganda"`, `/towns/kampala/buildings-bbox-params` becomes usable automatically — no change needed on the town side.

---

## Partitioner Pipeline

The partitioner runs the same 11 stages regardless of dataset kind — only stage 1 (staging) differs between OSM and buildings, since the two source schemas genuinely differ (a `tags` MAP vs. discrete building columns). Everything from stage 5 onward operates on the shared, widened schema.

```bash
python partition_data.py --db-path ./data/osm.duckdb --dataset canary [--dry-run]
python partition_data.py --db-path ./data/buildings.duckdb --dataset kenya-buildings [--dry-run]
```

| Stage | Name | What it does |
|-------|------|-------------|
| 1 | load_source | OSM: read `osm` table (`feature_id`, `tags MAP`, `geometry BLOB`, `country`). Buildings: read the table `materialize_to_duckdb()` populated from parquet (`id`, `occupancy`, `height`, `geometry`, `bbox`, ...) |
| 2 | compute_quadtree | Assign `tile_z/x/y` from centroid via Web Mercator |
| 3 | compute_h3 | Assign `h3_7` through `h3_10` using `printf('%x', h3_latlng_to_cell(...)::BIGINT)` — geometry-generic, works on polygons as well as points |
| 4 | compute_zorder | Morton key: `(h3_8_int & 0xFFFFFFFF) << 32 \| (hash(feature_id) & 0xFFFFFFFF)` |
| 5 | compute_entropy | Shannon entropy over `feature_type` distribution per H3 cell |
| 6 | compute_entropy_bucket | Map entropy to macro partition bucket 0–9 |
| 7 | compute_importance | `I(S) = (0.4·A_norm + 0.5·C_type + 0.1·D_local) + 0.2·U_traj` → `importance_byte` 0–127. Uses real `area_m2` for buildings instead of the flat OSM placeholder, and falls back to `occupancy` as a class-weight tag when no OSM `building`/`amenity` tag is present |
| 8 | assign_resolution | Adaptive H3 resolution from entropy + cell variance |
| 9 | apply_hysteresis | Suppress partition migration when entropy shift < 0.20 |
| 10 | export | Write Parquet sorted by `entropy_bucket ASC, zorder_key ASC, importance_byte DESC` |
| 11 | write_catalog | Write `manifest.json` with entropy, size, fetchPriority, and budgetHint per tile |

**Output path**: OSM writes to `{output}/{country_lower}/`; buildings write to `{output}/{country_lower}/buildings/` — the nested subdirectory is mandatory, not cosmetic, since an OSM dataset and a building dataset can share a country code (`kenya` / `kenya-buildings` are both `KE`) and would otherwise silently overwrite each other's tiles.

### Locked Schema (v3.1 — 36 columns, every file)

Building rows leave the OSM-only columns `NULL`; OSM rows leave the building-only columns `NULL`. Nothing is repurposed across the two — `occupancy` and `building` (the OSM tag) are separate columns, for example.

| Column | Type | Purpose |
|--------|------|---------|
| `feature_id` | STRING | Stable ID — `relation/1809123` (OSM) or `building/796743226` (buildings) |
| `feature_type` | STRING | OSM: derived from tags (road, building, amenity, landuse…). Buildings: always `'building'` |
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
| `highway/building/amenity/landuse` | STRING | OSM only — extracted from `tags` MAP at ingestion |
| `population` | INT32 | OSM only — `TRY_CAST(tags['population'] AS INT)` |
| `road_class` | INT8 | OSM only — 1=motorway … 6=other |
| `floorspace` | STRING | Buildings only — raw source attribute |
| `occupancy` | STRING | Buildings only — RES/COM/IND/CIV/UNK |
| `height_raw` | STRING | Buildings only — original encoding, e.g. `'HBET:1-2'` |
| `height_m` / `height_floors` | DOUBLE | Buildings only — parsed from `height_raw` (`H:`/`HBET:`/`HHT:`/numeric patterns) |
| `quadkey` | STRING | Buildings only — legacy Bing quadkey, kept for interop |
| `relation_id` | STRING | Buildings only |
| `source_provider` | STRING | Buildings only — `'OSM'` / `'Google'` / `'Microsoft'` etc. |
| `area_m2` / `perimeter_m` | DOUBLE | Buildings only — computed via `ST_Transform` into the dataset's `utm_epsg`, not raw WGS84 degrees |

---

## Project Structure

```
himap/
├── API/
│   └── main.py                     # App factory — middleware, routers, startup
│                                    #   (builds both ViewGenerator and
│                                    #    BuildingViewGenerator at boot)
├── Models/
│   ├── requests.py                 # BBoxQueryParams, H3QueryParams, PartitionParams
│   └── responses.py                # FeaturesResponse, PartitionManifest, HealthStatus
├── Routes/
│   ├── RoutesQuery.py               # /query/all, /query/h3
│   ├── RoutesPartinitions.py        # /partitions/{dataset}/{z}/{x}/{y}.parquet
│   ├── RoutesCatalog.py             # /health, /datasets, /buildings-datasets, /towns, /zoom
│   └── RoutesBuidings.py            # /buildings/{dataset}
├── Services/
│   └── DuckLakeService.py          # Connection layer only — no domain queries
├── Export/
│   ├── Partitioner.py              # 11-stage pipeline, locked schema (OSM + buildings)
│   ├── ParquetExporter.py          # In-memory Parquet packaging for network responses
│   └── Writer/                     # ParquetStreamWriter
├── Generator/
│   ├── viewGenerator.py            # DuckDB views for OSM datasets
│   └── Buildingviewgenerator.py    # DuckDB views for building datasets (3-tier)
├── Ingestion/
│   ├── DataRegistry.py             # DatasetConfig, global OSM registry
│   └── BuildingRegistry.py         # BuildingConfig, global building registry
├── Registry/                       # Town registration sources (top-level —
│   ├── curated_towns.py            #   sibling of Towns/, not nested, so other
│   └── auto_registry.py            #   registries could move here too later)
├── Towns/
│   ├── TownBase.py                 # Named viewport with bbox/H3 factories
│   ├── TownRegistry.py             # Class + singleton only — no registration calls
│   ├── generate_towns.py           # Bulk town generator (writes into towns/)
│   └── towns/                      # Auto-generated TownBase subclasses
├── Utils/
│   ├── Utils.py                    # Lat-corrected spatial math
│   └── Zoom.py                     # Ground resolution, H3↔zoom mapping
└── partition_data.py               # CLI entry point — resolves --dataset against
                                     #   both DataRegistry and BuildingRegistry
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `HIMAP_LAKE_ROOT` | Root directory for partitioned Parquet lake (OSM and buildings both derive their output paths from this) | `./lake` |
| `HIMAP_DATASET_CANARY` | Override path for canary dataset | `./lake/es/` |
| `HIMAP_DATASET_KENYA` | Override path for kenya dataset | `./lake/ke/` |
| `HIMAP_OBM_ROOT` | OpenBuildingMap raw parquet source (pre-enrichment) | S3 path |
| `HIMAP_BUILDINGS_KENYA` | Override raw source path for kenya-buildings | `{HIMAP_OBM_ROOT}/` |
| `HIMAP_BUILDINGS_CANARY` | Override raw source path for canary-buildings | `{HIMAP_OBM_ROOT}/` |
| `HOST` | API server host | `0.0.0.0` |
| `PORT` | API server port | `9910` |

S3 paths work natively via DuckDB — set `HIMAP_DATASET_*` / `HIMAP_BUILDINGS_*` to `s3://your-bucket/path/` and no other code changes are needed. `utm_epsg` (per building dataset, used for `area_m2`/`perimeter_m`) is set in code in `BuildingRegistry.py`, not via environment variable, since it depends on the dataset's actual geography rather than deployment environment.

---

## Filtering Model

Three spatial filters operate at different layers and stack independently:

**OSM dataset filter** — applied at ingestion time by the Partitioner:
```sql
WHERE country = 'canary-islands'   -- exact match against osm.country column
```
This determines what goes into the Parquet files. The DuckDB view reads only those files.

**Building dataset filter** — applied at ingestion time (quadkey prefix, region-scoped) and again at query time, depending on `h3_enriched`:
```sql
-- pre-H3 (h3_enriched=False): bbox STRUCT + quadkey prefix
WHERE quadkey LIKE '1223%'

-- post-H3 (h3_enriched=True): the partitioned view's own h3_9 column
WHERE h3_9 IN (...)
```

**Town/viewport filter** — applied at query time by `/query/all` (OSM) or `/buildings/{dataset}` (buildings):
```sql
-- OSM
WHERE ST_Intersects(centroid_geom, ST_MakeEnvelope(sw_lng, sw_lat, ne_lng, ne_lat))

-- buildings, post-H3
WHERE h3_9 IN (<cells covering the viewport>)
```
This scopes results to the viewport the user is looking at.

A query for Nairobi applies two of these: the Kenya dataset filter ensures only Kenyan OSM data is in scope, and the Nairobi town bbox narrows to the city viewport. A buildings query for Nairobi additionally goes through the buildings dataset's own quadkey/H3 filter, since it's a separate Parquet source from OSM entirely.
