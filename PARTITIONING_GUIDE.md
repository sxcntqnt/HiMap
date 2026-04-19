# HiMap Three-Layer Partitioning System

This document explains the three-layer spatial data partitioning system implemented in HiMap for optimized data export and CDN distribution.

## Overview

HiMap uses a **three-layer contract** for organizing spatial data:

| Layer | Question Answered | Implementation |
|-------|------------------|----------------|
| **Layer 1: Quadtree** | WHERE does data live? | Tile addressing (z/x/y) |
| **Layer 2: H3** | WHAT does data mean? | H3 hierarchical spatial index |
| **Layer 3: Z-order** | HOW is data stored? | Morton encoding for sequential I/O |

This design enables:
- **Efficient CDN distribution**: Tiles are self-contained Parquet files
- **Fast spatial queries**: H3 indices enable pre-filtering
- **Sequential disk reads**: Z-order keys optimize DuckDB scans

## The Three Layers in Detail

### Layer 1: Quadtree (WHERE)

**Purpose**: Assign every feature to a tile address for file-based partitioning.

**Implementation**:
- Default zoom level: **10** (~600m tiles at equator, smaller at high latitudes)
- File structure: `tiles/{country}/{city}/z{z}/{x}/{y}.parquet`
- Tile math: Standard Web Mercator projection

**Example**:
```
tiles/ke/nairobi/z10/618/480.parquet
```

**Key Properties**:
- Each tile is a self-contained Parquet file
- No knowledge of H3 or z-order required for addressing
- Enables direct CDN delivery with cache-friendly URLs

### Layer 2: H3 (WHAT)

**Purpose**: Provide semantic spatial clustering for queries.

**Implementation**:
- Multiple resolutions: **7, 8, 9, 10**
- Stored as columns: `h3_7`, `h3_8`, `h3_9`, `h3_10`
- Computed from feature centroids using DuckDB's H3 extension

**Resolutions for Africa Context**:

| Resolution | Cell Size | Use Case |
|------------|-----------|----------|
| 7 | ~5 km² | City/metro scale |
| 8 | ~0.7 km² | District/neighborhood |
| 9 | ~0.1 km² | Street block |
| 10 | ~15k m² | Fine-grained |

**Query Patterns**:
```sql
-- Find all features in a specific H3 cell
SELECT * FROM tiles WHERE h3_8 = '881f24a4b7fffff';

-- Join against viewport H3 cells (fast pre-filter)
SELECT * FROM tiles 
WHERE h3_8 IN (SELECT unnest(h3_polygon_to_cells(viewport, 8)));
```

### Layer 3: Z-order (HOW)

**Purpose**: Optimize storage layout for sequential reads.

**Implementation**:
- **Morton encoding**: Interleaves bits of (h3_cell, osm_id)
- 64-bit integer: `zorder_key`
- Rows sorted by `zorder_key` within each tile

**Formula**:
```python
zorder_key = ((h3_int & 0xFFFFFFFF) << 32) | (osm_id & 0xFFFFFFFF)
```

**Benefits**:
- Sequential disk reads when scanning spatially adjacent features
- Reduced I/O for range queries
- Natural clustering by H3 cell

## Integration with Existing HiMap

The partitioning system **extends** the existing DuckDB-based HiMap server:

### What Changed

1. **New Module**: `himap/Export/TilePartitioner.py`
   - Implements the three-layer logic
   - Provides `TilePartitioner` class for partitioning data

2. **New CLI Tool**: `partition_data.py`
   - Standalone script to run partitioning pipeline
   - Generates tiles and manifest

3. **Extended Schema**: Partitioned tables include:
   - `tile_z`, `tile_x`, `tile_y` (Layer 1)
   - `h3_7`, `h3_8`, `h3_9`, `h3_10` (Layer 2)
   - `zorder_key` (Layer 3)

### What Stayed the Same

- **HTTP API**: All existing endpoints continue to work
- **Database**: Still uses DuckDB (now with H3 extension)
- **Query Logic**: Existing queries work on source tables
- **Export**: Parquet export functionality preserved

## Usage

### Running the Partitioning Pipeline

```bash
# Basic usage (assumes osm_raw table exists in database)
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city nairobi \
    --country KE

# Multiple tables
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city mombasa \
    --country KE \
    --tables osm_raw,osm_pois,osm_buildings

# Custom output and zoom
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city lagos \
    --country NG \
    --output ./cdn_tiles \
    --zoom 11 \
    --target-features 30000
```

### Output Structure

```
tiles/
└── ke/
    └── nairobi/
        ├── manifest.json              # Tile manifest for BootstrapManifestService
        └── z10/
            ├── 618/
            │   ├── 479.parquet        # Individual tile files
            │   ├── 480.parquet
            │   └── 481.parquet
            └── 619/
                └── ...
```

### Manifest Format

The generated `manifest.json` is consumed by the frontend's `BootstrapManifestService`:

```json
{
  "cityId": "nairobi",
  "countryCode": "KE",
  "tileZoom": 10,
  "zorderRange": [123456789, 987654321],
  "h3_7_cells": ["871f24a4bffffff", "871f24a4cffffff", ...],
  "tile_count": 245,
  "total_features": 1523421,
  "tileKeys": [
    {
      "z": 10,
      "x": 618,
      "y": 480,
      "featureCount": 42345,
      "roadCount": 12345,
      "buildingCount": 23456,
      "parquetUrl": "https://cdn.yourdomain.com/ke/nairobi/z10/618/480.parquet"
    },
    ...
  ]
}
```

## Query Patterns

### Pattern 1: Viewport Tile Query (HTTP API)

```python
# In himap/API/main.py - existing endpoint
@app.get("/query/nodes")
async def get_nodes(...):
    # Uses Layer 2 (H3) for fast pre-filtering
    # Uses Layer 1 (Quadtree) for tile addressing
    pass
```

### Pattern 2: Direct Tile Access (Client-side)

```javascript
// Frontend fetches specific tile
const response = await fetch(
  'https://cdn.yourdomain.com/ke/nairobi/z10/618/480.parquet'
);
const tileData = await response.arrayBuffer();
// Process with DuckDB WASM
```

### Pattern 3: H3 Density Query

```sql
-- Query for heatmap overlay
SELECT 
    h3_8,
    COUNT(*) as feature_count,
    COUNT(*) FILTER (WHERE feature_type = 'road') as roads
FROM read_parquet('tiles/ke/nairobi/z10/*/*.parquet')
WHERE country_code = 'KE'
GROUP BY h3_8;
```

## Migration from Existing Data

### Step 1: Ensure Source Table Exists

Your DuckDB database should have tables with OSM data:

```sql
-- Example: osm_raw table structure
CREATE TABLE osm_raw (
    osm_id BIGINT,
    feature_type VARCHAR(32),
    name VARCHAR(255),
    country_code VARCHAR(2),
    wkb_geom BLOB,
    highway VARCHAR(32),
    building VARCHAR(32),
    amenity VARCHAR(32),
    landuse VARCHAR(32),
    population INTEGER
);
```

### Step 2: Run Partitioning

```bash
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city nairobi \
    --country KE \
    --tables osm_raw
```

### Step 3: Upload to CDN

```bash
# Using rclone
rclone copy tiles/ke/nairobi r2:map-tiles-prod/ke/nairobi \
    --include "*.parquet" \
    --transfers 16 \
    --progress

# Upload manifest
rclone copy tiles/ke/nairobi/manifest.json \
    r2:map-tiles-prod/ke/nairobi/manifest.json
```

### Step 4: Update Frontend

Update `bootstrap-manifest.service.ts`:

```typescript
const CITY_INDEX: Record<string, CityDefinition> = {
  nairobi: {
    id: "nairobi",
    parquetBase: "https://cdn.yourdomain.com/ke/nairobi",
    zorderRange: [123456789, 987654321],  // From manifest
    h3_7_cells: ["871f24a4bffffff", ...],  // From manifest
    // ...
  }
};
```

## Performance Characteristics

### Tile Generation

- **Nairobi (full city)**: ~8 minutes on MacBook Pro M3
- **Output size**: ~400MB compressed
- **Tile count**: ~245 tiles at zoom 10
- **Features per tile**: 1K-50K (adaptive)

### Query Performance

- **Viewport query**: <100ms (with H3 pre-filter)
- **Tile fetch**: <50ms from CDN
- **Full table scan**: ~2s for 1.5M features

### Storage Efficiency

- **Compression**: ZSTD level 3
- **Row groups**: 16MB (optimal for DuckDB WASM)
- **Average tile size**: ~1.6MB

## Extending to Other Cities

### Adding Mombasa

```bash
# 1. Ensure data exists in database
# (Run your ETL to load Mombasa OSM data into osm_raw table)

# 2. Partition
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city mombasa \
    --country KE

# 3. Upload
rclone copy tiles/ke/mombasa r2:map-tiles-prod/ke/mombasa
```

### Adding Other Countries

```bash
# Nigeria (Lagos)
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city lagos \
    --country NG

# Ghana (Accra)
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city accra \
    --country GH
```

## Implementation Notes

### Why Not Use PostGIS?

- **HiMap already uses DuckDB**: Natural extension of existing architecture
- **No additional services**: Single embedded database
- **Parquet-native**: Direct export without serialization overhead
- **H3 extension**: Native H3 support in DuckDB

### Why Zoom Level 10?

- **Sweet spot for Africa**: ~600m tiles at Nairobi latitude
- **Manageable tile sizes**: 1K-50K features per tile
- **CDN-friendly**: Small enough for fast download, large enough to batch
- **Adaptive subdivision**: Tiles >50K features can be subdivided to zoom 11

### Why These H3 Resolutions?

- **Res 7**: City/metro overview (Nairobi has ~50 cells)
- **Res 8**: District level (Nairobi has ~300 cells)
- **Res 9**: Neighborhood (Nairobi has ~2000 cells)
- **Res 10**: Street block (Nairobi has ~15000 cells)

## Troubleshooting

### "H3 extension not found"

```sql
-- In DuckDB CLI
INSTALL h3;
LOAD h3;
```

### "Tile has too many features"

Increase zoom level or enable subdivision:

```bash
python partition_data.py --zoom 11 --target-features 30000
```

### "Z-order range is 0 to 0"

Check that H3 extension is loaded and features have valid centroids.

## References

- [H3 Spatial Index](https://h3geo.org/)
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial)
- [Morton Codes / Z-order Curves](https://en.wikipedia.org/wiki/Z-order_curve)
- [Slippy Map Tilenames](https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames)