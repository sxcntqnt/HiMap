# HiMap v2.0 - DuckLake Spatial Data API

HiMap is a FastAPI-based HTTP server providing unified access to spatial data through DuckLake - a hybrid architecture using DuckDB as the analytical engine with optional PostGIS catalog integration for metadata and transactional queries.

## Features

- **RESTful API** for querying spatial data (traffic nodes, corridors, vehicles, H3 cells)
- **DuckLake Architecture**: DuckDB engine + optional PostGIS catalog for unified data access
- **Partitioned Parquet Export**: Spatially-partitioned data files via z/x/y addressing
- **Streaming Parquet Writer**: Memory-efficient chunked export using `ParquetStreamWriter` for large datasets
- **Three-Layer Spatial Partitioning**: Quadtree (WHERE) + H3 (WHAT) + Z-order (HOW) for optimized organization
- **GeoJSON Support**: Optional GeoJSON output for map visualization
- **Automatic Documentation**: Interactive API docs via Swagger UI at `/docs`

## Architecture

**DuckLake** combines the best of both worlds:
- **DuckDB**: Columnar analytical engine for fast spatial queries
- **PostGIS Catalog**: Optional external catalog for metadata, snapshots, and transactional queries (not legacy access)
- **Three-Layer Partitioning**: Spatial data organization (Quadtree/H3/Z-order)
- **Unified API**: Single interface regardless of data source

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
python run_server.py

# Access API documentation
open http://localhost:8000/docs
```

## API Endpoints

### Core Query Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API information |
| `/health` | GET | Health check with catalog status |
| `/catalogs` | GET | List available catalog sources |
| `/query/nodes` | GET | Traffic nodes within bounds |
| `/query/corridors` | GET | Corridor analytics |
| `/query/vehicles` | GET | Vehicle tracking |
| `/query/h3` | GET | H3 grid cells |
| `/query/all` | GET | All data types |

### Partitioned Data Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/partitions/{z}/{x}/{y}.parquet` | GET | Spatially-partitioned Parquet file |
| `/partitions/{z}/{x}/{y}/data` | GET | Partition data as GeoJSON/raw |
| `/partitions/manifest` | GET | Partition manifest for a city |

### Configuration & Export

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/set-catalog` | POST | Configure catalog (postgis/duckdb) |
| `/export/{data_type}` | GET | Export data as Parquet |

## Streaming Parquet Export

HiMap includes `ParquetStreamWriter` for memory-efficient export of large datasets:

### Features

- **Streaming Architecture**: Processes data in configurable chunks without loading entire dataset into memory
- **Automatic Buffering**: Buffers records in memory and flushes to disk when threshold reached
- **File Sharding**: Automatically creates new Parquet files when size limit reached
- **Compression**: ZSTD compression with configurable levels
- **Progress Tracking**: Logs export progress for monitoring

### Usage

```python
from himap.Export.Writer.parquet_stream_writer.src.parquet_stream_writer import ParquetStreamWriter

# Streaming export for large datasets
partitioner = Partitioner(db_path="./data/osm.duckdb")
manifest = partitioner.export_partitions_streaming(
    partition_table="osm_raw_partitioned",
    city_id="nairobi",
    country_code="KE",
    shard_size_bytes=512_000_000,  # 512MB per shard
    buffer_size_bytes=16_777_216,   # 16MB buffer
    row_group_size=10000
)
```

### CLI Partitioning

```bash
# Standard export using DuckDB COPY
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city nairobi \
    --country KE \
    --output ./partitions

# The Partitioner automatically uses ParquetStreamWriter
# for memory-efficient chunked writing
```

## Configuration

### DuckLake Catalog Options

**Option 1: Native DuckDB (default)**
```bash
curl -X POST "http://localhost:8000/set-catalog?catalog=duckdb"
```

**Option 2: PostGIS Catalog**
```bash
curl -X POST "http://localhost:8000/set-catalog" \
  -G \
  -d "catalog=postgis" \
  -d "host=localhost" \
  -d "port=5432" \
  -d "database=himap" \
  -d "user=postgres" \
  -d "password=secret"
```

## Three-Layer Spatial Partitioning

HiMap organizes spatial data using a three-layer contract:

| Layer | Purpose | Implementation |
|-------|---------|----------------|
| **Quadtree** | WHERE data lives | z/x/y spatial addressing |
| **H3** | WHAT data means | Hierarchical spatial index (res 7-10) |
| **Z-order** | HOW data is stored | Morton encoding for sequential I/O |

### Partitioning Data

```bash
python partition_data.py \
    --db-path ./data/osm.duckdb \
    --city nairobi \
    --country KE \
    --output ./partitions
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DUCKDB_PATH` | DuckDB database path | `:memory:` |
| `POSTGIS_HOST` | PostGIS server host | - |
| `POSTGIS_PORT` | PostGIS server port | `5432` |
| `POSTGIS_DB` | PostGIS database name | - |
| `POSTGIS_USER` | PostGIS username | - |
| `POSTGIS_PASSWORD` | PostGIS password | - |
| `HOST` | API server host | `0.0.0.0` |
| `PORT` | API server port | `8000` |

## Project Structure

```
himap/
├── API/
│   └── main.py              # FastAPI HTTP endpoints
├── Services/
│   └── DuckLakeService.py   # Unified DuckDB+PostGIS service
├── Export/
│   ├── Partitioner.py       # Three-layer spatial partitioning
│   ├── ParquetExporter.py   # Parquet export utilities
│   └── Writer/              # Streaming Parquet writer
├── requirements.txt         # Dependencies
└── run_server.py           # Server startup script
```

## Migration from v1.x

HiMap v2.0 replaces the dual-service architecture (DuckDBService + PostGISService) with a unified DuckLake service:

- **Before**: Switch between `duckdb` and `postgis` backends
- **After**: Use DuckLake with optional PostGIS catalog attachment

The API remains backward-compatible for query endpoints.

## License

MIT License - See LICENSE file for details.
