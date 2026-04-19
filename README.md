# HiMap Spatial Data HTTP Server

HiMap is now a FastAPI-based HTTP server that provides programmatic access to spatial data from PostGIS or DuckDB databases, with the ability to export results as Parquet files. The server has been migrated from the original Google Maps Static API-based image generator to a modern spatial data service.

## 🚀 Features

- **RESTful API** for querying spatial data (traffic nodes, corridors, vehicles, H3 cells)
- **Three-Layer Partitioning System**: Quadtree (WHERE) + H3 (WHAT) + Z-order (HOW) for optimized data organization
- **Tile Serving**: Direct access to partitioned Parquet tiles via z/x/y coordinates
- **Database Backends**: DuckDB (default, optimized for analytical workloads) with PostGIS compatibility layer (deprecated)
- **Parquet Export**: Download query results as optimized Parquet files
- **GeoJSON Support**: Optional GeoJSON output for map visualization
- **Spatial Queries**: Bounding box, point-in-polygon, proximity searches, H3 cell-based queries
- **Automatic Documentation**: Interactive API docs via Swagger UI
- **Health Monitoring**: Database connectivity and performance metrics
- **Resource Management**: Per-query connection limits to prevent resource exhaustion
- **Input Validation**: Comprehensive query parameter validation and sanitization

### Three-Layer Architecture

| Layer | Question Answered | Implementation |
|-------|------------------|----------------|
| **Layer 1: Quadtree** | WHERE does data live? | Tile addressing (z/x/y) for CDN distribution |
| **Layer 2: H3** | WHAT does data mean? | H3 hierarchical spatial index (resolutions 7-10) |
| **Layer 3: Z-order** | HOW is data stored? | Morton encoding for sequential disk reads |

## 📊 Default Configuration

- **Database**: DuckDB (in-memory by default, configurable for persistence)
- **Port**: 8000
- **Host**: 0.0.0.0 (accessible from any interface)

## 🔧 Installation

```bash
# Clone the repository
git clone <repository-url>
cd HiMap

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:
.venv/Scripts/activate
# Unix/MacOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## ▶️ Usage

### Start the Server
```bash
python run_server.py
```

The server will be available at http://localhost:8000

### API Documentation
Visit http://localhost:8000/docs for interactive Swagger UI documentation

### Example Queries

#### Get Traffic Nodes
```bash
curl "http://localhost:8000/query/nodes?south_west_lng=13.0&south_west_lat=52.0&north_east_lng=14.0&north_east_lat=53.0"
```

#### Get Vehicles with Filtering
```bash
curl "http://localhost:8000/query/vehicles?south_west_lng=13.0&south_west_lat=52.0&north_east_lng=14.0&north_east_lat=53.0&status=active&limit=100"
```

#### Export Data as Parquet
```bash
curl -o nodes.parquet "http://localhost:8000/export/nodes?south_west_lng=13.0&south_west_lat=52.0&north_east_lng=14.0&north_east_lat=53.0"
```

#### Switch Database Backend
```bash
# To DuckDB (default)
curl -X POST "http://localhost:8000/set-backend?backend=duckdb"

# To PostGIS (deprecated compatibility layer)
curl -X POST "http://localhost:8000/set-backend?backend=postgis"
```

#### Health Check
```bash
curl http://localhost:8000/health
```

## 🗃️ Database Configuration

### DuckDB (Default)
- Uses in-memory database by default for fastest performance (data lost on restart)
- Can be configured for persistent storage via environment variables:
  - `DUCKDB_PATH`: Path to DuckDB file (default: ":memory:" for in-memory)
- Each query uses a dedicated read-only connection with resource limits:
  - Memory limit: 2GB per query (configurable via DuckDBService)
  - Thread limit: 4 threads per query (configurable via DuckDBService)
- Designed for read-heavy analytical workloads; write operations should be handled externally

### PostGIS (Deprecated - Compatibility Layer)
- Still supported for backward compatibility but **not recommended for new deployments**
- Configure via environment variables:
  - `POSTGIS_HOST` (default: localhost)
  - `POSTGIS_PORT` (default: 5432)
  - `POSTGIS_DB` (default: himap)
  - `POSTGIS_USER` (default: postgres)
  - `POSTGIS_PASSWORD` (default: empty)
- Note: DuckDB's spatial extension covers most common spatial predicates (ST_Intersects, ST_DWithin, ST_AsWKB, ST_AsGeoJSON, etc.) but lacks advanced PostGIS features like topology, raster, and advanced buffer styling. For users with existing PostGIS workloads, evaluate DuckDB support for your specific use cases before migrating.

## 📦 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API information and available endpoints |
| GET | `/health` | Health check with database status |
| GET | `/query/nodes` | Traffic nodes within bounding box |
| GET | `/query/corridors` | Corridor analytics within bounding box |
| GET | `/query/vehicles` | Vehicle tracking data |
| GET | `/query/h3` | H3 grid cells |
| GET | `/query/all` | All data types in single request |
| GET | `/export/{data_type}` | Export data as Parquet file |
| POST | `/set-backend` | Switch database backend |
| GET | `/backends` | List available backends |

## 📄 Data Formats

### Parquet Export
- Optimized columnar format for analytical workloads
- SNAPPY compression for efficient storage
- Includes statistics for query optimization
- Schema preserved for downstream consumption

### GeoJSON Output
- Standard GeoJSON FeatureCollection format
- Compatible with mapping libraries (Leaflet, Mapbox GL JS, etc.)
- Available via query parameters on most endpoints

## ⚙️ Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DUCKDB_PATH` | DuckDB file path | `:memory:` (in-memory) |
| `POSTGIS_HOST` | PostGIS server host | `localhost` |
| `POSTGIS_PORT` | PostGIS server port | `5432` |
| `POSTGIS_DB` | PostGIS database name | `himap` |
| `POSTGIS_USER` | PostGIS username | `postgres` |
| `POSTGIS_PASSWORD` | PostGIS password | `` (empty) |
| `HOST` | Server host | `0.0.0.0` |
| `PORT` | Server port | `8000` |

## 🛠️ Development

### Backend Services
- `himap/Services/DuckDBService.py`: Primary database service (default)
- `himap/Services/PostGISService.py`: Compatibility layer (deprecated)
- `himap/Export/ParquetExporter.py`: Optimized Parquet export functionality

### Running Tests
```bash
# Run basic functionality tests
python -m pytest tests/ -v
```

## 📝 Notes

### Performance
- DuckDB provides excellent performance for analytical workloads (vectorized execution, columnar storage)
- In-memory database offers sub-second response times for most queries
- Parquet export is optimized for large result sets
- Each query runs with resource limits to ensure fair sharing under concurrent load
- For extremely large datasets (>100M rows), consider persistent DuckDB storage and tuning memory limits

### Migration Path
1. **Current**: DuckDB is default, PostGIS available for compatibility (evaluate your spatial function needs)
2. **Future**: PostGIS service will be removed in favor of native DuckDB
3. **Data Migration**: Tools available to migrate from PostGIS to DuckDB (e.g., using `ogr2ogr` or DuckDB's PostgreSQL scanner)

### Limitations
- Designed for read-heavy analytical workloads
- Write operations should be handled through external ETL processes
- Very large datasets may require adjusting memory/thread limits in DuckDBService
- Assumes WGS84 (EPSG:4326) for all input and output coordinates; for accurate distance/area calculations, clients should reproject to an appropriate CRS (e.g., Africa Albers Equal Area ESRI:102022) in their application layer
- Authentication and rate limiting are not included; these should be implemented at the deployment level (reverse proxy, API gateway, or middleware) as needed for your security requirements

### Security Considerations
- All query parameters are validated for type, range, and format
- Result sets are capped per endpoint to prevent accidental large responses:
  - Nodes: 500 (configurable via endpoint limit parameter)
  - Corridors: 200
  - Vehicles: 1000
  - H3 cells: 5000
  - All data types: individual limits per type
- Geometry complexity is indirectly limited by memory and thread constraints per query
- Temporary export files are cleaned up automatically (files older than 1 hour)
- For production deployments, consider adding authentication, rate limiting, and input sanitization at the infrastructure level

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## ⚠️ Disclaimer

This software is provided for educational and analytical purposes. Ensure compliance with data usage policies and regulations when working with spatial data.