# HiMap PostGIS Usage Examples

## Basic Usage

### Export traffic nodes within a bounding box
```bash
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query nodes
```

### Export corridors within a bounding box
```bash
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query corridors
```

### Export vehicles within a bounding box
```bash
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query vehicles
```

### Export H3 cells within a bounding box
```bash
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query h3 --h3-resolution 9
```

### Export all data types
```bash
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query all
```

## With Width/Height Instead of End Coordinates

### Specify map size in parts
```bash
python -m himap ./output/ --start 13.0 52.0 --width 10 --height 10 --query nodes
```

## With Database Connection Parameters

### Specify database connection details
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --db-host localhost --db-port 5432 \
  --db-name himap --db-user postgres --db-password secret \
  --query nodes
```

## With Filtering Options

### Filter nodes by type and minimum saturation
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --query nodes \
  --node-types train_station bus_stop \
  --min-saturation 70.0
```

### Find nearest vehicles to a point
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --query vehicles \
  --nearest-point 13.5 52.5 \
  --vehicle-limit 50
```

## Advanced Options

### Skip Parquet validation (faster)
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --query nodes \
  --no-validate
```

### Also export as GeoJSON
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --query all \
  --geojson
```

### Verbose logging
```bash
python -m himap ./output/ \
  --start 13.0 52.0 --end 14.0 53.0 \
  --query nodes \
  --verbose
```

## Environment Variables

You can also set database connection parameters via environment variables:

- `POSTGIS_HOST` (default: localhost)
- `POSTGIS_PORT` (default: 5432)
- `POSTGIS_DB` (default: himap)
- `POSTGIS_USER` (default: postgres)
- `POSTGIS_PASSWORD` (default: empty)

Example:
```bash
SET POSTGIS_HOST=localhost
SET POSTGIS_PORT=5432
SET POSTGIS_DB=himap
SET POSTGIS_USER=postgres
SET POSTGIS_PASSWORD=secret
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query nodes
```

## Output Files

Depending on the query type, the following files will be generated in the output directory:

- `traffic_nodes.parquet` - Traffic node data
- `corridors.parquet` - Corridor analytics data
- `vehicles.parquet` - Vehicle tracking data
- `h3_cells.parquet` - H3 grid cell data
- `*.geojson` - GeoJSON versions (if --geojson flag is used)

All Parquet files are compressed with SNAPPY compression and include statistics for optimal query performance.