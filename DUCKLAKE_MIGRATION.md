# HiMap v2.0 - DuckLake Migration Guide

## Overview

HiMap v2.0 migrates from a dual-backend architecture (DuckDB vs PostGIS) to a unified **DuckLake** architecture.

**DuckLake** = DuckDB (analytical engine) + Optional PostGIS catalog

## What Changed

### Architecture Evolution

**v1.x Architecture:**
```
API → Switch between:
  ├─ DuckDBService (default)
  └─ PostGISService (deprecated)
```

**v2.0 Architecture:**
```
API → DuckLakeService (unified)
  ├─ DuckDB (query engine)
  └─ Optional: PostGIS catalog (via PostgreSQL scanner)
```

### Key Differences

| Aspect | v1.x | v2.0 |
|--------|------|------|
| **Services** | 2 services (DuckDBService, PostGISService) | 1 unified service (DuckLakeService) |
| **Backend switching** | Runtime switch between backends | PostGIS attached as catalog |
| **Query engine** | Either DuckDB OR PostGIS | Always DuckDB (with PostGIS data via scanner) |
| **Spatial functions** | Different implementations | Unified (DuckDB spatial extension) |
| **API version** | 1.0.0 | 2.0.0 |

## Files Changed

### New Files

- `himap/Services/DuckLakeService.py` - Unified service replacing both v1.x services

### Removed Files

- `himap/Services/DuckDBService.py` - Replaced by DuckLakeService
- `himap/Services/PostGISService.py` - Replaced by DuckLakeService
- `himap/Database/PostGISPool.py` - No longer needed (DuckDB handles connections)

### Modified Files

- `himap/API/main.py` - Updated to use DuckLakeService
- `himap/Export/Partitioner.py` - Now uses DuckLake for data access
- `requirements.txt` - Updated dependencies
- `README.md` - New architecture documentation

## API Changes

### New Endpoints

| Endpoint | Description |
|----------|-------------|
| `/catalogs` | List available catalog sources |
| `/partitions/{z}/{x}/{y}.parquet` | Spatially-partitioned data files |
| `/partitions/{z}/{x}/{y}/data` | Partition data as GeoJSON/raw |
| `/partitions/manifest` | Partition manifest |

### Changed Endpoints

| Old | New | Notes |
|-----|-----|-------|
| `/backends` | `/catalogs` | Returns catalog configuration |
| `/set-backend` | `/set-catalog` | Configure catalog source |
| `/tiles/` | `/partitions/` | Terminology corrected |

### Request/Response Changes

**Health Check (`/health`):**
```json
// v1.x
{
  "status": "healthy",
  "database": { "healthy": true, "latency": 12.34 },
  "timestamp": "..."
}

// v2.0
{
  "status": "healthy",
  "service": "DuckLake",
  "catalog": "postgis",
  "latency_ms": 12.34,
  "timestamp": "..."
}
```

**Set Catalog (`/set-catalog`):**
```bash
# v1.x
POST /set-backend?backend=postgis

# v2.0
POST /set-catalog?catalog=postgis&host=localhost&port=5432&database=himap&user=postgres&password=secret
```

## Migration Steps

### For API Users

1. **Update health check parsing**
   - Check for `service: "DuckLake"` instead of backend switching
   - Use `catalog` field to determine data source

2. **Update catalog configuration**
   - Replace `/set-backend` calls with `/set-catalog`
   - Pass PostGIS connection parameters explicitly

3. **Update endpoint URLs**
   - Change `/tiles/` to `/partitions/`
   - Update `/backends` to `/catalogs`

### For Developers

1. **Update imports**
   ```python
   # v1.x
   from himap.Services.DuckDBService import duckdb_service
   from himap.Services.PostGISService import postgis_service
   
   # v2.0
   from himap.Services.DuckLakeService import ducklake_service
   ```

2. **Update service usage**
   ```python
   # v1.x - Switch backends
   current_service = duckdb_service  # or postgis_service
   
   # v2.0 - Configure catalog once
   ducklake_service = DuckLakeService(postgis_catalog={...})
   ```

## Feature Preservation

All query functionality is preserved:

- ✅ Traffic nodes queries
- ✅ Corridor analytics
- ✅ Vehicle tracking
- ✅ H3 grid queries
- ✅ Bounding box queries
- ✅ GeoJSON export
- ✅ Parquet export

## New Features

- **Unified query interface** - Same SQL regardless of data source
- **Catalog discovery** - List all available tables from DuckDB + PostGIS
- **Three-layer partitioning** - Quadtree/H3/Z-order for optimized exports
- **Streaming Parquet writer** - Memory-efficient chunked exports

## Performance Notes

### Query Performance

- **DuckDB native tables**: Fastest (in-memory or persistent)
- **PostGIS via scanner**: Slightly slower due to network, but vectorized
- **Mixed queries**: DuckDB optimizer handles joins across sources

### Best Practices

1. **Use PostGIS catalog for**: Legacy data access, read-only workloads
2. **Use DuckDB native for**: High-performance analytics, large exports
3. **Use three-layer partitioning for**: CDN distribution, tile-based APIs

## Troubleshooting

### "Failed to attach PostGIS catalog"

Check:
- PostgreSQL scanner extension is installed: `INSTALL postgres; LOAD postgres`
- Connection parameters are correct
- PostGIS database is accessible

### "Table not found"

Check catalog configuration:
```sql
-- List all available tables
SELECT * FROM information_schema.tables
WHERE table_schema IN ('main', 'postgis')
```

## Backward Compatibility

- Query endpoints remain unchanged (except `/tiles/` → `/partitions/`)
- Response formats are enhanced but backward-compatible
- Health check adds new fields but preserves `status` field

## Migration Timeline

| Phase | Action | Status |
|-------|--------|--------|
| 1 | Create DuckLakeService | ✅ Complete |
| 2 | Update API to use DuckLake | ✅ Complete |
| 3 | Remove deprecated services | ✅ Complete |
| 4 | Update Partitioner | ✅ Complete |
| 5 | Update documentation | ✅ Complete |
| 6 | Testing | 🔄 Pending |

## Summary

DuckLake provides a cleaner architecture:
- Single service to maintain
- Unified query interface
- Optional PostGIS integration (not either/or)
- Better performance through DuckDB's vectorized engine

The migration maintains backward compatibility while enabling new capabilities through the unified catalog system.
