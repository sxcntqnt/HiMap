# Migration Path: PostGIS to DuckDB

## Overview
This document outlines the path for migrating from PostGIS to DuckDB for spatial queries in HiMap.

## Why DuckDB?
- Single-file or in-memory database
- Excellent Parquet integration (native support)
- Columnar storage for analytical workloads
- Spatial extensions with good performance
- No server required - simplifies deployment

## Migration Strategy

### Phase 1: Dual Support (Current State)
- Maintain both PostGIS and DuckDB service layers
- Configuration flag to switch between backends
- Gradual testing with DuckDB alongside PostGIS

### Phase 2: Feature Parity
- Implement all required spatial queries in DuckDBService
- Ensure identical output formats
- Performance benchmarking

### Phase 3: Cutover
- Switch default to DuckDB
- Keep PostGIS as fallback option
- Remove PostGIS dependency after validation

## Implementation Details

### Connection Management
DuckDB uses a file path or ":memory:" for in-memory databases:
```python
# For persistent storage
db = DuckDBService("/path/to/spatial.duckdb")

# For in-memory (fastest, but not persistent)
db = DuckDBService(":memory:")
```

### Spatial Functions
DuckDB spatial extension provides similar functions to PostGIS:
- `ST_AsWKB`, `ST_AsGeoJSON` for geometry conversion
- `ST_Intersects`, `ST_DWithin` for spatial relationships
- `ST_MakeEnvelope` for bounding boxes
- `ST_Distance` for proximity queries

### Performance Considerations
1. **Parquet Integration**: DuckDB can read/write Parquet natively
   ```sql
   COPY (SELECT * FROM nodes) TO 'nodes.parquet' (FORMAT PARQUET);
   ```
2. **Vectorized Operations**: DuckDB's columnar engine excels at analytical queries
3. **Memory Management**: Configure memory limits for large datasets

## Code Changes Required

### 1. Update Service Interface
Create a common abstract base class for database services:
```python
from abc import ABC, abstractmethod

class SpatialDatabaseService(ABC):
    @abstractmethod
    def get_nodes_in_bounds(self, bounds, node_types=None, min_saturation=None):
        pass
    
    # ... other abstract methods
```

### 2. Factory Pattern for Service Selection
```python
def get_database_service(backend="postgis"):
    if backend == "duckdb":
        return DuckDBService()
    elif backend == "postgis":
        return PostGISService()
    else:
        raise ValueError(f"Unknown backend: {backend}")
```

### 3. Configuration Updates
Add backend selection to command line arguments:
```bash
--db-backend [postgis|duckdb]  # Default: postgis
```

### 4. Gradual Migration Approach
1. Implement DuckDBService with full functionality
2. Add backend selection flag
3. Test both backends with identical queries
4. Benchmark performance
5. Switch default to DuckDB
6. Deprecate PostGIS service

## Advantages of DuckDB for HiMap

1. **Deployment Simplicity**: No database server to manage
2. **Performance**: Excellent for read-heavy analytical workloads
3. **Format Alignment**: Native Parquet support reduces conversion overhead
4. **Cost**: No licensing or infrastructure costs
5. **Portability**: Single file database or in-memory

## Challenges to Address

1. **Concurrent Writes**: DuckDB is optimized for read-heavy workloads
2. **Very Large Datasets**: May require careful memory management
3. **Advanced Features**: Some PostGIS extensions may not have DuckDB equivalents
4. **Migration Path**: Existing PostGIS data needs to be imported

## Next Steps

1. Complete DuckDBService implementation with all query methods
2. Add benchmarking scripts to compare PostGIS vs DuckDB performance
3. Create data migration tools (PostGIS → DuckDB)
4. Implement connection pooling for DuckDB (if needed for concurrent access)
5. Add backup/restore functionality for file-based DuckDB instances

## Example Usage After Migration
```bash
# Use DuckDB backend
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query nodes --db-backend duckdb

# Use in-memory DuckDB for testing
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query nodes --db-backend duckdb --db-path :memory:

# Use persistent DuckDB file
python -m himap ./output/ --start 13.0 52.0 --end 14.0 53.0 --query nodes --db-backend duckdb --db-path ./spatial_data.duckdb
```