"""
HiMap v2.0 - DuckLake Spatial Data API
FastAPI-based HTTP server with Pydantic validation and DuckLake integration.

DuckLake = DuckDB (analytical engine) + Optional PostGIS catalog
"""

import logging
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from pathlib import Path
import json
from datetime import datetime

# Import our services
from ..Services.DuckLakeService import DuckLakeService, ducklake_service

# Import Pydantic models
from ..Models.requests import (
    NodeQueryParams,
    VehicleQueryParams,
    CorridorQueryParams,
    H3QueryParams,
    H3CellQueryParams,
    AllDataQueryParams,
    ExportParams,
    PartitionExportParams,
    CatalogConfig,
)
from ..Models.responses import (
    NodesResponse,
    VehiclesResponse,
    CorridorsResponse,
    H3CellsResponse,
    H3CellQueryResponse,
    AllDataResponse,
    HealthStatus,
    CatalogsResponse,
    CatalogInfo,
    ErrorResponse,
    ErrorDetail,
    PartitionManifest,
    CatalogSetResponse,
    APIRootResponse,
)
from ..Models.config import PostGISCatalogConfig, DuckLakeConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app with enhanced metadata
app = FastAPI(
    title="HiMap Spatial Data API",
    description="HTTP API for querying spatial data from DuckLake (DuckDB + optional PostGIS catalog)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variable - now using unified DuckLake service
current_db_service = ducklake_service

def get_db_service() -> DuckLakeService:
    """Get the current database service"""
    return current_db_service


# Exception Handlers with Full Context

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with field-level details."""
    errors = []
    for error in exc.errors():
        errors.append(ErrorDetail(
            loc=list(error.get("loc", [])),
            msg=error.get("msg", "Unknown validation error"),
            type=error.get("type", "validation_error")
        ))
    
    # Build helpful message
    field_errors = [f"{'.'.join(e.loc)}: {e.msg}" for e in errors if e.loc]
    message = "Validation failed"
    if field_errors:
        message = f"Validation failed: {'; '.join(field_errors[:3])}"
    
    response = ErrorResponse(
        status="error",
        code="VALIDATION_ERROR",
        message=message,
        details=errors
    )
    return JSONResponse(status_code=422, content=response.dict())


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP errors with context."""
    response = ErrorResponse(
        status="error",
        code=f"HTTP_{exc.status_code}",
        message=exc.detail,
        details=None
    )
    return JSONResponse(status_code=exc.status_code, content=response.dict())


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors with request context."""
    logger.error(f"Unhandled exception in {request.method} {request.url.path}: {exc}", exc_info=True)
    
    response = ErrorResponse(
        status="error",
        code="INTERNAL_ERROR",
        message=f"Internal server error: {str(exc)[:100]}",
        details=[ErrorDetail(loc=["server"], msg=str(exc), type=type(exc).__name__)]
    )
    return JSONResponse(status_code=500, content=response.dict())


# Startup/Shutdown Events

@app.on_event("startup")
async def startup_event():
    """Initialize DuckLake service on startup."""
    logger.info("=" * 60)
    logger.info("HiMap v2.0 - DuckLake Spatial Data API Starting")
    logger.info("Architecture: DuckDB engine + Optional PostGIS catalog")
    logger.info("=" * 60)
    
    try:
        db_service = get_db_service()
        health = db_service.health_check()
        
        if health['healthy']:
            logger.info(f"DuckLake initialized: {health}")
            
            # List available tables
            try:
                tables = db_service.list_catalog_tables()
                logger.info(f"Available tables: {len(tables)}")
                for table in tables[:10]:
                    logger.info(f"  - {table['name']} ({table['source']})")
            except Exception as e:
                logger.warning(f"Could not list tables: {e}")
        else:
            logger.warning(f"DuckLake health check failed: {health}")
    except Exception as e:
        logger.error(f"Failed to initialize DuckLake: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("HiMap Spatial Data API shutting down...")


# API Endpoints

@app.get(
    "/",
    response_model=APIRootResponse,
    tags=["info"],
    summary="API information",
)
async def root():
    """Get API information and available endpoints."""
    return APIRootResponse(
        message="HiMap Spatial Data API",
        version="2.0.0",
        description="Query spatial data from DuckLake (DuckDB + optional PostGIS catalog)",
        architecture={
            "engine": "DuckDB",
            "catalog": "Unified (DuckDB native + optional PostGIS)",
            "service": "DuckLake"
        },
        endpoints={
            "GET /": "API information",
            "GET /health": "Health check",
            "GET /catalogs": "List available catalog sources",
            "GET /query/nodes": "Get traffic nodes",
            "GET /query/corridors": "Get corridors",
            "GET /query/vehicles": "Get vehicles",
            "GET /query/h3": "Get H3 cells",
            "GET /query/h3-cells": "Query features by H3 cell",
            "GET /query/all": "Get all data types",
            "GET /partitions/{z}/{x}/{y}.parquet": "Get spatially-partitioned Parquet file",
            "GET /partitions/{z}/{x}/{y}/data": "Get partition data as GeoJSON/raw",
            "GET /partitions/manifest": "Get partition manifest",
            "GET /export/{data_type}": "Export data as Parquet file",
            "POST /set-catalog": "Configure DuckLake catalog"
        }
    )


@app.get(
    "/health",
    response_model=HealthStatus,
    tags=["info"],
    summary="Health check",
    responses={
        503: {"model": ErrorResponse, "description": "Service unavailable"}
    }
)
async def health_check():
    """Health check endpoint with catalog status."""
    try:
        db_service = get_db_service()
        health = db_service.health_check()
        
        return HealthStatus(
            status="healthy" if health['healthy'] else "unhealthy",
            catalog=health.get('catalog', 'unknown'),
            latency_ms=health.get('latency', 0),
            error=health.get('error')
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))


@app.get(
    "/catalogs",
    response_model=CatalogsResponse,
    tags=["config"],
    summary="List available catalog sources"
)
async def list_catalogs():
    """List available catalog sources and current configuration."""
    db_service = get_db_service()
    health = db_service.health_check()
    
    available_catalogs = [
        CatalogInfo(
            name="duckdb",
            description="Native DuckDB tables (in-memory or persistent)",
            type="embedded"
        ),
        CatalogInfo(
            name="postgis",
            description="PostGIS database attached as catalog",
            type="external"
        )
    ]
    
    return CatalogsResponse(
        catalog=health.get('catalog', 'duckdb'),
        healthy=health.get('healthy', False),
        latency_ms=health.get('latency', 0),
        available_catalogs=available_catalogs,
        note="Use /set-catalog to configure catalog source"
    )


# Query Endpoints

@app.get(
    "/query/nodes",
    response_model=NodesResponse,
    tags=["queries"],
    summary="Get traffic nodes",
    responses={
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Server error"}
    }
)
async def get_nodes(
    params: NodeQueryParams = Query(...)
):
    """
    Get traffic nodes within a bounding box.
    
    Returns GeoJSON FeatureCollection with node locations and metrics.
    """
    try:
        bounds = {
            'southWest': {'lng': params.south_west_lng, 'lat': params.south_west_lat},
            'northEast': {'lng': params.north_east_lng, 'lat': params.north_east_lat}
        }
        
        db_service = get_db_service()
        nodes = db_service.get_nodes_in_bounds(
            bounds,
            node_types=params.node_types,
            min_saturation=params.min_saturation
        )
        
        # Apply limit
        if params.limit and len(nodes) > params.limit:
            nodes = nodes[:params.limit]
        
        # Convert to GeoJSON
        features = []
        for node in nodes:
            features.append({
                "type": "Feature",
                "properties": {
                    "id": node["id"],
                    "name": node["name"],
                    "node_type": node["type"],
                    "passenger_throughput": node["metrics"]["passengerThroughput"],
                    "average_dwell_time": node["metrics"]["averageDwellTime"],
                    "peak_hour": node["metrics"]["peakHour"],
                    "saturation_level": node["metrics"]["saturationLevel"],
                    "connected_routes": node["connectedRoutes"]
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [node["position"]["lng"], node["position"]["lat"]]
                }
            })
        
        return NodesResponse(
            features=features,
            count=len(features),
            bbox=[
                params.south_west_lng,
                params.south_west_lat,
                params.north_east_lng,
                params.north_east_lat
            ]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/query/corridors",
    response_model=CorridorsResponse,
    tags=["queries"],
    summary="Get corridors"
)
async def get_corridors(
    params: CorridorQueryParams = Query(...)
):
    """Get corridors within a bounding box."""
    try:
        bounds = {
            'southWest': {'lng': params.south_west_lng, 'lat': params.south_west_lat},
            'northEast': {'lng': params.north_east_lng, 'lat': params.north_east_lat}
        }
        
        db_service = get_db_service()
        corridors = db_service.get_corridors_in_bounds(bounds)
        
        if params.limit and len(corridors) > params.limit:
            corridors = corridors[:params.limit]
        
        features = []
        for corridor in corridors:
            features.append({
                "type": "Feature",
                "properties": {
                    "id": corridor["id"],
                    "name": corridor["name"],
                    "start_node": corridor["startNode"],
                    "end_node": corridor["endNode"],
                    "fuel_burn_rate": corridor["metrics"]["fuelBurnRate"],
                    "idling_hotspot_score": corridor["metrics"]["idlingHotspotScore"],
                    "vehicle_stress_index": corridor["metrics"]["vehicleStressIndex"],
                    "average_speed": corridor["metrics"]["averageSpeed"],
                    "peak_flow_time": corridor["metrics"]["peakFlowTime"]
                },
                "geometry": corridor.get("geometry", {"type": "LineString", "coordinates": []})
            })
        
        return CorridorsResponse(
            features=features,
            count=len(features),
            bbox=[
                params.south_west_lng,
                params.south_west_lat,
                params.north_east_lng,
                params.north_east_lat
            ]
        )
    except Exception as e:
        logger.error(f"Error fetching corridors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/query/vehicles",
    response_model=VehiclesResponse,
    tags=["queries"],
    summary="Get vehicles"
)
async def get_vehicles(
    params: VehicleQueryParams = Query(...)
):
    """Get vehicles within a bounding box."""
    try:
        bounds = {
            'southWest': {'lng': params.south_west_lng, 'lat': params.south_west_lat},
            'northEast': {'lng': params.north_east_lng, 'lat': params.north_east_lat}
        }
        
        db_service = get_db_service()
        vehicles = db_service.get_vehicles_in_bounds(bounds)
        
        if params.status:
            vehicles = [v for v in vehicles if v["status"] == params.status]
        
        if params.limit and len(vehicles) > params.limit:
            vehicles = vehicles[:params.limit]
        
        features = []
        for vehicle in vehicles:
            features.append({
                "type": "Feature",
                "properties": {
                    "id": vehicle["id"],
                    "sacco_id": vehicle["saccoId"],
                    "sacco_name": vehicle["saccoName"],
                    "plate_number": vehicle["plateNumber"],
                    "capacity": vehicle["capacity"],
                    "heading": vehicle["heading"],
                    "speed": vehicle["speed"],
                    "status": vehicle["status"],
                    "last_updated": vehicle["lastUpdated"]
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        vehicle["currentPosition"]["lng"],
                        vehicle["currentPosition"]["lat"]
                    ]
                }
            })
        
        return VehiclesResponse(
            features=features,
            count=len(features),
            bbox=[
                params.south_west_lng,
                params.south_west_lat,
                params.north_east_lng,
                params.north_east_lat
            ]
        )
    except Exception as e:
        logger.error(f"Error fetching vehicles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/query/h3",
    response_model=H3CellsResponse,
    tags=["queries"],
    summary="Get H3 cells"
)
async def get_h3_cells(
    params: H3QueryParams = Query(...)
):
    """Get H3 cells within a bounding box."""
    try:
        bounds = {
            'southWest': {'lng': params.south_west_lng, 'lat': params.south_west_lat},
            'northEast': {'lng': params.north_east_lng, 'lat': params.north_east_lat}
        }
        
        db_service = get_db_service()
        h3_cells = db_service.get_h3_cells_in_bounds(
            bounds,
            resolution=params.resolution
        )
        
        if params.limit and len(h3_cells) > params.limit:
            h3_cells = h3_cells[:params.limit]
        
        features = []
        for cell in h3_cells:
            features.append({
                "type": "Feature",
                "properties": {
                    "cell_id": cell["cellId"],
                    "resolution": cell["resolution"],
                    **cell.get("properties", {})
                },
                "geometry": cell["boundary"]
            })
        
        return H3CellsResponse(
            features=features,
            count=len(features),
            bbox=[
                params.south_west_lng,
                params.south_west_lat,
                params.north_east_lng,
                params.north_east_lat
            ]
        )
    except Exception as e:
        logger.error(f"Error fetching H3 cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Additional endpoints would be added here...

# Partition Endpoints

@app.get(
    "/partitions/{z}/{x}/{y}.parquet",
    tags=["partitions"],
    summary="Get partitioned Parquet file",
    response_class=FileResponse
)
async def get_partition(
    params: PartitionExportParams = Query(...)
):
    """Get a spatially-partitioned Parquet file by z/x/y coordinates."""
    try:
        partition_path = Path(
            f"./partitions/{params.country.lower()}/{params.city.lower()}"
            f"/z{params.z}/{params.x}/{params.y}.parquet"
        )
        
        if not partition_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Partition not found: z={params.z}, x={params.x}, y={params.y}"
            )
        
        return FileResponse(
            path=str(partition_path),
            media_type="application/octet-stream",
            headers={
                "X-Partition-Z": str(params.z),
                "X-Partition-X": str(params.x),
                "X-Partition-Y": str(params.y),
                "Cache-Control": "public, max-age=3600"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching partition: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Configuration Endpoints

@app.post(
    "/set-catalog",
    response_model=CatalogSetResponse,
    tags=["config"],
    summary="Configure DuckLake catalog"
)
async def set_catalog(
    params: CatalogConfig = Query(...)
):
    """Configure DuckLake catalog source."""
    global current_db_service
    
    try:
        if params.catalog.lower() == "postgis":
            if not all([params.host, params.database, params.user, params.password]):
                raise HTTPException(
                    status_code=400,
                    detail="PostGIS catalog requires host, database, user, and password"
                )
            
            postgis_config = {
                'host': params.host,
                'port': str(params.port),
                'database': params.database,
                'user': params.user,
                'password': params.password
            }
            
            # Create new DuckLake service with PostGIS catalog
            current_db_service = DuckLakeService(
                db_path=":memory:",
                memory_limit="2GB",
                threads=4,
                postgis_catalog=postgis_config
            )
            
            logger.info(f"Switched to DuckLake with PostGIS catalog: {params.host}:{params.port}/{params.database}")
            
            return CatalogSetResponse(
                message="Switched to DuckLake with PostGIS catalog",
                catalog="postgis",
                host=params.host,
                database=params.database
            )
        
        elif params.catalog.lower() == "duckdb":
            current_db_service = DuckLakeService(
                db_path=":memory:",
                memory_limit="2GB",
                threads=4,
                postgis_catalog=None
            )
            
            logger.info("Switched to DuckLake (DuckDB only)")
            
            return CatalogSetResponse(
                message="Switched to DuckLake (DuckDB only)",
                catalog="duckdb"
            )
        
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid catalog. Must be 'postgis' or 'duckdb'"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set catalog: {e}")
        raise HTTPException(status_code=500, detail=str(e))
