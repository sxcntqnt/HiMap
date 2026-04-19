import logging
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import uvicorn
from pathlib import Path
import json
import os
from datetime import datetime

# Import our services
from ..Services.DuckDBService import duckdb_service
from ..Services.PostGISService import postgis_service  # Kept for backward compatibility
from ..Export.ParquetExporter import parquet_exporter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="HiMap Spatial Data API",
    description="HTTP API for querying spatial data from PostGIS/DuckDB and exporting as Parquet",
    version="1.0.0"
)

# Global variable to track selected database service
current_db_service = duckdb_service  # Default to DuckDB (PostGIS deprecated)

def get_db_service():
    """Get the current database service"""
    return current_db_service

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("HiMap Spatial Data API starting up with DuckDB as default backend (PostGIS deprecated)...")
    # Test database connection
    try:
        db_service = get_db_service()
        if hasattr(db_service, 'health_check'):
            health = db_service.health_check()
            if not health['healthy']:
                logger.warning(f"Database health check failed: {health}")
            else:
                logger.info(f"Database connected successfully: {health}")
        else:
            logger.info("Database service initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("HiMap Spatial Data API shutting down...")
    # Close database connections if needed
    try:
        if hasattr(postgis_service, 'pool') and postgis_service.pool:
            postgis_service.pool.closeall()
    except Exception as e:
        logger.error(f"Error closing PostGIS connections: {e}")

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "HiMap Spatial Data API",
        "version": "1.0.0",
        "description": "Query spatial data and export as Parquet files",
        "endpoints": {
            "GET /": "API information",
            "GET /health": "Health check",
            "GET /query/nodes": "Get traffic nodes",
            "GET /query/corridors": "Get corridors",
            "GET /query/vehicles": "Get vehicles",
            "GET /query/h3": "Get H3 cells",
            "GET /query/all": "Get all data types",
            "GET /export/{data_type}": "Export data as Parquet file",
            "POST /set-backend": "Switch database backend (PostGIS/DuckDB)"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        db_service = get_db_service()
        if hasattr(db_service, 'health_check'):
            health = db_service.health_check()
            return JSONResponse(content={
                "status": "healthy" if health['healthy'] else "unhealthy",
                "database": health,
                "timestamp": datetime.now().isoformat()
            })
        else:
            return JSONResponse(content={
                "status": "healthy",
                "message": "Database service available",
                "timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

@app.get("/query/nodes")
async def get_nodes(
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    node_types: Optional[List[str]] = Query(None, description="Filter by node types"),
    min_saturation: Optional[float] = Query(None, description="Minimum saturation level (0-100)", ge=0, le=100),
    limit: int = Query(500, description="Maximum number of results", ge=1, le=10000)
):
    """Get traffic nodes within a bounding box"""
    try:
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        nodes = db_service.get_nodes_in_bounds(
            bounds,
            node_types=node_types,
            min_saturation=min_saturation
        )
        
        # Apply limit if specified
        if limit and len(nodes) > limit:
            nodes = nodes[:limit]
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": [
                {
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
                }
                for node in nodes
            ],
            "count": len(nodes),
            "bbox": [south_west_lng, south_west_lat, north_east_lng, north_east_lat]
        })
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/corridors")
async def get_corridors(
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    limit: int = Query(200, description="Maximum number of results", ge=1, le=5000)
):
    """Get corridors within a bounding box"""
    try:
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        corridors = db_service.get_corridors_in_bounds(bounds)
        
        # Apply limit if specified
        if limit and len(corridors) > limit:
            corridors = corridors[:limit]
        
        features = []
        for corridor in corridors:
            # Extract coordinates from geometry (simplified for LineString)
            coords = []
            if corridor.get("geometry", {}).get("coordinates"):
                coords = corridor["geometry"]["coordinates"]
            
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
                "geometry": corridor["geometry"] if corridor.get("geometry") else {
                    "type": "LineString",
                    "coordinates": coords
                }
            })
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "bbox": [south_west_lng, south_west_lat, north_east_lng, north_east_lat]
        })
    except Exception as e:
        logger.error(f"Error fetching corridors: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/vehicles")
async def get_vehicles(
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    status: Optional[str] = Query("active", description="Filter by vehicle status"),
    limit: int = Query(1000, description="Maximum number of results", ge=1, le=10000)
):
    """Get vehicles within a bounding box"""
    try:
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        vehicles = db_service.get_vehicles_in_bounds(bounds)
        
        # Filter by status if specified
        if status:
            vehicles = [v for v in vehicles if v["status"] == status]
        
        # Apply limit if specified
        if limit and len(vehicles) > limit:
            vehicles = vehicles[:limit]
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": [
                {
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
                }
                for vehicle in vehicles
            ],
            "count": len(vehicles),
            "bbox": [south_west_lng, south_west_lat, north_east_lng, north_east_lat]
        })
    except Exception as e:
        logger.error(f"Error fetching vehicles: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/h3")
async def get_h3_cells(
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    resolution: int = Query(9, description="H3 resolution (0-15)", ge=0, le=15),
    limit: int = Query(5000, description="Maximum number of results", ge=1, le=20000)
):
    """Get H3 cells within a bounding box"""
    try:
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        h3_cells = db_service.get_h3_cells_in_bounds(bounds, resolution=resolution)
        
        # Apply limit if specified
        if limit and len(h3_cells) > limit:
            h3_cells = h3_cells[:limit]
        
        features = []
        for cell in h3_cells:
            features.append({
                "type": "Feature",
                "properties": {
                    "cell_id": cell["cellId"],
                    "resolution": cell["resolution"],
                    **cell["properties"]
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [cell["boundary"]["coordinates"][0]]  # Convert to proper GeoJSON format
                }
            })
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "bbox": [south_west_lng, south_west_lat, north_east_lng, north_east_lat]
        })
    except Exception as e:
        logger.error(f"Error fetching H3 cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/all")
async def get_all_data(
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    node_types: Optional[List[str]] = Query(None, description="Filter by node types"),
    min_saturation: Optional[float] = Query(None, description="Minimum saturation level (0-100)", ge=0, le=100),
    h3_resolution: int = Query(9, description="H3 resolution (0-15)", ge=0, le=15),
    vehicle_limit: int = Query(1000, description="Limit for vehicle queries", ge=1, le=10000),
    nodes_limit: int = Query(500, description="Limit for node queries", ge=1, le=10000),
    corridors_limit: int = Query(200, description="Limit for corridor queries", ge=1, le=5000),
    h3_limit: int = Query(5000, description="Limit for H3 queries", ge=1, le=20000)
):
    """Get all data types within a bounding box"""
    try:
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        
        # Fetch all data types
        nodes = db_service.get_nodes_in_bounds(
            bounds,
            node_types=node_types,
            min_saturation=min_saturation
        )
        
        corridors = db_service.get_corridors_in_bounds(bounds)
        vehicles = db_service.get_vehicles_in_bounds(bounds)
        h3_cells = db_service.get_h3_cells_in_bounds(bounds, resolution=h3_resolution)
        
        # Apply limits
        if nodes_limit and len(nodes) > nodes_limit:
            nodes = nodes[:nodes_limit]
        if corridors_limit and len(corridors) > corridors_limit:
            corridors = corridors[:corridors_limit]
        if vehicle_limit and len(vehicles) > vehicle_limit:
            vehicles = vehicles[:vehicle_limit]
        if h3_limit and len(h3_cells) > h3_limit:
            h3_cells = h3_cells[:h3_limit]
        
        # Convert to GeoJSON FeatureCollections
        nodes_features = [
            {
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
            }
            for node in nodes
        ]
        
        corridors_features = []
        for corridor in corridors:
            coords = corridor.get("geometry", {}).get("coordinates", [])
            corridors_features.append({
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
                "geometry": corridor.get("geometry", {
                    "type": "LineString",
                    "coordinates": coords
                })
            })
        
        vehicles_features = [
            {
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
            }
            for vehicle in vehicles
        ]
        
        h3_features = []
        for cell in h3_cells:
            h3_features.append({
                "type": "Feature",
                "properties": {
                    "cell_id": cell["cellId"],
                    "resolution": cell["resolution"],
                    **cell["properties"]
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [cell["boundary"]["coordinates"][0]]
                }
            })
        
        return JSONResponse(content={
            "nodes": {
                "type": "FeatureCollection",
                "features": nodes_features,
                "count": len(nodes_features)
            },
            "corridors": {
                "type": "FeatureCollection",
                "features": corridors_features,
                "count": len(corridors_features)
            },
            "vehicles": {
                "type": "FeatureCollection",
                "features": vehicles_features,
                "count": len(vehicles_features)
            },
            "h3_cells": {
                "type": "FeatureCollection",
                "features": h3_features,
                "count": len(h3_features)
            },
            "bbox": [south_west_lng, south_west_lat, north_east_lng, north_east_lat],
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error fetching all data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export/{data_type}")
async def export_data(
    data_type: str,
    south_west_lng: float = Query(..., description="Southwest longitude"),
    south_west_lat: float = Query(..., description="Southwest latitude"),
    north_east_lng: float = Query(..., description="Northeast longitude"),
    north_east_lat: float = Query(..., description="Northeast latitude"),
    node_types: Optional[List[str]] = Query(None, description="Filter by node types"),
    min_saturation: Optional[float] = Query(None, description="Minimum saturation level (0-100)", ge=0, le=100),
    h3_resolution: int = Query(9, description="H3 resolution (0-15)", ge=0, le=15),
    validate: bool = Query(True, description="Validate Parquet file after creation"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Export data as Parquet file"""
    try:
        # Validate data_type
        valid_types = ["nodes", "corridors", "vehicles", "h3", "all"]
        if data_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data type. Must be one of: {', '.join(valid_types)}"
            )
        
        bounds = {
            'southWest': {'lng': south_west_lng, 'lat': south_west_lat},
            'northEast': {'lng': north_east_lng, 'lat': north_east_lat}
        }
        
        db_service = get_db_service()
        
        # Create temporary directory for export
        export_dir = Path("./temp_exports")
        export_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.parquet"
        filepath = export_dir / filename
        
        # Export based on data type
        if data_type == "nodes":
            data = db_service.get_nodes_in_bounds(
                bounds,
                node_types=node_types,
                min_saturation=min_saturation
            )
            parquet_exporter.export_nodes(data, str(filepath), validate=validate)
            
        elif data_type == "corridors":
            data = db_service.get_corridors_in_bounds(bounds)
            parquet_exporter.export_corridors(data, str(filepath), validate=validate)
            
        elif data_type == "vehicles":
            data = db_service.get_vehicles_in_bounds(bounds)
            parquet_exporter.export_vehicles(data, str(filepath), validate=validate)
            
        elif data_type == "h3":
            data = db_service.get_h3_cells_in_bounds(bounds, resolution=h3_resolution)
            parquet_exporter.export_h3_cells(data, str(filepath), validate=validate)
            
        elif data_type == "all":
            # For 'all', we'll create a combined export or just export nodes as primary
            # In a real implementation, you might want to export multiple files or use a different format
            data = db_service.get_nodes_in_bounds(
                bounds,
                node_types=node_types,
                min_saturation=min_saturation
            )
            parquet_exporter.export_nodes(data, str(filepath), validate=validate)
        
        # Schedule cleanup of old files (older than 1 hour)
        def cleanup_old_files():
            try:
                now = datetime.now().timestamp()
                for file in export_dir.glob("*.parquet"):
                    if now - file.stat().st_mtime > 3600:  # 1 hour
                        file.unlink()
                        logger.info(f"Cleaned up old export file: {file}")
            except Exception as e:
                logger.error(f"Error cleaning up old files: {e}")
        
        background_tasks.add_task(cleanup_old_files)
        
        # Return the file
        return FileResponse(
            path=str(filepath),
            filename=filename,
            media_type='application/octet-stream'
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting {data_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/set-backend")
async def set_backend(backend: str = Query(..., description="Database backend: postgis or duckdb")):
    """Switch database backend"""
    global current_db_service
    
    if backend.lower() == "postgis":
        current_db_service = postgis_service
        logger.info("Switched to PostGIS backend")
        return {"message": "Switched to PostGIS backend", "backend": "postgis"}
    elif backend.lower() == "duckdb":
        current_db_service = duckdb_service
        logger.info("Switched to DuckDB backend")
        return {"message": "Switched to DuckDB backend", "backend": "duckdb"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid backend. Must be 'postgis' or 'duckdb'"
        )

@app.get("/backends")
async def list_backends():
    """List available database backends"""
    return {
        "available": ["postgis", "duckdb"],
        "current": "duckdb" if current_db_service == duckdb_service else "postgis",
        "note": "DuckDB is the default backend. PostGIS is maintained for backward compatibility only."
    }

@app.get("/tiles/{z}/{x}/{y}.parquet")
async def get_tile(
    z: int,
    x: int,
    y: int,
    city: str = Query("nairobi", description="City identifier"),
    country: str = Query("KE", description="Country code")
):
    """Get a specific tile by z/x/y coordinates (Three-Layer: Quadtree)"""
    try:
        from fastapi.responses import FileResponse
        from pathlib import Path
        
        # Construct tile path
        tile_path = Path(f"./tiles/{country.lower()}/{city.lower()}/z{z}/{x}/{y}.parquet")
        
        if not tile_path.exists():
            raise HTTPException(status_code=404, detail=f"Tile not found: z={z}, x={x}, y={y}")
        
        return FileResponse(
            path=str(tile_path),
            media_type="application/octet-stream",
            headers={
                "X-Tile-Z": str(z),
                "X-Tile-X": str(x),
                "X-Tile-Y": str(y),
                "Cache-Control": "public, max-age=3600"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching tile: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tiles/{z}/{x}/{y}/data")
async def get_tile_data(
    z: int,
    x: int,
    y: int,
    city: str = Query("nairobi", description="City identifier"),
    country: str = Query("KE", description="Country code"),
    format: str = Query("geojson", description="Output format: geojson or raw")
):
    """
    Get tile data as GeoJSON or raw Parquet bytes.
    Three-Layer: Quadtree (z/x/y) + H3 (pre-filter) + Z-order (sorted)
    """
    try:
        import duckdb
        from shapely import wkb
        
        tile_path = Path(f"./tiles/{country.lower()}/{city.lower()}/z{z}/{x}/{y}.parquet")
        
        if not tile_path.exists():
            raise HTTPException(status_code=404, detail=f"Tile not found")
        
        if format == "raw":
            # Return raw Parquet bytes
            with open(tile_path, 'rb') as f:
                data = f.read()
            return Response(content=data, media_type="application/octet-stream")
        
        # Read tile with DuckDB and convert to GeoJSON
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial")
        
        result = conn.execute(f"""
            SELECT 
                osm_id,
                feature_type,
                name,
                ST_GeomFromWKB(wkb_geom) as geom,
                h3_8,
                zorder_key,
                highway,
                building,
                amenity
            FROM read_parquet('{tile_path}')
            LIMIT 10000
        """).fetchall()
        
        conn.close()
        
        # Convert to GeoJSON FeatureCollection
        features = []
        for row in result:
            feature = {
                "type": "Feature",
                "properties": {
                    "osm_id": row[0],
                    "feature_type": row[1],
                    "name": row[2],
                    "h3_8": row[4],
                    "zorder_key": row[5],
                    "highway": row[6],
                    "building": row[7],
                    "amenity": row[8]
                },
                "geometry": json.loads(wkb.loads(bytes(row[3])).__geo_interface__)
            }
            features.append(feature)
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "tile": {"z": z, "x": x, "y": y},
            "features": features,
            "count": len(features)
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading tile data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tiles/manifest")
async def get_manifest(
    city: str = Query("nairobi", description="City identifier"),
    country: str = Query("KE", description="Country code")
):
    """Get tile manifest for a city (Three-Layer: Metadata)"""
    try:
        manifest_path = Path(f"./tiles/{country.lower()}/{city.lower()}/manifest.json")
        
        if not manifest_path.exists():
            raise HTTPException(status_code=404, detail=f"Manifest not found for {city}")
        
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        return JSONResponse(content=manifest)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading manifest: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/h3-cells")
async def query_h3_cells(
    h3_cell: str = Query(..., description="H3 cell ID (e.g., 881f24a4b7fffff)"),
    city: str = Query("nairobi", description="City identifier"),
    country: str = Query("KE", description="Country code"),
    resolution: int = Query(8, description="H3 resolution to query", ge=7, le=10)
):
    """
    Query features by H3 cell (Three-Layer: H3 semantic indexing).
    Uses Layer 2 (H3) for fast pre-filtering across all tiles.
    """
    try:
        import duckdb
        from pathlib import Path
        
        # Find which tiles might contain this H3 cell
        # In production, this would use the manifest for efficient tile lookup
        tile_pattern = Path(f"./tiles/{country.lower()}/{city.lower()}/z10/*/*.parquet")
        
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial")
        
        h3_col = f"h3_{resolution}"
        
        result = conn.execute(f"""
            SELECT 
                osm_id,
                feature_type,
                name,
                ST_GeomFromWKB(wkb_geom) as geom,
                {h3_col},
                centroid_lat,
                centroid_lng
            FROM read_parquet('{tile_pattern}')
            WHERE {h3_col} = '{h3_cell}'
            LIMIT 10000
        """).fetchall()
        
        conn.close()
        
        # Convert to GeoJSON
        features = []
        for row in result:
            feature = {
                "type": "Feature",
                "properties": {
                    "osm_id": row[0],
                    "feature_type": row[1],
                    "name": row[2],
                    "h3_cell": row[4],
                    "centroid": [row[6], row[5]]
                },
                "geometry": json.loads(wkb.loads(bytes(row[3])).__geo_interface__)
            }
            features.append(feature)
        
        return JSONResponse(content={
            "type": "FeatureCollection",
            "h3_cell": h3_cell,
            "h3_resolution": resolution,
            "features": features,
            "count": len(features)
        })
        
    except Exception as e:
        logger.error(f"Error querying H3 cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    uvicorn.run(
        "himap.API.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )