"""
HiMap v3.0 — App Factory

This file does three things only:
    1. Create the FastAPI app with middleware and exception handlers
    2. Mount routers (query, partitions, catalog)
    3. Run startup/shutdown lifecycle (build DuckDB views)

No SQL. No business logic. No direct service calls beyond startup.

Architecture:
    main.py
        ↓ mounts
    routes/query.py       — /query/all, /query/h3
    routes/partitions.py  — /partitions/{dataset}/{z}/{x}/{y}.parquet
    routes/catalog.py     — /health, /datasets

    All routes delegate to:
    ViewGenerator         — produces SQL strings (bbox_query, h3_query)
    DuckLakeService       — executes SQL
    DatasetRegistry       — resolves dataset keys to config
"""

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ..Services.DuckLakeService import ducklake_service
from ..dataset_registry import registry
from ..view_generator import ViewGenerator
from .Models.responses import ErrorDetail, ErrorResponse
from .routes.catalog import router as catalog_router
from .routes.partitions import router as partitions_router
from .routes.query import router as query_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="HiMap Spatial Data API",
    description=(
        "Query spatial data from the HiMap Parquet lake. "
        "Datasets are country-scoped Parquet partitions indexed by H3 and quadtree."
    ),
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = [
        ErrorDetail(
            loc=[str(l) for l in error.get("loc", [])],
            msg=error.get("msg", "Validation error"),
            type=error.get("type", "validation_error"),
        )
        for error in exc.errors()
    ]

    field_msgs = [f"{'.'.join(e.loc)}: {e.msg}" for e in errors if e.loc]
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            code="VALIDATION_ERROR",
            message=f"Validation failed: {'; '.join(field_msgs[:3])}",
            details=errors,
        ).model_dump(),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            code=f"HTTP_{exc.status_code}",
            message=exc.detail,
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(
        f"Unhandled exception — {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            code="INTERNAL_ERROR",
            message=f"Internal server error: {str(exc)[:100]}",
            details=[ErrorDetail(loc=["server"], msg=str(exc), type=type(exc).__name__)],
        ).model_dump(),
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(catalog_router)
app.include_router(query_router)
app.include_router(partitions_router)


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------

@app.get("/", response_model=APIRootResponse, tags=["info"], summary="API info")
async def root():
    return APIRootResponse(
        datasets=registry.list(),
        endpoints={
            "GET /health":                                   "DuckLake health check",
            "GET /datasets":                                 "List registered datasets",
            "GET /datasets/{key}":                           "Dataset config",
            "GET /datasets/{key}/views":                     "DuckDB view state",
            "GET /query/all?dataset=&sw_lng=...":            "Bounding box query",
            "GET /query/h3?dataset=&h3_index=...":           "H3 index query",
            "GET /partitions/{dataset}/{z}/{x}/{y}.parquet": "Serve Parquet partition",
            "GET /partitions/{dataset}/manifest":            "Partition manifest",
        },
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    logger.info("=" * 60)
    logger.info("HiMap v3.0 starting")
    logger.info(f"Registered datasets: {registry.list()}")
    logger.info("=" * 60)

    # Health check
    health = ducklake_service.health_check()
    if health["healthy"]:
        logger.info(f"DuckLake ready — catalog={health['catalog']} latency={health['latency_ms']}ms")
    else:
        logger.warning(f"DuckLake unhealthy at startup: {health.get('error')}")

    # Build DuckDB views for all registered datasets
    # Views are CREATE OR REPLACE — safe to run on every startup
    vg = ViewGenerator(ducklake_service)
    built = vg.build_all()
    logger.info(f"Views built for: {built}")

    # Log table inventory
    try:
        tables = ducklake_service.list_tables()
        logger.info(f"Visible tables: {[t['name'] for t in tables]}")
    except Exception as e:
        logger.warning(f"Could not list tables: {e}")


@app.on_event("shutdown")
async def shutdown():
    logger.info("HiMap v3.0 shutting down")
