"""
Configuration models for DuckLake and PostGIS catalog.
"""

from typing import Optional
from pydantic import BaseModel, Field, validator


class PostGISCatalogConfig(BaseModel):
    """PostGIS catalog connection configuration."""
    
    host: str = Field(
        ...,
        description="PostGIS server hostname",
        example="localhost"
    )
    port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostGIS server port"
    )
    database: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="PostGIS database name"
    )
    user: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="PostGIS username"
    )
    password: str = Field(
        ...,
        min_length=1,
        description="PostGIS password"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "host": "localhost",
                "port": 5432,
                "database": "himap",
                "user": "postgres",
                "password": "secret"
            }
        }


class DuckLakeConfig(BaseModel):
    """DuckLake service configuration."""
    
    db_path: str = Field(
        default=":memory:",
        description="Path to DuckDB file or ':memory:' for in-memory"
    )
    memory_limit: str = Field(
        default="2GB",
        pattern=r"^\d+(GB|MB|KB)?$",
        description="Memory limit per connection (e.g., '2GB', '512MB')"
    )
    threads: int = Field(
        default=4,
        ge=1,
        le=64,
        description="Number of threads per connection"
    )
    postgis_catalog: Optional[PostGISCatalogConfig] = Field(
        default=None,
        description="Optional PostGIS catalog configuration"
    )
    
    @validator('memory_limit')
    def validate_memory_limit(cls, v):
        """Ensure memory limit is valid."""
        if v == ":memory:":
            return v
        
        units = ['GB', 'MB', 'KB']
        has_unit = any(v.endswith(unit) for unit in units)
        
        if not has_unit:
            raise ValueError(f"Memory limit must include unit (GB, MB, KB): {v}")
        
        try:
            num = int(''.join(filter(str.isdigit, v)))
            if num <= 0:
                raise ValueError("Memory limit must be positive")
        except ValueError:
            raise ValueError(f"Invalid memory limit format: {v}")
        
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "db_path": ":memory:",
                "memory_limit": "2GB",
                "threads": 4,
                "postgis_catalog": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "himap",
                    "user": "postgres",
                    "password": "secret"
                }
            }
        }


class CatalogConfig(BaseModel):
    """Catalog selection and configuration."""
    
    catalog: str = Field(
        ...,
        pattern=r"^(postgis|duckdb)$",
        description="Catalog type: 'postgis' or 'duckdb'"
    )
    host: Optional[str] = Field(
        default=None,
        description="PostGIS host (required if catalog='postgis')"
    )
    port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostGIS port"
    )
    database: Optional[str] = Field(
        default=None,
        description="PostGIS database (required if catalog='postgis')"
    )
    user: Optional[str] = Field(
        default=None,
        description="PostGIS user (required if catalog='postgis')"
    )
    password: Optional[str] = Field(
        default=None,
        description="PostGIS password (required if catalog='postgis')"
    )
    
    @validator('catalog')
    def validate_catalog(cls, v):
        """Ensure catalog is valid."""
        if v.lower() not in ['postgis', 'duckdb']:
            raise ValueError(f"Catalog must be 'postgis' or 'duckdb', got: {v}")
        return v.lower()
    
    @validator('host', 'database', 'user', 'password')
    def validate_postgis_params(cls, v, values):
        """Ensure PostGIS parameters are provided when catalog='postgis'."""
        catalog = values.get('catalog', '').lower()
        if catalog == 'postgis' and not v:
            raise ValueError(f"PostGIS catalog requires host, database, user, and password")
        return v
