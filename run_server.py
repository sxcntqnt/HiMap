#!/usr/bin/env python3
"""
HiMap v2.0 Spatial Data HTTP Server
Run this script to start the FastAPI server with DuckLake architecture

DuckLake = DuckDB (engine) + Optional PostGIS catalog
"""

import uvicorn
import logging
import sys
from pathlib import Path

# Add the himap directory to the path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the server"""
    logger.info("=" * 60)
    logger.info("HiMap v2.0 - DuckLake Spatial Data API")
    logger.info("Architecture: DuckDB engine + Optional PostGIS catalog")
    logger.info("=" * 60)
    
    try:
        # Run the FastAPI application with Uvicorn
        uvicorn.run(
            "himap.API.main:app",
            host="0.0.0.0",
            port=9910,
            reload=True,  # Set to False in production
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
