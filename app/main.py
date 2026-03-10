"""
SCM Digital Twin Backend - FastAPI
Main entry point with all routes
"""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
import asyncio
import os
from pathlib import Path
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import requests
from app.config import settings
from app.api import ( 
    #routes_tsi,
    #routes_whatif,
    #routes_ml,
    routes_network_design
)

# Load environment variables
load_dotenv()

# MVP Configuration
MVP_MODE = os.getenv("MVP_MODE", "true")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
STORAGE_TYPE = os.getenv("STORAGE_TYPE", "sqlite")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "20"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "25"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Reduce Azure SDK verbosity
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def ensure_baseline_downloaded():
    """Lazy download baseline data only when needed"""
    from pathlib import Path
    import requests
    import os
    import time
    
    DATA_DIR = Path("data")
    BASELINE_PATH = DATA_DIR / "combined_df.csv"
    
    DATA_DIR.mkdir(exist_ok=True)
    if BASELINE_PATH.exists():
        return
    
    url = os.getenv(
        "BASELINE_DATA_URL",
        "https://scmdata2026.blob.core.windows.net/data/combined_df.csv",
    )
    logger.info(f"Downloading baseline data from {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=25)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        downloaded = 0
        start_time = time.time()
        
        with open(BASELINE_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Progress check
                    if time.time() - start_time > 20:  # 5s buffer
                        logger.info(f"Downloaded {downloaded}/{total_size} bytes")
                        if downloaded < total_size * 0.1:  # At least 10% progress
                            continue  # Continue if making progress
                        else:
                            logger.warning("Download too slow, will use fallback")
                            return False
        
        success = downloaded >= total_size * 0.95  # 95% complete
        logger.info(f"Baseline download {'successful' if success else 'partial'}: {downloaded} bytes")
        return success
        
    except Exception as e:
        logger.error(f"Baseline download failed: {e}")
        return False

# Import routers
from app.api.routes_network_design import router as network_design_router

# Import background job
#from app.jobs.background_decoder_job import start_background_job, stop_background_job

# Azure Blob Storage Configuration
BASELINE_DATA_URL = os.getenv(
    "BASELINE_DATA_URL",
    "https://scmdata2026.blob.core.windows.net/data/combined_df.csv",
)
MASTER_DATA_URL = os.getenv(
    "MASTER_DATA_URL",
    "https://scmdata2026.blob.core.windows.net/data/Master_data_with_pincodes.csv",
)

def download_if_missing(url: str, target_path: Path, label: str) -> None:
    """Download file from URL if it doesn't exist locally"""
    if not url or target_path.exists():
        return
    
    logger.info(f"[startup] Downloading {label} from {url}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"[startup] Successfully downloaded {label} to {target_path}")
    except Exception as e:
        logger.error(f"[startup] Failed to download {label}: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown"""
    # Startup
    logger.info("🚀 Starting SCM Digital Twin API")
    
    # Ensure data directory exists
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    logger.info(f"📁 Using data directory: {data_dir.absolute()}")
    
    # Download only master data if missing (baseline downloaded on demand)
    download_if_missing(
        MASTER_DATA_URL, 
        data_dir / "Master_data_with_pincodes.csv", 
        "master pincode data"
    )
    
    # Start background job
    """
    try:
        await start_background_job(interval=30)  # 30 seconds for testing
        logger.info("✅ Background job started")
    except Exception as e:
        logger.error(f"Failed to start background job: {e}")
        raise
    """
    logger.info("📊 Telemetry API: /api/telemetry")
    logger.info("📖 API Docs: http://localhost:8000/docs")
    
    # App runs here
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down...")
    """await stop_background_job()"""
    logger.info("👋 Goodbye!")
    
# Initialize FastAPI with lifespan
app = FastAPI(
    title="SCM Digital Twin API",
    description="Supply Chain Management Digital Twin Backend",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# CORS Middleware
# ============================================================================
def normalize_origins():
    """Normalize CORS origins based on environment"""
    origins = []
    
    # Add localhost for development only
    if os.getenv("ENVIRONMENT") != "production":
        origins.extend(["http://localhost:3000", "http://127.0.0.1:3000"])
    
    # Add frontend URL from environment
    frontend_url = os.getenv("FRONTEND_URL")
    if frontend_url:
        origins.append(frontend_url.strip())
    
    # Add additional production origins
    cors_origins = os.getenv("CORS_ORIGINS", "")
    if cors_origins:
        origins.extend([origin.strip() for origin in cors_origins.split(",") if origin.strip()])
    
    # Remove duplicates and filter empty
    return list(set(filter(None, origins)))

# Use normalized origins
origins = normalize_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,  # Stricter policy
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ============================================================================
# Include Routers
# ============================================================================

# Network Design endpoints
app.include_router(network_design_router, prefix="/api/network", tags=["network-design"])

#Machinelearning endpoints  
#app.include_router(routes_whatif.router,   prefix="/api/what_if", tags=["what-if"])
#app.include_router(routes_ml.router,       prefix="/api/ML", tags=["machine-learning"])

# ============================================================================
# Root Endpoints
# ============================================================================
@app.get("/")
async def root():
    """API root - health check"""
    return {
        "service": "SCM Digital Twin API",
        "status": "running",
        "version": "1.0.0",
        "endpoints": {
            "network": "/api/network",
            "what_if" : "/api/what_if",
            "ML":"/api/ML",
            "docs": "/docs"
        }
    }

@app.get("/health")
def health_check():
    """Kubernetes/Docker health check"""
    return {"status": "healthy"}

# ============================================================================
# Health Check Endpoint
# ============================================================================

# ============================================================================
# Run Server
# ============================================================================

