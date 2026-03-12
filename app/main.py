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
import aiohttp
import aiofiles
from app.config import settings

# Global State Management (Python 3.11.9 compatible)
csv_ready: bool = False
csv_download_progress: int = 0
csv_file_path: Path = Path("/tmp/master_data.csv")
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

# ============================================================================
# Async Download Functions (Python 3.11.9 Compatible)
# ============================================================================
async def stream_download_from_azure(url: str, dest_path: Path, progress_callback: callable) -> bool:
    """
    Downloads file from Azure Blob with streaming and progress tracking.
    
    Python 3.11.9 optimized with proper async/await patterns.
    Compatible with Gunicorn UvicornWorker.
    """
    if not url:
        logger.critical("MASTER_DATA_URL environment variable not set")
        raise ValueError("Cannot start without data source URL")
    
    try:
        # Python 3.11.9: Use ClientTimeout for better control
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes
        async with aiohttp.ClientSession(timeout=timeout) as session:
            logger.info("CSV download started", extra={"url": url, "dest": str(dest_path)})
            
            async with session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                # Ensure destination directory exists
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Python 3.11.9: Use aiofiles for async file operations
                async with aiofiles.open(dest_path, 'wb') as f:
                    # 1MB chunks - optimal for Python 3.11.9
                    async for chunk in response.content.iter_chunked(1024*1024):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Progress tracking
                        if total_size > 0:
                            progress = int((downloaded / total_size) * 100)
                            progress_callback(progress)
                            
                            # Log at 20%, 40%, 60%, 80%, 100%
                            if progress % 20 == 0:
                                downloaded_mb = downloaded // 1_000_000
                                logger.info(f"CSV download progress: {progress}%", 
                                          extra={"downloaded_mb": downloaded_mb})
                
                file_size_mb = dest_path.stat().st_size // 1_000_000
                logger.info("CSV ready for production use", extra={"file_size_mb": file_size_mb})
                return True
                
    except aiohttp.ClientError as e:
        logger.error(f"Azure Blob connection failed: {e}", exc_info=True)
        raise
    except asyncio.TimeoutError:
        logger.error("Download timeout after 5 minutes")
        raise
    except OSError as e:
        logger.error(f"File system error during download: {e}", exc_info=True)
        raise

async def download_csv_background():
    """
    Downloads 900MB master CSV from Azure Blob in background.
    
    Python 3.11.9 compatible implementation.
    Sets csv_ready=True when complete. Runs non-blocking during startup.
    Container boots in <10s regardless of download speed.
    
    Raises: Does NOT raise - logs errors and sets csv_ready=False on failure
    """
    global csv_ready, csv_download_progress
    
    try:
        # Check if file exists (skip download if present)
        if csv_file_path.exists():
            csv_ready = True
            csv_download_progress = 100
            logger.info("Master data already exists, marking as ready")
            return
        
        # Update progress callback to modify global state
        def update_progress(progress: int):
            global csv_download_progress
            csv_download_progress = progress
        
        success = await stream_download_from_azure(
            MASTER_DATA_URL, 
            csv_file_path, 
            update_progress
        )
        
        if success:
            csv_ready = True
            csv_download_progress = 100
            logger.info("Background download completed successfully")
        else:
            csv_ready = False
            logger.error("Background download failed")
            
    except Exception as e:
        logger.error(f"Background download failed: {e}", exc_info=True)
        csv_ready = False

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
    """Handle application startup and shutdown - Python 3.11.9 compatible"""
    logger.info("🚀 Starting SCM Digital Twin API")
    
    # Ensure data directory exists
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    logger.info(f"📁 Using data directory: {data_dir.absolute()}")
    
    # START BACKGROUND DOWNLOAD (NON-BLOCKING)
    # Python 3.11.9: Create task in lifespan
    asyncio.create_task(download_csv_background())
    logger.info("🔄 Background download started - app ready immediately")
    
    # YIELD IMMEDIATELY (STARTUP < 10s)
    # Gunicorn UvicornWorker will handle the async context properly
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down...")
    if csv_file_path.exists():
        csv_file_path.unlink()
        logger.info("Cleaned up temporary CSV file")
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
# Health Endpoint
# ============================================================================
@app.get("/health")
def health_check():
    """Health check endpoint with CSV loading status"""
    headers = {}
    message = "Ready"
    
    # Defensive programming
    if csv_download_progress > 100:
        logger.warning(f"Invalid progress value: {csv_download_progress}, resetting to 100")
        csv_download_progress = 100
    
    if not csv_ready:
        headers["Retry-After"] = "30"
        message = "CSV loading..."
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "csv_ready": csv_ready,
            "csv_progress": csv_download_progress,
            "message": message
        },
        headers=headers
    )

# ============================================================================
# Exception Handlers
# ============================================================================
from fastapi.responses import JSONResponse
from app.validators.network_validators import NetworkValidationError

@app.exception_handler(NetworkValidationError)
async def validation_exception_handler(request, exc):
    """Handle validation errors with proper HTTP status codes"""
    return JSONResponse(
        status_code=400,
        content={"status": "error", "error": {"code": "VALIDATION_ERROR", "message": str(exc)}}
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

