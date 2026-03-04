from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)
router = APIRouter()

# Get the absolute path to the data directory
#BASE_DIR = Path(__file__).resolve().parent.parent
#DATA_FILE = BASE_DIR / "data" / "telemetry_export_latest.json"

DATA_FILE = Path(r"D:\Digital twin\Project Main\Web App\backend\data\telemetry_export_latest.json")
# Ensure data directory exists
DATA_FILE.parent.mkdir(exist_ok=True)

# Test endpoint to verify file access
@router.get("/api/test/file")
async def test_file_access():
    """Test endpoint to verify file access and show debug info"""
    try:
        file_info = {
            "file_path": str(DATA_FILE.absolute()),
            "file_exists": DATA_FILE.exists(),
            "file_size": DATA_FILE.stat().st_size if DATA_FILE.exists() else 0,
            "current_working_directory": str(Path.cwd()),
            "data_directory_contents": []
        }
        
        if DATA_FILE.parent.exists():
            file_info["data_directory_contents"] = [
                {"name": f.name, "is_file": f.is_file(), "size": f.stat().st_size if f.is_file() else 0}
                for f in DATA_FILE.parent.iterdir()
            ]
            
        if DATA_FILE.exists():
            try:
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    file_info["sample_data"] = data[0] if isinstance(data, list) and len(data) > 0 else "No data"
                    file_info["record_count"] = len(data) if isinstance(data, list) else 0
            except Exception as e:
                file_info["load_error"] = str(e)
        
        return file_info
    except Exception as e:
        return {"error": str(e), "type": type(e).__name__}

@router.get("/api/mock/telemetry")
async def get_telemetry(
    deviceId: Optional[str] = None,
    metric: Optional[str] = None,
    time_range: str = "1h"  # Default to last hour
) -> List[Dict[str, Any]]:
    """
    Get telemetry data with optional filtering.
    
    Args:
        deviceId: Filter by device ID
        metric: Filter by metric name
        time_range: Time range filter (e.g., '1h' for last hour, '1d' for last day)
    """
    try:
        logger.info(f"Loading telemetry data from: {DATA_FILE.absolute()}")
        
        if not DATA_FILE.exists():
            error_msg = f"Telemetry data file not found at: {DATA_FILE.absolute()}"
            logger.error(error_msg)
            raise HTTPException(status_code=404, detail=error_msg)

        # Load the data with error handling
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    error_msg = f"Expected JSON array in {DATA_FILE}, got {type(data).__name__}"
                    logger.error(error_msg)
                    raise HTTPException(status_code=500, detail=error_msg)
                logger.info(f"Successfully loaded {len(data)} records from {DATA_FILE.name}")
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in {DATA_FILE}: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        except Exception as e:
            error_msg = f"Error reading {DATA_FILE}: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

        # Calculate time range
        end_time = datetime.utcnow()
        if time_range.endswith('h'):
            start_time = end_time - timedelta(hours=int(time_range[:-1]))
        elif time_range.endswith('d'):
            start_time = end_time - timedelta(days=int(time_range[:-1]))
        else:
            start_time = end_time - timedelta(hours=1)  # Default to 1 hour

        # Filter data
        filtered_data = []
        invalid_items = 0
        
        for item in data:
            try:
                # Validate required fields
                if not all(key in item for key in ["timestamp", "deviceId", "metric"]):
                    invalid_items += 1
                    continue
                
                # Parse timestamp
                try:
                    item_time = datetime.fromisoformat(str(item["timestamp"]).replace("Z", ""))
                except ValueError as e:
                    logger.warning(f"Invalid timestamp format in item: {e}")
                    invalid_items += 1
                    continue
                
                # Apply filters
                if item_time < start_time:
                    continue
                if deviceId and item.get("deviceId") != deviceId:
                    continue
                if metric and item.get("metric") != metric:
                    continue
                
                filtered_data.append(item)
                
            except Exception as e:
                logger.warning(f"Error processing item: {e}", exc_info=True)
                invalid_items += 1
                continue
        
        if invalid_items > 0:
            logger.warning(f"Skipped {invalid_items} invalid items during processing")

        logger.info(f"Returning {len(filtered_data)} filtered records")
        return filtered_data

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        error_msg = f"Unexpected error processing request: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get("/api/mock/telemetry/metrics")
async def get_available_metrics() -> Dict[str, List[str]]:
    """
    Get list of available metrics and devices
    
    Returns:
        Dictionary with 'devices' and 'metrics' lists
    """
    try:
        logger.info(f"Getting available metrics from {DATA_FILE}")
        
        if not DATA_FILE.exists():
            error_msg = f"Telemetry data file not found: {DATA_FILE}"
            logger.error(error_msg)
            return {"devices": [], "metrics": [], "error": error_msg}

        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                error_msg = f"Expected JSON array in {DATA_FILE}, got {type(data).__name__}"
                logger.error(error_msg)
                return {"devices": [], "metrics": [], "error": error_msg}
                
            devices = set()
            metrics = set()
            
            for item in data:
                if isinstance(item, dict):
                    if "deviceId" in item and item["deviceId"]:
                        devices.add(item["deviceId"])
                    if "metric" in item and item["metric"]:
                        metrics.add(item["metric"])

            result = {
                "devices": sorted(list(devices)),
                "metrics": sorted(list(metrics)),
                "total_records": len(data)
            }
            
            logger.info(f"Found {len(devices)} devices and {len(metrics)} metrics in {len(data)} records")
            return result
            
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in {DATA_FILE}: {str(e)}"
            logger.error(error_msg)
            return {"devices": [], "metrics": [], "error": error_msg}
            
    except Exception as e:
        error_msg = f"Error getting available metrics: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"devices": [], "metrics": [], "error": error_msg}
