from fastapi import APIRouter, HTTPException, Depends, status,Query
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
from pydantic import BaseModel, Field
from app.services.cosmos_service import CosmosService
from app.services.file_service import FileService
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for request/response validation
class TelemetryData(BaseModel):
    """Model for telemetry data points"""
    id: str = Field(..., description="Unique identifier for the telemetry point")
    deviceId: str = Field(..., description="ID of the device sending the telemetry")
    metric: str = Field(..., description="Name of the metric being measured")
    value: Any = Field(..., description="Value of the measurement")
    status: Optional[str] = Field(None, description="Operational status of the device")
    ts: str = Field(..., description="ISO 8601 timestamp of the measurement")
    source: Optional[str] = Field("iot-hub", description="Source of the telemetry data")

class TelemetryResponse(BaseModel):
    """Standard response model for telemetry operations"""
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None

logger = logging.getLogger(__name__)
router = APIRouter()

decoded_telemetry = []  # In-memory storage for demo purposes

@router.get(
    "/query",
    response_model=List[TelemetryData],
    summary="Query Telemetry Data",
    description="Fetch telemetry data for a specific device and optional metric"
)
async def query_telemetry(
    device_id: str = Query(..., description="ID of the device to query"),
    minutes: int = Query(5, ge=1, le=1440, description="Time window in minutes (1-1440)"),
    metric: Optional[str] = Query(None, description="Optional metric name to filter by")
) -> List[TelemetryData]:
    """
    Query telemetry data for a specific device.
    
    Args:
        device_id: The ID of the device to query
        minutes: Time window in minutes (1-1440, default: 5)
        metric: Optional metric name to filter by
        
    Returns:
        List of telemetry data points matching the query
    """
    try:
        since = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
        
        # Filter data (in-memory for demo)
        results = [
            item for item in decoded_telemetry 
            if item.deviceId == device_id and item.ts >= since
        ]
        
        if metric:
            results = [item for item in results if item.metric == metric]
            
        return sorted(results, key=lambda x: x.ts, reverse=True)
        
    except Exception as e:
        logger.exception(f"Query error for device {device_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to query telemetry data"
        )

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=TelemetryResponse,
    summary="Add Telemetry Data",
    description="Add a new telemetry data point"
)
async def add_telemetry(data: TelemetryData) -> TelemetryResponse:
    """
    Add a new telemetry data point.
    
    Args:
        data: The telemetry data to add
        
    Returns:
        Confirmation of the added telemetry data
    """
    try:
        decoded_telemetry.append(data)
        logger.info(f"Added telemetry for device {data.deviceId}")
        return TelemetryResponse(
            status="success",
            message="Telemetry data added successfully",
            data={"id": data.id, "deviceId": data.deviceId, "metric": data.metric}
        )
    except Exception as e:
        logger.exception(f"Failed to add telemetry data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add telemetry data"
        )

@router.get(
    "/devices",
    response_model=List[str],
    summary="List Devices",
    description="Get a list of unique device IDs with telemetry data"
)
async def get_devices() -> List[str]:
    """
    Get a list of unique device IDs that have telemetry data.
    
    Returns:
        List of unique device IDs
    """
    try:
        devices = {item.deviceId for item in decoded_telemetry if hasattr(item, 'deviceId')}
        return sorted(list(devices))
    except Exception as e:
        logger.exception("Failed to get device list")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve device list"
        )

@router.get(
    "/metrics",
    response_model=List[str],
    summary="List Metrics",
    description="Get a list of unique metric names, optionally filtered by device"
)
async def get_metrics(
    device_id: Optional[str] = Query(
        None, 
        description="Optional device ID to filter metrics by"
    )
) -> List[str]:
    """
    Get a list of unique metric names.
    
    Args:
        device_id: Optional device ID to filter metrics by
        
    Returns:
        List of unique metric names
    """
    try:
        metrics = set()
        for item in decoded_telemetry:
            if device_id and item.deviceId != device_id:
                continue
            if hasattr(item, 'metric') and item.metric:
                metrics.add(item.metric)
        return sorted(list(metrics))
    except Exception as e:
        logger.exception("Failed to get metrics list")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve metrics list"
        )
