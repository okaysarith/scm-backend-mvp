"""
Telemetry API for Grafana with Cosmos DB integration
"""
from fastapi import APIRouter, Query, HTTPException, status, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import json
import base64
import logging
from pathlib import Path
from pydantic import BaseModel, Field

router = APIRouter(prefix="/telemetry", tags=["telemetry"])
logger = logging.getLogger(__name__)

# Pydantic models for request/response validation
class TelemetryItem(BaseModel):
    time: str = Field(..., description="Timestamp of the measurement")
    device_id: str = Field(..., alias="deviceId", description="Device identifier")
    metric_name: str = Field(..., alias="metricName", description="Name of the metric")
    metric_value: float = Field(..., alias="metricValue", description="Value of the measurement")
    status: Optional[str] = Field(None, description="Device status")
    source: str = Field("iot-hub", description="Data source")

    class Config:
        allow_population_by_field_name = True

class TelemetryResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    count: Optional[int] = None

# Global cache for backward compatibility
_COSMOS_DATA = None

def load_data():
    """Load cosmos_export.json once (legacy function)"""
    global _COSMOS_DATA
    if _COSMOS_DATA is None:
        try:
            path = Path(__file__).parent.parent.parent / "cosmos_export.json"
            with open(path) as f:
                _COSMOS_DATA = json.load(f)
            logger.info(f"✅ Loaded {len(_COSMOS_DATA)} documents from file")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            _COSMOS_DATA = []
    return _COSMOS_DATA

def normalize(doc: Dict) -> List[Dict]:
    """Convert raw Cosmos doc to flat format (legacy function)"""
    try:
        # Handle Base64 encoded body
        if "Body" in doc:
            decoded = json.loads(base64.b64decode(doc["Body"]).decode("utf-8"))
            device = decoded.get("device_id", "unknown")
            telemetry = decoded.get("telemetry", {})
            metadata = decoded.get("metadata", {})
            
            result = []
            for metric, value in telemetry.items():
                result.append({
                    "time": metadata.get("edge_timestamp", ""),
                    "deviceId": device,
                    "metricName": metric,
                    "metricValue": float(value) if isinstance(value, (int, float)) else 0,
                    "status": metadata.get("operational_status", "unknown"),
                    "source": "iot-hub"
                })
            return result
        
        # Already flat format
        else:
            return [{
                "time": doc.get("timestamp", ""),
                "deviceId": doc.get("deviceId", "unknown"),
                "metricName": doc.get("metricName", ""),
                "metricValue": doc.get("metricValue", 0),
                "status": doc.get("operationalStatus", "unknown"),
                "source": doc.get("source", "legacy")
            }]
    except Exception as e:
        logger.error(f"Error normalizing document: {e}")
        return []

# ============================================================================
# ENDPOINTS
# ============================================================================

@router.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

@router.get("/test")
async def test():
    """Quick test - returns 2 sample records"""
    return [
        {
            "time": datetime.utcnow().isoformat(),
            "deviceId": "CNC_01",
            "metricName": "QualityScore",
            "metricValue": 98.5,
            "status": "operational"
        },
        {
            "time": (datetime.utcnow() - timedelta(minutes=1)).isoformat(),
            "deviceId": "CNC_01",
            "metricName": "QualityScore",
            "metricValue": 97.2,
            "status": "operational"
        }
    ]

@router.get("/all", response_model=List[TelemetryItem])
async def get_telemetry(
    device_id: Optional[str] = Query(None, description="Filter by device ID"),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
    limit: int = Query(100, le=1000, description="Maximum number of records to return"),
    hours: int = Query(24, description="Time window in hours (max 168)")
):
    """Main telemetry endpoint with filtering and pagination"""
    try:
        data = load_data()
        all_records = []
        
        # Normalize documents
        for doc in data[:1000]:  # Limit to first 1000 for performance
            all_records.extend(normalize(doc))
        
        # Apply filters
        if device_id:
            all_records = [r for r in all_records if r.get("deviceId") == device_id]
        if metric_name:
            all_records = [r for r in all_records if r.get("metricName") == metric_name]
        
        # Sort by time (newest first) and limit results
        all_records.sort(key=lambda x: x.get("time", ""), reverse=True)
        
        # Apply time filter if needed
        if hours > 0:
            cutoff = (datetime.utcnow() - timedelta(hours=min(hours, 168))).isoformat()
            all_records = [r for r in all_records if r.get("time", "") >= cutoff]
        
        return all_records[:limit]
        
    except Exception as e:
        logger.error(f"Error in get_telemetry: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch telemetry data"
        )

@router.get("/devices", response_model=List[str])
async def get_devices():
    """List unique device IDs"""
    try:
        data = load_data()
        all_records = []
        for doc in data[:1000]:
            all_records.extend(normalize(doc))
        
        devices = sorted(set(r.get("deviceId") for r in all_records if r.get("deviceId")))
        return devices
    except Exception as e:
        logger.error(f"Error in get_devices: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch device list"
        )

@router.get("/metrics", response_model=List[str])
async def get_metrics(
    device_id: Optional[str] = Query(None, description="Filter metrics by device ID")
):
    """List unique metric names, optionally filtered by device"""
    try:
        data = load_data()
        all_records = []
        for doc in data[:1000]:
            all_records.extend(normalize(doc))
        
        # Apply device filter if specified
        if device_id:
            all_records = [r for r in all_records if r.get("deviceId") == device_id]
        
        metrics = sorted(set(
            r.get("metricName") for r in all_records 
            if r.get("metricName") and (device_id is None or r.get("deviceId") == device_id)
        ))
        return metrics
    except Exception as e:
        logger.error(f"Error in get_metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch metrics list"
        )

@router.get("/quality-score", response_model=List[Dict[str, Any]])
async def quality_score(
    device_id: Optional[str] = Query(None, description="Filter by device ID"),
    hours: int = Query(24, description="Time window in hours (max 168)")
):
    """Quality Score time series with filtering"""
    try:
        data = load_data()
        all_records = []
        for doc in data[:1000]:
            all_records.extend(normalize(doc))
        
        # Apply filters
        result = [
            {
                "time": r["time"], 
                "value": r["metricValue"], 
                "device_id": r["deviceId"],
                "status": r.get("status", "unknown")
            }
            for r in all_records 
            if r.get("metricName") == "QualityScore"
            and (device_id is None or r.get("deviceId") == device_id)
        ]
        
        # Apply time filter
        if hours > 0:
            cutoff = (datetime.utcnow() - timedelta(hours=min(hours, 168))).isoformat()
            result = [r for r in result if r["time"] >= cutoff]
        
        return sorted(result, key=lambda x: x["time"])
    except Exception as e:
        logger.error(f"Error in quality_score: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch quality score data"
        )

@router.get("/defect-count", response_model=List[Dict[str, Any]])
async def defect_count(
    device_id: Optional[str] = Query(None, description="Filter by device ID"),
    hours: int = Query(24, description="Time window in hours (max 168)")
):
    """Defect Count time series with filtering"""
    try:
        data = load_data()
        all_records = []
        for doc in data[:1000]:
            all_records.extend(normalize(doc))
        
        # Apply filters
        result = [
            {
                "time": r["time"], 
                "value": r["metricValue"], 
                "device_id": r["deviceId"],
                "status": r.get("status", "unknown")
            }
            for r in all_records 
            if r.get("metricName") == "DefectCount"
            and (device_id is None or r.get("deviceId") == device_id)
        ]
        
        # Apply time filter
        if hours > 0:
            cutoff = (datetime.utcnow() - timedelta(hours=min(hours, 168))).isoformat()
            result = [r for r in result if r["time"] >= cutoff]
        
        return sorted(result, key=lambda x: x["time"])
    except Exception as e:
        logger.error(f"Error in defect_count: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch defect count data"
        )

# New endpoints for enhanced functionality
@router.get("/latest", response_model=List[TelemetryItem])
async def get_latest_telemetry(
    device_id: Optional[str] = Query(None, description="Filter by device ID"),
    limit: int = Query(10, le=100, description="Number of latest records to return")
):
    """Get the most recent telemetry records"""
    try:
        data = load_data()
        all_records = []
        
        # Normalize and filter documents
        for doc in data[:1000]:
            records = normalize(doc)
            if device_id:
                records = [r for r in records if r.get("deviceId") == device_id]
            all_records.extend(records)
        
        # Sort by time (newest first) and limit results
        all_records.sort(key=lambda x: x.get("time", ""), reverse=True)
        return all_records[:limit]
        
    except Exception as e:
        logger.error(f"Error in get_latest_telemetry: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch latest telemetry"
        )

@router.get("/export", response_model=TelemetryResponse)
async def export_telemetry():
    """Export current telemetry data to a JSON file"""
    try:
        data = load_data()
        all_records = []
        
        # Normalize all documents
        for doc in data:
            all_records.extend(normalize(doc))
        
        # Create export directory if it doesn't exist
        export_dir = Path(__file__).parent.parent.parent / "exports"
        export_dir.mkdir(exist_ok=True)
        
        # Save to file with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        export_path = export_dir / f"telemetry_export_{timestamp}.json"
        
        with open(export_path, 'w') as f:
            json.dump(all_records, f, indent=2, default=str)
        
        return TelemetryResponse(
            success=True,
            data={
                "file_path": str(export_path),
                "record_count": len(all_records),
                "export_time": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error in export_telemetry: {e}")
        return TelemetryResponse(
            success=False,
            error=f"Failed to export telemetry: {str(e)}"
        )
