from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

# Import the service
try:
    from app.services.inventory_prediction_service import InventoryPredictionService
    prediction_service = InventoryPredictionService()
except Exception as e:
    logger.error(f"Failed to initialize prediction service: {e}")
    prediction_service = None


router = APIRouter(prefix="/api/ml", tags=["inventory-prediction"])


# ==================== REQUEST/RESPONSE MODELS ====================

class InventoryPredictionRequest(BaseModel):
    """Inventory prediction request"""
    hub_pincode: str
    sku_class: str  # "A", "B", or "C"
    last_inventory_qty: Optional[float] = None  # Current inventory level


class BatchPredictionRequest(BaseModel):
    """Batch prediction for all classes"""
    hub_pincode: str
    last_inventory_dict: Dict[str, float]  # {"A": 100, "B": 150, "C": 200}


# ==================== ENDPOINTS ====================

@router.get("/inventory/hubs")
async def get_available_hubs():
    """
    Get list of available hub pincodes

    Returns:
        {"hubs": ["600055", "560067", ...]}
    """
    if prediction_service is None:
        raise HTTPException(status_code=503, detail="Prediction service not initialized")

    hubs = prediction_service.get_available_hubs()
    return {"hubs": hubs, "count": len(hubs)}


@router.get("/inventory/classes")
async def get_available_classes():
    """
    Get list of available inventory classes

    Returns:
        {"classes": ["A", "B", "C"]}
    """
    if prediction_service is None:
        raise HTTPException(status_code=503, detail="Prediction service not initialized")

    classes = prediction_service.get_available_classes()
    return {"classes": classes}


@router.post("/inventory/predict")
async def predict_inventory(request: InventoryPredictionRequest):
    """
    Predict inventory levels for 7 days

    Request body:
    {
        "hub_pincode": "600055",
        "sku_class": "A",
        "last_inventory_qty": 150
    }

    Response:
    {
        "status": "success",
        "hub_pincode": "600055",
        "sku_class": "A",
        "forecast": [
            {
                "date": "2025-12-31",
                "day": "Wednesday",
                "predicted_qty": 145,
                "confidence": 0.80
            },
            ...
        ],
        "summary": {
            "avg_7day_qty": 150,
            "min_qty": 140,
            "max_qty": 160,
            "trend": "stable",
            "trend_change": 5,
            "recommendation": "monitor",
            "model_confidence": 0.85
        }
    }
    """
    if prediction_service is None:
        raise HTTPException(status_code=503, detail="Prediction service not initialized")

    try:
        result = prediction_service.predict(
            hub_pincode=request.hub_pincode,
            sku_class=request.sku_class,
            last_inventory_value=request.last_inventory_qty
        )

        if result['status'] == 'error':
            raise HTTPException(status_code=400, detail=result['message'])

        return result

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/inventory/predict-batch")
async def predict_inventory_batch(request: BatchPredictionRequest):
    """
    Predict inventory for all classes (A, B, C) at once

    Request body:
    {
        "hub_pincode": "600055",
        "last_inventory_dict": {
            "A": 100,
            "B": 150,
            "C": 200
        }
    }

    Response:
    {
        "hub_pincode": "600055",
        "classes": {
            "A": {...forecast...},
            "B": {...forecast...},
            "C": {...forecast...}
        }
    }
    """
    if prediction_service is None:
        raise HTTPException(status_code=503, detail="Prediction service not initialized")

    try:
        result = prediction_service.batch_predict(
            hub_pincode=request.hub_pincode,
            last_inventory_dict=request.last_inventory_dict
        )
        return result

    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory/health")
async def health_check():
    """Health check for prediction service"""
    if prediction_service is None:
        return {
            "status": "error",
            "message": "Prediction service not initialized",
            "models_loaded": 0
        }

    return {
        "status": "ok",
        "models_loaded": len(prediction_service.models),
        "hubs": len(prediction_service.hubs),
        "classes": len(prediction_service.classes),
    }