from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import Dict, Any, List
import logging
import pandas as pd
from datetime import datetime

from app.services.ml_supply_chain_service import ml_service
from ..models.ml_models import (
    TrainModelRequest,
    PredictionRequest,
    ModelInfoResponse,
    TrainModelResponse,
    FileUploadRequest,
    DataValidationResponse,
    FeatureDetectionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
    ModelPerformanceResponse
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["machine-learning"])

@router.post("/validate-data", response_model=DataValidationResponse)
async def validate_training_data(request: FileUploadRequest):
    """
    Validate training data before model training.
    """
    try:
        # Parse file content
        data, metadata = ml_service.parse_file_upload(
            request.file_content, 
            request.file_type
        )
        
        # Validate data
        validation_result = ml_service.validate_training_data(
            data=data,
            target_column=request.target_column,
            date_column=request.date_column or 'date'
        )
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Error validating training data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/detect-features", response_model=FeatureDetectionResponse)
async def detect_features(
    data: List[Dict[str, Any]],
    date_column: str = 'date'
):
    """
    Auto-detect feature types from data.
    """
    try:
        features = ml_service.detect_features(data, date_column)
        return features
        
    except Exception as e:
        logger.error(f"Error detecting features: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/train/demand-forecast", response_model=TrainModelResponse)
async def train_demand_forecast_model(
    request: TrainModelRequest,
    background_tasks: BackgroundTasks
):
    """
    Train a demand forecasting model with the provided data.
    """
    try:
        # Convert request data to DataFrame
        df = pd.DataFrame(request.data)
        
        # Train model
        result = ml_service.train_demand_forecast_model(
            data=df,
            target_column=request.target_column,
            test_size=request.test_size
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error training demand forecast model: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/train/inventory", response_model=Dict[str, Any])
async def train_inventory_model(request: Dict[str, Any]):
    """
    Train an inventory forecasting model
    """
    try:
        data = request.get("data", [])
        target_column = request.get("target_column", "inventory_level")
        model_name = request.get("model_name", "inventory_forecast")
        
        if not data or not target_column:
            raise HTTPException(status_code=400, detail="data and target_column are required")
        
        # Convert to DataFrame
        import pandas as pd
        df = pd.DataFrame(data)
        
        result = ml_service.train_inventory_forecast_model(
            data=df,
            target_column=target_column
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error training inventory forecast model: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/predict/inventory", response_model=Dict[str, Any])
async def predict_inventory(request: Dict[str, Any]):
    """
    Predict inventory levels for 7 days based on hub pincode and class
    """
    try:
        hub_pincode = request.get("hub_pincode")
        inventory_class = request.get("inventory_class")
        model_name = request.get("model_name", "inventory_forecast")
        
        if not hub_pincode or not inventory_class:
            raise HTTPException(status_code=400, detail="hub_pincode and inventory_class are required")
        
        result = ml_service.predict_inventory_7_days(
            hub_pincode=hub_pincode,
            inventory_class=inventory_class,
            model_name=model_name
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error making inventory prediction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/predict/demand", response_model=Dict[str, Any])
async def predict_demand(request: PredictionRequest):
    """
    Make a demand prediction using the trained model.
    """
    try:
        result = ml_service.predict_demand(
            input_data=request.features,
            model_name=request.model_name or "demand_forecast"
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error making prediction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/predict/batch", response_model=BatchPredictionResponse)
async def batch_predict_demand(request: BatchPredictionRequest):
    """
    Make batch demand predictions using the trained model.
    """
    try:
        result = ml_service.batch_predict_demand(
            input_data_list=request.input_data,
            model_name=request.model_name or "demand_forecast"
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error making batch prediction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models/{model_name}/performance", response_model=ModelPerformanceResponse)
async def get_model_performance_history(model_name: str, limit: int = 100):
    """
    Get performance history for a specific model.
    """
    try:
        result = ml_service.get_model_performance_history(model_name, limit)
        
        if result["status"] == "error":
            raise HTTPException(status_code=404, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error getting model performance: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models/{model_name}", response_model=ModelInfoResponse)
async def get_model_info(model_name: str):
    """
    Get information about a trained model.
    """
    try:
        result = ml_service.get_model_info(model_name)
        if result["status"] == "error":
            raise HTTPException(status_code=404, detail=result["message"])
        return result
    except Exception as e:
        logger.error(f"Error getting model info: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models", response_model=Dict[str, Any])
async def list_models():
    """
    List all available ML models.
    """
    try:
        return ml_service.get_available_models()
    except Exception as e:
        logger.error(f"Error listing models: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))