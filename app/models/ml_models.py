from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum

class FileUploadRequest(BaseModel):
    """Request model for file upload validation"""
    file_content: str = Field(..., description="Base64 encoded file content")
    file_type: str = Field(..., description="File type: 'csv' or 'json'")
    target_column: str = Field(..., description="Target column for training")
    date_column: Optional[str] = Field('date', description="Date column name")

class DataValidationResponse(BaseModel):
    """Response model for data validation"""
    valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    detected_types: Dict[str, str] = {}
    missing_columns: List[str] = []
    extra_columns: List[str] = []
    sample_data: List[Dict[str, Any]] = []
    row_count: int = 0

class FeatureDetectionResponse(BaseModel):
    """Response model for feature detection"""
    date_features: List[str] = []
    numeric_features: List[str] = []
    categorical_features: List[str] = []
    target_candidates: List[str] = []

class BatchPredictionRequest(BaseModel):
    """Request model for batch predictions"""
    input_data: List[Dict[str, Any]] = Field(..., description="List of input feature dictionaries")
    model_name: Optional[str] = Field("demand_forecast", description="Model name to use")

class BatchPredictionResponse(BaseModel):
    """Response model for batch predictions"""
    status: str
    predictions: List[Dict[str, Any]] = []
    model: str
    timestamp: str
    count: int
    message: Optional[str] = None

class ModelPerformanceResponse(BaseModel):
    """Response model for model performance history"""
    status: str
    model: str
    performance_history: List[Dict[str, Any]] = []
    count: int
    message: Optional[str] = None

class TrainModelRequest(BaseModel):
    """Request model for training a new ML model"""
    data: List[Dict[str, Any]] = Field(..., description="Training data as a list of dictionaries")
    target_column: str = Field(..., description="Name of the target column")
    model_name: Optional[str] = Field(None, description="Name to save the model as")
    test_size: float = Field(0.2, ge=0.1, le=0.5, description="Proportion of data to use for testing")
    random_state: int = Field(42, description="Random seed for reproducibility")

class PredictionRequest(BaseModel):
    """Request model for making predictions"""
    features: Dict[str, Any] = Field(..., description="Input features for prediction")
    model_name: Optional[str] = Field(None, description="Name of the model to use for prediction")

class ModelInfoResponse(BaseModel):
    """Response model for model information"""
    status: str
    model: str
    metrics: Dict[str, Any] = {}
    features: Dict[str, Any] = {}
    last_updated: str

class TrainModelResponse(BaseModel):
    """Response model for model training"""
    status: str
    model: str
    metrics: Dict[str, Any] = {}
    features: Dict[str, Any] = {}
    message: Optional[str] = None
