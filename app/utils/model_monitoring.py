import logging
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from pydantic import BaseModel
from pathlib import Path

logger = logging.getLogger(__name__)

class ModelPerformanceMetrics(BaseModel):
    """Model performance metrics"""
    model_name: str
    timestamp: datetime
    metrics: Dict[str, float]
    data_drift: Optional[Dict[str, Any]] = None
    feature_importance: Optional[Dict[str, float]] = None

class ModelMonitor:
    """Class for monitoring ML model performance"""
    
    def __init__(self, storage_path: str = "monitoring"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.metrics_history = []
    
    def log_performance(
        self,
        model_name: str,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        feature_importance: Optional[Dict[str, float]] = None,
        data_drift: Optional[Dict[str, Any]] = None
    ) -> ModelPerformanceMetrics:
        """Log model performance metrics."""
        from sklearn.metrics import (
            mean_absolute_error,
            mean_squared_error,
            r2_score,
            mean_absolute_percentage_error
        )
        
        metrics = {
            "mae": float(mean_absolute_error(y_true, y_pred)),
            "mse": float(mean_squared_error(y_true, y_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "r2": float(r2_score(y_true, y_pred)),
            "mape": float(mean_absolute_percentage_error(y_true, y_pred))
        }
        
        if feature_importance is None:
            feature_importance = {}
            
        if data_drift is None:
            data_drift = {}
        
        performance = ModelPerformanceMetrics(
            model_name=model_name,
            timestamp=datetime.utcnow(),
            metrics=metrics,
            feature_importance=feature_importance,
            data_drift=data_drift
        )
        
        self.metrics_history.append(performance)
        self._save_metrics(performance)
        
        return performance
    
    def _save_metrics(self, metrics: ModelPerformanceMetrics):
        """Save metrics to storage"""
        # Sanitize the timestamp for filename
        timestamp_str = metrics.timestamp.isoformat().replace(':', '-')
        filename = self.storage_path / f"{metrics.model_name}_{timestamp_str}.json"
        with open(filename, 'w') as f:
            f.write(metrics.json())
    
    def get_performance_history(
        self,
        model_name: Optional[str] = None,
        limit: int = 100
    ) -> List[ModelPerformanceMetrics]:
        """Get performance history for a model or all models."""
        if model_name:
            history = [m for m in self.metrics_history if m.model_name == model_name]
        else:
            history = self.metrics_history
            
        return sorted(history, key=lambda x: x.timestamp, reverse=True)[:limit]
