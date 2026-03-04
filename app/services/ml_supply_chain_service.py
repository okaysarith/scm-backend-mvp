import logging
import json
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

from ..utils.sales_data_preprocessor import SalesDataPreprocessor, DataValidationResult
from ..utils.model_monitoring import ModelMonitor

logger = logging.getLogger(__name__)

class MLSupplyChainService:
    """Machine Learning service for supply chain predictions"""
    
    def __init__(self, model_dir: str = "ml_models"):
        self.model_dir = Path(model_dir)
        self.models = {}
        self.features = {}
        self.metrics = {}
        self.monitor = None
        self._initialized = False
        logger.info("ML Service initialized (lazy loading)")
    
    def _ensure_initialized(self):
        """Lazy initialization of monitor and models"""
        if not self._initialized:
            self.monitor = ModelMonitor()
            self._ensure_model_dir()
            self._load_models()
            self._load_features()
            self._initialized = True
            logger.info("ML Service fully initialized")
    
    def _get_features_path(self, model_name: str) -> Path:
        """Get path for a features file"""
        return self.model_dir / f"{model_name}_features.json"
    
    def _save_features(self, model_name: str, features: Dict):
        """Save feature information to disk"""
        features_path = self._get_features_path(model_name)
        with open(features_path, 'w') as f:
            json.dump(features, f, indent=2)
    
    def _load_features(self):
        """Load feature information from disk"""
        for features_file in self.model_dir.glob("*_features.json"):
            try:
                model_name = features_file.stem.replace("_features", "")
                with open(features_file, 'r') as f:
                    self.features[model_name] = json.load(f)
                logger.info(f"Loaded features for model: {model_name}")
            except Exception as e:
                logger.error(f"Error loading features {features_file}: {e}")
    
    def _ensure_model_dir(self):
        """Ensure model directory exists"""
        self.model_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_model_path(self, model_name: str) -> Path:
        """Get path for a model file"""
        return self.model_dir / f"{model_name}.joblib"
    
    def _load_models(self):
        """Load all saved models from disk"""
        for model_file in self.model_dir.glob("*.joblib"):
            try:
                model_name = model_file.stem
                self.models[model_name] = joblib.load(model_file)
                logger.info(f"Loaded model: {model_name}")
            except Exception as e:
                logger.error(f"Error loading model {model_file}: {e}")
    
    def train_demand_forecast_model(
        self,
        data: pd.DataFrame,
        target_column: str,
        date_column: str = 'date',
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Dict:
        """Train a demand forecasting model"""
        try:
            self._ensure_initialized()
            
            # Preprocess the data with sales data preprocessor
            preprocessor = SalesDataPreprocessor()
            df_processed = preprocessor.preprocess_sales_data(
                data,
                date_column=date_column,
                target_column=target_column
            )
            
            # Prepare features and target
            X = df_processed.drop(columns=[target_column])
            y = df_processed[target_column]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            
            # Train model
            model = RandomForestRegressor(n_estimators=100, random_state=random_state)
            model.fit(X_train, y_train)
            
            # Make predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Calculate metrics
            train_score = model.score(X_train, y_train)
            test_score = model.score(X_test, y_test)
            mae = mean_absolute_error(y_test, y_pred_test)
            mse = mean_squared_error(y_test, y_pred_test)
            
            # Get feature importances
            feature_importances = {
                feature: importance 
                for feature, importance in zip(X.columns, model.feature_importances_)
            }
            
            # Log performance
            self.monitor.log_performance(
                model_name="demand_forecast",
                y_true=y_test,
                y_pred=y_pred_test,
                feature_importance=feature_importances
            )
            
            # Save model
            model_name = "demand_forecast"
            model_path = self._get_model_path(model_name)
            joblib.dump(model, model_path)
            self.models[model_name] = model
            
            # Save metrics
            self.metrics[model_name] = {
                "train_score": train_score,
                "test_score": test_score,
                "mae": mae,
                "mse": mse,
                "last_trained": datetime.utcnow().isoformat(),
                "feature_importances": feature_importances
            }
            
            # Save feature information
            feature_info = {
                "features": list(X.columns),
                "target": target_column,
                "feature_types": {
                    col: str(X[col].dtype) for col in X.columns
                }
            }
            self.features[model_name] = feature_info
            self._save_features(model_name, feature_info)
            
            return {
                "status": "success",
                "model": model_name,
                "metrics": self.metrics[model_name],
                "features": self.features[model_name]
            }
            
        except Exception as e:
            logger.error(f"Error training demand forecast model: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def train_inventory_forecast_model(
        self,
        data: pd.DataFrame,
        target_column: str = 'inventory_level',
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Dict:
        """Train an inventory forecasting model"""
        try:
            self._ensure_initialized()
            
            # Prepare features and target
            X = data.drop(columns=[target_column])
            y = data[target_column]
            
            # Encode categorical variables
            X = pd.get_dummies(X, columns=['inventory_class'], drop_first=True)
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            
            # Train model
            model = RandomForestRegressor(n_estimators=100, random_state=random_state)
            model.fit(X_train, y_train)
            
            # Make predictions
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            r2 = model.score(X_test, y_test)
            
            # Get feature importances
            feature_importances = {
                feature: importance 
                for feature, importance in zip(X.columns, model.feature_importances_)
            }
            
            # Save model
            model_name = "inventory_forecast"
            model_path = self._get_model_path(model_name)
            joblib.dump(model, model_path)
            self.models[model_name] = model
            
            # Save metrics
            self.metrics[model_name] = {
                "train_score": r2,
                "test_score": r2,
                "mae": mae,
                "mse": mse,
                "last_trained": datetime.utcnow().isoformat(),
                "feature_importances": feature_importances
            }
            
            # Save feature information
            feature_info = {
                "features": list(X.columns),
                "target": target_column,
                "feature_types": {
                    col: str(X[col].dtype) for col in X.columns
                }
            }
            self.features[model_name] = feature_info
            self._save_features(model_name, feature_info)
            
            return {
                "status": "success",
                "model": model_name,
                "metrics": self.metrics[model_name],
                "features": self.features[model_name]
            }
            
        except Exception as e:
            logger.error(f"Error training inventory forecast model: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def predict_inventory_7_days(
        self,
        hub_pincode: int,
        inventory_class: str,
        model_name: str = "inventory_forecast"
    ) -> Dict:
        """Predict inventory levels for 7 days"""
        try:
            if model_name not in self.models:
                return {
                    "status": "error",
                    "message": f"Model {model_name} not found"
                }
            
            features = self.features.get(model_name, {}).get("features", [])
            if not features:
                return {
                    "status": "error",
                    "message": "Feature information not found"
                }
            
            # Generate predictions for 7 days
            predictions = []
            model = self.models[model_name]
            
            for day in range(7):
                # Prepare input data for this day
                input_data = {
                    'hub_pincode': hub_pincode,
                    'day_of_week': day
                }
                
                # Add class encoding
                if inventory_class == 'B':
                    input_data['inventory_class_B'] = 1
                elif inventory_class == 'C':
                    input_data['inventory_class_C'] = 1
                
                # Prepare input DataFrame
                input_df = pd.DataFrame([input_data])
                input_df = input_df.reindex(columns=features, fill_value=0)
                
                # Make prediction
                prediction = model.predict(input_df)[0]
                
                predictions.append({
                    "day": day + 1,
                    "day_name": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][day],
                    "predicted_inventory": float(prediction),
                    "hub_pincode": hub_pincode,
                    "inventory_class": inventory_class
                })
            
            return {
                "status": "success",
                "model": model_name,
                "predictions": predictions,
                "hub_pincode": hub_pincode,
                "inventory_class": inventory_class,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error making inventory prediction: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def predict_demand(
        self,
        input_data: Dict[str, Any],
        model_name: str = "demand_forecast"
    ) -> Dict:
        """Predict demand using the trained model"""
        try:
            if model_name not in self.models:
                return {
                    "status": "error",
                    "message": f"Model {model_name} not found"
                }
            
            features = self.features.get(model_name, {}).get("features", [])
            if not features:
                return {
                    "status": "error",
                    "message": "Feature information not found"
                }
            
            # Prepare input data
            input_df = pd.DataFrame([input_data])
            input_df = input_df.reindex(columns=features, fill_value=0)
            
            # Make prediction
            model = self.models[model_name]
            prediction = model.predict(input_df)[0]
            
            return {
                "status": "success",
                "prediction": float(prediction),
                "model": model_name,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_model_info(self, model_name: str) -> Dict:
        """Get information about a trained model"""
        if model_name in self.models:
            return {
                "status": "success",
                "model": model_name,
                "metrics": self.metrics.get(model_name, {}),
                "features": self.features.get(model_name, {}),
                "last_updated": datetime.utcnow().isoformat()
            }
        return {
            "status": "error",
            "message": f"Model {model_name} not found"
        }
    
    def get_available_models(self) -> Dict:
        """Get list of all available models"""
        return {
            "status": "success",
            "models": list(self.models.keys()),
            "count": len(self.models)
        }
    
    def validate_training_data(
        self,
        data: List[Dict[str, Any]],
        target_column: str,
        date_column: str = 'date'
    ) -> DataValidationResult:
        """Validate training data before model training"""
        preprocessor = SalesDataPreprocessor()
        return preprocessor.validate_sales_data(data, target_column, date_column)
    
    def detect_features(
        self,
        data: List[Dict[str, Any]],
        date_column: str = 'date'
    ) -> Dict[str, List[str]]:
        """Auto-detect feature types from data"""
        try:
            df = pd.DataFrame(data)
            preprocessor = SalesDataPreprocessor()
            return preprocessor.detect_features(df, date_column)
        except Exception as e:
            logger.error(f"Error detecting features: {e}")
            return {
                'date_features': [],
                'numeric_features': [],
                'categorical_features': [],
                'target_candidates': []
            }
    
    def parse_file_upload(
        self,
        file_content: str,
        file_type: str = 'csv'
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Parse uploaded file content"""
        # CSV and JSON parsing only (Excel support removed)
        from io import StringIO
        try:
            if file_type.lower() == 'csv':
                df = pd.read_csv(StringIO(file_content))
            elif file_type.lower() == 'json':
                df = pd.read_json(StringIO(file_content))
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Convert to list of dictionaries
            data = df.to_dict('records')
            
            metadata = {
                'columns': list(df.columns),
                'row_count': len(df),
                'file_type': file_type,
                'sample_data': df.head(3).to_dict('records')
            }
            
            return data, metadata
            
        except Exception as e:
            raise ValueError(f"Error parsing file: {str(e)}")
    
    def batch_predict_demand(
        self,
        input_data_list: List[Dict[str, Any]],
        model_name: str = "demand_forecast"
    ) -> Dict:
        """Make batch demand predictions using the trained model"""
        try:
            if model_name not in self.models:
                return {
                    "status": "error",
                    "message": f"Model {model_name} not found"
                }
            
            features = self.features.get(model_name, {}).get("features", [])
            if not features:
                return {
                    "status": "error",
                    "message": "Feature information not found"
                }
            
            # Prepare input data
            input_df = pd.DataFrame(input_data_list)
            input_df = input_df.reindex(columns=features, fill_value=0)
            
            # Make predictions
            model = self.models[model_name]
            predictions = model.predict(input_df)
            
            # Return results with metadata
            results = []
            for i, pred in enumerate(predictions):
                results.append({
                    "index": i,
                    "prediction": float(pred),
                    "input_features": input_data_list[i]
                })
            
            return {
                "status": "success",
                "predictions": results,
                "model": model_name,
                "timestamp": datetime.utcnow().isoformat(),
                "count": len(predictions)
            }
            
        except Exception as e:
            logger.error(f"Error making batch prediction: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_model_performance_history(
        self,
        model_name: str,
        limit: int = 100
    ) -> Dict:
        """Get performance history for a specific model"""
        try:
            history = self.monitor.get_performance_history(model_name, limit)
            
            # Convert to serializable format
            performance_data = []
            for perf in history:
                performance_data.append({
                    "timestamp": perf.timestamp.isoformat(),
                    "metrics": perf.metrics,
                    "feature_importance": perf.feature_importance,
                    "data_drift": perf.data_drift
                })
            
            return {
                "status": "success",
                "model": model_name,
                "performance_history": performance_data,
                "count": len(performance_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting performance history: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

# Create singleton instance
ml_service = MLSupplyChainService()