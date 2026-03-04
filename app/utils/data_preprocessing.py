import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import re
from io import StringIO
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

class DataValidationResult(BaseModel):
    """Data validation result"""
    valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    detected_types: Dict[str, str] = {}
    missing_columns: List[str] = []
    extra_columns: List[str] = []
    sample_data: List[Dict[str, Any]] = []
    row_count: int = 0

class DataPreprocessor:
    """Utility class for preprocessing supply chain data"""
    
    @staticmethod
    def build_baseline(picks_df: pd.DataFrame, returns_df: pd.DataFrame, 
                      sku: str, pincodes: List[str], warehouse: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Build baseline dataset for current warehouse configuration"""
        # Filter picks for baseline
        base = picks_df[
            (picks_df["SKU Code"] == sku) &
            (picks_df["Customer Pincode"].isin(pincodes)) &
            (picks_df["Warehouse ID"] == warehouse)
        ].copy()
        
        # Get returns for baseline orders
        if not base.empty and "Order ID" in base.columns and "Order ID" in returns_df.columns:
            base_returns = returns_df.merge(
                base[["Order ID"]],
                on="Order ID",
                how="inner"
            )
        else:
            base_returns = pd.DataFrame()
        
        return base, base_returns
    
    @staticmethod
    def build_scenario_proxy(picks_df: pd.DataFrame, returns_df: pd.DataFrame,
                           sku: str, pincodes: List[str], new_warehouse: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Build scenario dataset using proposed warehouse as proxy"""
        # Get proxy data from proposed warehouse (same SKU, ignore pincode matching if needed)
        proxy = picks_df[
            (picks_df["SKU Code"] == sku) &
            (picks_df["Warehouse ID"] == new_warehouse)
        ].copy()
        
        # If no exact matches, try with just warehouse and similar SKUs
        if proxy.empty:
            # Get all data from proposed warehouse as proxy
            proxy = picks_df[picks_df["Warehouse ID"] == new_warehouse].copy()
            logger.warning(f"No exact SKU matches for {sku} in warehouse {new_warehouse}, using all warehouse data as proxy")
        
        # Get returns for proxy orders
        if not proxy.empty and "Order ID" in proxy.columns and "Order ID" in returns_df.columns:
            proxy_returns = returns_df.merge(
                proxy[["Order ID"]],
                on="Order ID",
                how="inner"
            )
        else:
            proxy_returns = pd.DataFrame()
        
        return proxy, proxy_returns
    
    @staticmethod
    def preprocess_sales_data(
        df: pd.DataFrame,
        date_column: str = 'date',
        target_column: str = 'quantity'
    ) -> pd.DataFrame:
        """Preprocess sales data for demand forecasting."""
        df = df.copy()
        
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
            df['year'] = df[date_column].dt.year
            df['month'] = df[date_column].dt.month
            df['day'] = df[date_column].dt.day
            df['day_of_week'] = df[date_column].dt.dayofweek
            df['day_of_year'] = df[date_column].dt.dayofyear
            df['week_of_year'] = df[date_column].dt.isocalendar().week
            df['quarter'] = df[date_column].dt.quarter
            df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
            df['is_month_start'] = df[date_column].dt.is_month_start.astype(int)
            df['is_month_end'] = df[date_column].dt.is_month_end.astype(int)
            df = df.drop(columns=[date_column])
        
        # Handle missing values
        for col in df.columns:
            if col != target_column:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna(df[col].mode()[0])
                else:
                    df[col] = df[col].fillna(df[col].median())
        
        # Convert categorical variables
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
        
        return df
    
    @staticmethod
    def validate_training_data(
        data: List[Dict[str, Any]],
        target_column: str,
        date_column: str = 'date'
    ) -> DataValidationResult:
        """Validate training data before model training"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            errors = []
            warnings = []
            detected_types = {}
            missing_columns = []
            extra_columns = []
            
            # Check if DataFrame is empty
            if df.empty:
                errors.append("Data is empty")
                return DataValidationResult(
                    valid=False,
                    errors=errors,
                    row_count=0
                )
            
            # Check required columns
            required_columns = [target_column]
            if date_column not in df.columns:
                # Try to detect date column
                date_candidates = [col for col in df.columns if any(
                    keyword in col.lower() for keyword in ['date', 'time', 'timestamp']
                )]
                if date_candidates:
                    date_column = date_candidates[0]
                    warnings.append(f"Auto-detected date column: {date_column}")
                else:
                    errors.append("No date column found and no date column specified")
            
            # Check missing required columns
            for col in required_columns:
                if col not in df.columns:
                    missing_columns.append(col)
            
            if missing_columns:
                errors.append(f"Missing required columns: {missing_columns}")
            
            # Detect data types
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to parse as numeric first
                    try:
                        pd.to_numeric(df[col])
                        detected_types[col] = 'numeric'
                    except:
                        # Try to parse as datetime
                        try:
                            pd.to_datetime(df[col])
                            detected_types[col] = 'datetime'
                        except:
                            detected_types[col] = 'categorical'
                else:
                    detected_types[col] = str(df[col].dtype)
            
            # Check for missing values
            missing_counts = df.isnull().sum()
            high_missing_cols = missing_counts[missing_counts > len(df) * 0.5].index.tolist()
            if high_missing_cols:
                warnings.append(f"Columns with >50% missing values: {high_missing_cols}")
            
            # Check target column validity
            if target_column in df.columns:
                target_unique = df[target_column].nunique()
                if target_unique < 2:
                    warnings.append(f"Target column '{target_column}' has only {target_unique} unique values")
                
                # Check if target is numeric
                if detected_types.get(target_column) != 'numeric':
                    warnings.append(f"Target column '{target_column}' is not numeric")
            
            # Sample data for preview
            sample_data = df.head(5).to_dict('records')
            
            return DataValidationResult(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                detected_types=detected_types,
                missing_columns=missing_columns,
                extra_columns=list(set(df.columns) - set(required_columns + [date_column])),
                sample_data=sample_data,
                row_count=len(df)
            )
            
        except Exception as e:
            return DataValidationResult(
                valid=False,
                errors=[f"Validation error: {str(e)}"],
                row_count=0
            )
    
    @staticmethod
    def detect_features(
        df: pd.DataFrame,
        date_column: str = 'date'
    ) -> Dict[str, List[str]]:
        """Auto-detect feature types from data"""
        features = {
            'date_features': [],
            'numeric_features': [],
            'categorical_features': [],
            'target_candidates': []
        }
        
        for col in df.columns:
            if col == date_column:
                continue
                
            if df[col].dtype in ['int64', 'float64']:
                features['numeric_features'].append(col)
                # Numeric columns with reasonable variance could be targets
                if df[col].nunique() > 5 and df[col].std() > 0:
                    features['target_candidates'].append(col)
            elif df[col].dtype == 'object':
                features['categorical_features'].append(col)
                # Try to parse as datetime
                try:
                    pd.to_datetime(df[col])
                    features['date_features'].append(col)
                except:
                    pass
        
        return features
    
    @staticmethod
    def parse_file_upload(
        file_content: str,
        file_type: str = 'csv'
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Parse uploaded file content"""
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
    
    @staticmethod
    def create_lag_features(
        df: pd.DataFrame,
        column: str,
        lags: List[int],
        group_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Create lag features for time series data."""
        df = df.copy()
        
        if group_columns:
            for lag in lags:
                df[f'{column}_lag_{lag}'] = df.groupby(group_columns)[column].shift(lag)
        else:
            for lag in lags:
                df[f'{column}_lag_{lag}'] = df[column].shift(lag)
                
        return df
