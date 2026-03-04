import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import re
from io import StringIO
from pydantic import BaseModel

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

class SalesDataPreprocessor:
    """Enhanced preprocessor for actual sales order data"""
    
    def __init__(self):
        self.required_columns = ['order_date', 'qty']  # Minimal required columns
        self.optional_columns = ['order_time', 'customer_code', 'pincode', 'sku', 'sku_class']
        self.date_formats = [
            '%d-%b-%y',  # 13-Oct-25
            '%d-%m-%Y',  # 13-10-2025
            '%Y-%m-%d',  # 2025-10-13
            '%d/%m/%Y',  # 13/10/2025
            '%m/%d/%Y',  # 10/13/2025
        ]
    
    def detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Auto-detect date column from various possible names"""
        date_candidates = []
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
                date_candidates.append(col)
        
        # Try to parse each candidate to find the actual date column
        for col in date_candidates:
            sample_values = df[col].dropna().head(10)
            for fmt in self.date_formats:
                try:
                    pd.to_datetime(sample_values, format=fmt, errors='raise')
                    return col
                except:
                    continue
        
        return None
    
    def detect_quantity_column(self, df: pd.DataFrame) -> Optional[str]:
        """Auto-detect quantity column from various possible names"""
        qty_candidates = []
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if any(keyword in col_lower for keyword in ['qty', 'quantity', 'amount', 'volume']):
                qty_candidates.append(col)
        
        # Check which candidate is actually numeric
        for col in qty_candidates:
            if pd.api.types.is_numeric_dtype(df[col]):
                return col
        
        return None
    
    def preprocess_sales_data(
        self,
        df: pd.DataFrame,
        date_column: str = None,
        target_column: str = None
    ) -> pd.DataFrame:
        """Preprocess actual sales order data for demand forecasting"""
        df = df.copy()
        
        # Auto-detect columns if not provided
        if date_column is None:
            date_column = self.detect_date_column(df)
        if target_column is None:
            target_column = self.detect_quantity_column(df)
        
        if date_column is None:
            raise ValueError("Could not detect date column. Please specify date_column parameter.")
        if target_column is None:
            raise ValueError("Could not detect quantity column. Please specify target_column parameter.")
        
        print(f"Using date column: {date_column}")
        print(f"Using target column: {target_column}")
        
        # Convert date column
        if date_column in df.columns:
            # Try different date formats
            for fmt in self.date_formats:
                try:
                    df[date_column] = pd.to_datetime(df[date_column], format=fmt)
                    break
                except:
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            # Create date features
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
            
            # Drop original date column
            df = df.drop(columns=[date_column])
        
        # Handle time column if exists
        time_columns = [col for col in df.columns if 'time' in col.lower() and col != date_column]
        for time_col in time_columns:
            if time_col in df.columns:
                try:
                    # Extract hour from time if it's a string
                    if df[time_col].dtype == 'object':
                        df['hour'] = pd.to_datetime(df[time_col], format='%H:%M:%S', errors='coerce').dt.hour
                    else:
                        df['hour'] = df[time_col].dt.hour
                    df = df.drop(columns=[time_col])
                except:
                    pass
        
        # Handle missing values for features (not target)
        for col in df.columns:
            if col != target_column:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
                else:
                    df[col] = df[col].fillna(df[col].median() if not df[col].isna().all() else 0)
        
        # Convert categorical variables
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            print(f"Converting categorical columns: {list(categorical_cols)}")
            df = pd.get_dummies(df, columns=categorical_cols, drop_first=True, prefix='cat')
        
        # Ensure target column is numeric
        if target_column in df.columns:
            df[target_column] = pd.to_numeric(df[target_column], errors='coerce')
            df[target_column] = df[target_column].fillna(df[target_column].median())
        
        return df
    
    def validate_sales_data(
        self,
        data: List[Dict[str, Any]],
        target_column: str = None,
        date_column: str = None
    ) -> DataValidationResult:
        """Validate sales data before model training"""
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
            
            # Auto-detect columns if not provided
            if date_column is None:
                date_column = self.detect_date_column(df)
            if target_column is None:
                target_column = self.detect_quantity_column(df)
            
            # Check required columns
            if date_column and date_column not in df.columns:
                missing_columns.append(date_column)
            if target_column and target_column not in df.columns:
                missing_columns.append(target_column)
            
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
            if target_column and target_column in df.columns:
                target_unique = df[target_column].nunique()
                if target_unique < 2:
                    warnings.append(f"Target column '{target_column}' has only {target_unique} unique values")
                
                # Check if target is numeric
                if not pd.api.types.is_numeric_dtype(df[target_column]):
                    warnings.append(f"Target column '{target_column}' is not numeric")
            
            # Sample data for preview
            sample_data = df.head(5).to_dict('records')
            
            return DataValidationResult(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                detected_types=detected_types,
                missing_columns=missing_columns,
                extra_columns=list(set(df.columns) - set([date_column, target_column])),
                sample_data=sample_data,
                row_count=len(df)
            )
            
        except Exception as e:
            return DataValidationResult(
                valid=False,
                errors=[f"Validation error: {str(e)}"],
                row_count=0
            )
    
    def detect_features(
        self,
        df: pd.DataFrame,
        date_column: str = None
    ) -> Dict[str, List[str]]:
        """Auto-detect feature types from sales data"""
        features = {
            'date_features': [],
            'numeric_features': [],
            'categorical_features': [],
            'target_candidates': []
        }
        
        # Auto-detect date column
        if date_column is None:
            date_column = self.detect_date_column(df)
        
        for col in df.columns:
            if col == date_column:
                continue
                
            if pd.api.types.is_numeric_dtype(df[col]):
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
