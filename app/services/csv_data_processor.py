"""
CSV Data Processor for Supply Chain Management
Handles CSV file loading, date processing, and data analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import sys
import os

# Add Backend A path for data access
backend_a_path = Path("d:/Digital twin/Project Main/Web App/backend - A/backend")
if backend_a_path.exists():
    sys.path.append(str(backend_a_path))

logger = logging.getLogger(__name__)

class CSVDataProcessor:
    """CSV data processor with proper date handling"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.processed_data = {}
    
    def read_csv_with_multiple_engines(self, file_path: Path) -> pd.DataFrame:
        """Read CSV file with proper encoding handling"""
        try:
            # Try different encodings for CSV files
            encodings = ['utf-8', 'latin-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Successfully read {file_path.name} with {encoding} encoding")
                    return df
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail, try with error handling
            df = pd.read_csv(file_path, encoding='utf-8', errors='ignore')
            logger.warning(f"Read {file_path.name} with utf-8 encoding (ignoring errors)")
            return df
            
        except Exception as e:
            logger.error(f"Could not read CSV file {file_path}: {e}")
            raise
    
    def convert_csv_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date columns to proper datetime format"""
        df_converted = df.copy()
        
        # Identify date columns
        date_columns = []
        for col in df_converted.columns:
            col_lower = str(col).lower()
            if any(date_term in col_lower for date_term in ['date', 'order', 'time', 'pick', 'return']):
                date_columns.append(col)
        
        logger.info(f"Found potential date columns: {date_columns}")
        
        # Convert each date column
        for col in date_columns:
            if col in df_converted.columns:
                # Try regular date parsing for CSV dates
                df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce')
                
                # Log conversion results
                invalid_dates = df_converted[col].isna().sum()
                total_dates = len(df_converted[col])
                logger.info(f"  {col}: {total_dates - invalid_dates}/{total_dates} dates converted successfully")
                
                if invalid_dates > 0:
                    logger.warning(f"  {col}: {invalid_dates} invalid dates found")
        
        return df_converted
    
    def load_order_pick_data(self) -> pd.DataFrame:
        """Load order pick data from CSV file"""
        try:
            csv_path = self.data_dir / "Order_Data_csv_files" / "Order Pick Data 28.12.25.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                logger.info(f"Loaded {len(df)} order pick records from CSV")
                return df
            else:
                logger.warning(f"CSV file not found: {csv_path}")
                return self._get_fallback_order_data()
        except Exception as e:
            logger.error(f"Error loading order pick CSV data: {e}")
            return self._get_fallback_order_data()
    
    def _get_fallback_order_data(self) -> pd.DataFrame:
            # Hardcoded sample order data
            sample_orders = [
                {"order_date": "2023-12-25", "pincode": "400001", "quantity": 5, "sku": "SKU001", "location": "Mumbai GPO"},
                {"order_date": "2023-12-25", "pincode": "400002", "quantity": 3, "sku": "SKU002", "location": "Mumbai Central"},
                {"order_date": "2023-12-25", "pincode": "110001", "quantity": 8, "sku": "SKU003", "location": "New Delhi GPO"},
                {"order_date": "2023-12-25", "pincode": "560001", "quantity": 2, "sku": "SKU001", "location": "Bangalore GPO"},
                {"order_date": "2023-12-25", "pincode": "600001", "quantity": 6, "sku": "SKU004", "location": "Chennai GPO"},
                {"order_date": "2023-12-25", "pincode": "500001", "quantity": 4, "sku": "SKU002", "location": "Hyderabad GPO"},
                {"order_date": "2023-12-25", "pincode": "700001", "quantity": 7, "sku": "SKU003", "location": "Kolkata GPO"},
                {"order_date": "2023-12-25", "pincode": "380001", "quantity": 3, "sku": "SKU001", "location": "Ahmedabad GPO"},
                {"order_date": "2023-12-25", "pincode": "411001", "quantity": 5, "sku": "SKU004", "location": "Pune GPO"},
                {"order_date": "2023-12-25", "pincode": "302001", "quantity": 9, "sku": "SKU002", "location": "Jaipur GPO"},
                {"order_date": "2023-12-25", "pincode": "800001", "quantity": 2, "sku": "SKU003", "location": "Patna GPO"},
                {"order_date": "2023-12-25", "pincode": "226001", "quantity": 4, "sku": "SKU001", "location": "Lucknow GPO"},
                {"order_date": "2023-12-25", "pincode": "452001", "quantity": 6, "sku": "SKU004", "location": "Indore GPO"},
                {"order_date": "2023-12-25", "pincode": "395001", "quantity": 3, "sku": "SKU002", "location": "Surat GPO"},
                {"order_date": "2023-12-25", "pincode": "400060", "quantity": 8, "sku": "SKU003", "location": "Navi Mumbai"},
                {"order_date": "2023-12-26", "pincode": "400001", "quantity": 4, "sku": "SKU002", "location": "Mumbai GPO"},
                {"order_date": "2023-12-26", "pincode": "110001", "quantity": 7, "sku": "SKU001", "location": "New Delhi GPO"},
                {"order_date": "2023-12-26", "pincode": "560001", "quantity": 5, "sku": "SKU004", "location": "Bangalore GPO"},
                {"order_date": "2023-12-26", "pincode": "600001", "quantity": 3, "sku": "SKU003", "location": "Chennai GPO"},
                {"order_date": "2023-12-26", "pincode": "500001", "quantity": 6, "sku": "SKU001", "location": "Hyderabad GPO"},
                {"order_date": "2023-12-26", "pincode": "700001", "quantity": 2, "sku": "SKU002", "location": "Kolkata GPO"},
                {"order_date": "2023-12-26", "pincode": "380001", "quantity": 9, "sku": "SKU004", "location": "Ahmedabad GPO"},
                {"order_date": "2023-12-26", "pincode": "411001", "quantity": 4, "sku": "SKU003", "location": "Pune GPO"},
                {"order_date": "2023-12-26", "pincode": "302001", "quantity": 7, "sku": "SKU001", "location": "Jaipur GPO"},
                {"order_date": "2023-12-26", "pincode": "800001", "quantity": 5, "sku": "SKU002", "location": "Patna GPO"},
                {"order_date": "2023-12-26", "pincode": "226001", "quantity": 3, "sku": "SKU004", "location": "Lucknow GPO"},
                {"order_date": "2023-12-26", "pincode": "452001", "quantity": 8, "sku": "SKU003", "location": "Indore GPO"},
                {"order_date": "2023-12-26", "pincode": "395001", "quantity": 2, "sku": "SKU001", "location": "Surat GPO"},
                {"order_date": "2023-12-26", "pincode": "400060", "quantity": 6, "sku": "SKU002", "location": "Navi Mumbai"},
                {"order_date": "2023-12-27", "pincode": "400001", "quantity": 8, "sku": "SKU003", "location": "Mumbai GPO"},
                {"order_date": "2023-12-27", "pincode": "110001", "quantity": 4, "sku": "SKU004", "location": "New Delhi GPO"},
                {"order_date": "2023-12-27", "pincode": "560001", "quantity": 7, "sku": "SKU001", "location": "Bangalore GPO"},
                {"order_date": "2023-12-27", "pincode": "600001", "quantity": 5, "sku": "SKU002", "location": "Chennai GPO"},
                {"order_date": "2023-12-27", "pincode": "500001", "quantity": 3, "sku": "SKU003", "location": "Hyderabad GPO"},
                {"order_date": "2023-12-27", "pincode": "700001", "quantity": 9, "sku": "SKU004", "location": "Kolkata GPO"},
                {"order_date": "2023-12-27", "pincode": "380001", "quantity": 2, "sku": "SKU001", "location": "Ahmedabad GPO"},
                {"order_date": "2023-12-27", "pincode": "411001", "quantity": 6, "sku": "SKU002", "location": "Pune GPO"},
                {"order_date": "2023-12-27", "pincode": "302001", "quantity": 4, "sku": "SKU003", "location": "Jaipur GPO"},
                {"order_date": "2023-12-27", "pincode": "800001", "quantity": 7, "sku": "SKU004", "location": "Patna GPO"},
                {"order_date": "2023-12-27", "pincode": "226001", "quantity": 5, "sku": "SKU001", "location": "Lucknow GPO"},
                {"order_date": "2023-12-27", "pincode": "452001", "quantity": 8, "sku": "SKU002", "location": "Indore GPO"},
                {"order_date": "2023-12-27", "pincode": "395001", "quantity": 3, "sku": "SKU003", "location": "Surat GPO"},
                {"order_date": "2023-12-27", "pincode": "400060", "quantity": 6, "sku": "SKU004", "location": "Navi Mumbai"},
            ]
        except :
            pass
            
            df = pd.DataFrame(sample_orders)
            df['order_date'] = pd.to_datetime(df['order_date'])
            logger.info(f"Loaded {len(df)} hardcoded order records")
            return df
    
    def load_return_data(self) -> pd.DataFrame:
        """Load order return data from CSV file"""
        try:
            csv_path = self.data_dir / "Order_Data_csv_files" / "Order Return Data.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                logger.info(f"Loaded {len(df)} return records from CSV")
                return df
            else:
                logger.warning(f"Return CSV file not found: {csv_path}")
                return self._get_fallback_return_data()
        except Exception as e:
            logger.error(f"Error loading return CSV data: {e}")
            return self._get_fallback_return_data()
    
    def _get_fallback_return_data(self) -> pd.DataFrame:
            # Hardcoded sample return data
            sample_returns = [
                {"return_date": "2023-12-26", "pincode": "400001", "quantity": 1, "sku": "SKU001", "reason": "Damaged"},
                {"return_date": "2023-12-26", "pincode": "110001", "quantity": 2, "sku": "SKU003", "reason": "Wrong Item"},
                {"return_date": "2023-12-27", "pincode": "560001", "quantity": 1, "sku": "SKU001", "reason": "Customer Request"},
                {"return_date": "2023-12-27", "pincode": "600001", "quantity": 1, "sku": "SKU004", "reason": "Damaged"},
                {"return_date": "2023-12-27", "pincode": "500001", "quantity": 1, "sku": "SKU002", "reason": "Wrong Item"},
            ]
            
            df = pd.DataFrame(sample_returns)
            df['return_date'] = pd.to_datetime(df['return_date'])
            logger.info(f"Loaded {len(df)} hardcoded return records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading hardcoded return data: {e}")
            return pd.DataFrame()
    
    def load_master_data(self) -> pd.DataFrame:
        """Load SKU master data from CSV file"""
        try:
            csv_path = self.data_dir / "Master Data v2 dt 27 Dec 2025- SKU Master with Price.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                logger.info(f"Loaded {len(df)} master records from CSV")
                return df
            else:
                logger.warning(f"Master CSV file not found: {csv_path}")
                return self._get_fallback_master_data()
        except Exception as e:
            logger.error(f"Error loading master CSV data: {e}")
            return self._get_fallback_master_data()
    
    def _get_fallback_master_data(self) -> pd.DataFrame:
            # Hardcoded sample master data
            sample_master = [
                {"sku": "SKU001", "abc_class": "A", "price": 999.99, "category": "Electronics"},
                {"sku": "SKU002", "abc_class": "B", "price": 499.99, "category": "Clothing"},
                {"sku": "SKU003", "abc_class": "A", "price": 1299.99, "category": "Electronics"},
                {"sku": "SKU004", "abc_class": "C", "price": 199.99, "category": "Accessories"},
            ]
            
            df = pd.DataFrame(sample_master)
            logger.info(f"Loaded {len(df)} hardcoded master records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading hardcoded master data: {e}")
            return pd.DataFrame()
    
    def create_master_dataframe(self) -> pd.DataFrame:
        """Create master dataframe combining all data sources"""
        logger.info("Creating master dataframe...")
        
        # Load all data sources
        orders_df = self.load_order_pick_data()
        returns_df = self.load_return_data()
        master_df = self.load_master_data()
        
        if orders_df.empty:
            logger.error("No order data available - cannot create master dataframe")
            return pd.DataFrame()
        
        # Start with order data
        result_df = orders_df.copy()
        
        # Merge with master data
        if not master_df.empty and 'sku' in result_df.columns:
            result_df = result_df.merge(master_df, on='sku', how='left')
            logger.info("Merged with SKU master data")
        
        # Add time-based features
        if 'order_date' in result_df.columns:
            result_df['day_of_week'] = result_df['order_date'].dt.dayofweek
            result_df['month'] = result_df['order_date'].dt.month
            result_df['is_weekend'] = result_df['day_of_week'].isin([5, 6]).astype(int)
            logger.info("Added time-based features")
        
        logger.info(f"Master dataframe created: {len(result_df)} rows, {len(result_df.columns)} columns")
        return result_df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> Path:
        """Save dataframe to CSV with proper date formatting"""
        if df.empty:
            raise ValueError("Cannot save empty dataframe")
        
        # Create a copy for saving
        df_to_save = df.copy()
        
        # Ensure datetime columns are properly formatted
        date_columns = df_to_save.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            logger.info(f"Formatting date column '{col}' for CSV export")
            # Convert to ISO format for CSV
            df_to_save[col] = df_to_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Save to CSV
        output_path = self.data_dir / filename
        df_to_save.to_csv(output_path, index=False)
        
        logger.info(f"Data saved to: {output_path}")
        logger.info(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
        
        return output_path
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict:
        """Get summary statistics of the dataframe"""
        if df.empty:
            return {"error": "Empty dataframe"}
        
        summary = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "date_ranges": {},
            "numeric_summary": {},
            "categorical_summary": {}
        }
        
        # Date ranges
        date_cols = df.select_dtypes(include=['datetime64']).columns
        for col in date_cols:
            if not df[col].isna().all():
                summary["date_ranges"][col] = {
                    "min": df[col].min().strftime('%Y-%m-%d'),
                    "max": df[col].max().strftime('%Y-%m-%d')
                }
        
        # Numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if not df[col].isna().all():
                summary["numeric_summary"][col] = {
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "mean": float(df[col].mean()),
                    "std": float(df[col].std())
                }
        
        # Categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            summary["categorical_summary"][col] = {
                "unique_count": df[col].nunique(),
                "top_values": df[col].value_counts().head(5).to_dict()
            }
        
        return summary

# Create singleton instance
csv_data_processor = CSVDataProcessor()
