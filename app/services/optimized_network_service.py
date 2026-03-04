"""
Optimized Network Design Service
Avoids full recomputation on startup with lazy loading and caching
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List
import logging
import json
import time
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

class OptimizedNetworkDesignService:
    """Optimized version with lazy loading and no startup recomputation"""
    
    def __init__(self):
        # Basic initialization
        self.hubs_df = pd.DataFrame()
        self.pincode_hub_mapping = {}
        self.order_data = pd.DataFrame()
        self.pick_data = pd.DataFrame()
        self.compliance_df = pd.DataFrame()
        
        # Status flags
        self.hub_data_loaded = False
        self.csv_data_loaded = False
        self.compliance_calculated = False
        
        # Cache for performance
        self._compliance_cache = {}
        self._nearest_hub_cache = {}
        
        # Lazy loading flags
        self._startup_completed = False
        self._background_loading = False
        
        logger.info("🚀 Optimized Network Design Service initialized")
    
    async def initialize_async(self):
        """Async initialization without blocking startup"""
        if self._startup_completed:
            return
        
        logger.info("🔄 Starting async initialization...")
        
        # Load hub data first (essential)
        await self._load_hub_data_async()
        
        # Start background CSV loading
        asyncio.create_task(self._load_csv_data_background())
        
        self._startup_completed = True
        logger.info("✅ Async initialization completed")
    
    async def _load_hub_data_async(self):
        """Load hub data asynchronously"""
        try:
            base_data_dir = Path("D:/Digital twin/Project Main/Web App/backend/data")
            master_pincode_path = base_data_dir / "Master_data_with_pincodes.csv"
            
            if master_pincode_path.exists():
                logger.info("📍 Loading hub data...")
                master_data = pd.read_csv(master_pincode_path)
                
                # Create pincode to hub mapping
                for _, row in master_data.iterrows():
                    pincode = str(row['Pincode'])
                    hub_code = str(row['Hub Code'])
                    self.pincode_hub_mapping[pincode] = hub_code
                
                # Create hub details
                hub_details = {}
                for _, row in master_data.iterrows():
                    hub_code = str(row['Hub Code'])
                    if hub_code not in hub_details:
                        hub_details[hub_code] = {
                            'pincode': str(row['Pincode']),
                            'location': row['officename'],
                            'latitude': float(row['latitude']),
                            'longitude': float(row['longitude']),
                            'district': row['district'],
                            'statename': row['statename']
                        }
                
                self.hubs_df = pd.DataFrame.from_dict(hub_details, orient='index')
                self.hub_data_loaded = True
                
                logger.info(f"✅ Hub data loaded: {len(self.pincode_hub_mapping)} pincodes, {len(self.hubs_df)} hubs")
            else:
                logger.warning("Hub data file not found")
                
        except Exception as e:
            logger.error(f"Error loading hub data: {e}")
    
    async def _load_csv_data_background(self):
        """Load CSV data in background without blocking"""
        if self._background_loading:
            return
        
        self._background_loading = True
        try:
            logger.info("📊 Starting background CSV loading...")
            start_time = time.time()
            
            base_data_dir = Path("D:/Digital twin/Project Main/Web App/backend/data")
            order_csv_path = base_data_dir / "Order_Data_csv_files/Order Data 28.12.25.csv"
            pick_csv_path = base_data_dir / "Order_Pick_Data_csv_files/Order Pick Data 28.12.25.csv"
            
            if order_csv_path.exists() and pick_csv_path.exists():
                # Load data
                self.order_data = pd.read_csv(order_csv_path)
                self.pick_data = pd.read_csv(pick_csv_path)
                
                # Standardize columns
                self.order_data.columns = self.order_data.columns.str.lower().str.replace(' ', '_')
                self.pick_data.columns = self.pick_data.columns.str.lower().str.replace(' ', '_')
                
                self.csv_data_loaded = True
                
                load_time = time.time() - start_time
                logger.info(f"✅ CSV data loaded in {load_time:.2f}s: {len(self.order_data)} orders, {len(self.pick_data)} picks")
                
                # Start compliance calculation in background
                asyncio.create_task(self._calculate_compliance_background())
            else:
                logger.warning("CSV files not found")
                
        except Exception as e:
            logger.error(f"Error loading CSV data: {e}")
        finally:
            self._background_loading = False
    
    async def _calculate_compliance_background(self):
        """Calculate compliance in background without blocking"""
        if not self.csv_data_loaded or not self.hub_data_loaded:
            return
        
        try:
            logger.info("📊 Starting background compliance calculation...")
            start_time = time.time()
            
            # Merge data
            merged_df = self.order_data.merge(
                self.pick_data[['order_no', 'hub_pincode']], 
                on='order_no', 
                how='inner'
            )
            
            # Standardize data types
            merged_df['pincode'] = merged_df['pincode'].astype(str).str.strip()
            merged_df['hub_pincode'] = merged_df['hub_pincode'].astype(str).str.strip()
            
            # Build mapping
            pincode_to_hub = {}
            for pincode, hub_code in self.pincode_hub_mapping.items():
                pincode_to_hub[str(pincode)] = str(hub_code)
            
            # Add expected hub and check compliance
            merged_df['expected_hub'] = merged_df['pincode'].map(pincode_to_hub)
            merged_df['is_compliant'] = merged_df['hub_pincode'] == merged_df['expected_hub']
            
            # Store results
            self.compliance_df = merged_df.copy()
            self.compliance_calculated = True
            
            # Calculate statistics
            compliant_orders = merged_df['is_compliant'].sum()
            total_orders = len(merged_df)
            compliance_rate = (compliant_orders / total_orders * 100) if total_orders > 0 else 0
            
            calc_time = time.time() - start_time
            logger.info(f"✅ Compliance calculated in {calc_time:.2f}s: {compliance_rate:.2f}% ({compliant_orders:,}/{total_orders:,})")
            
        except Exception as e:
            logger.error(f"Error calculating compliance: {e}")
    
    def find_nearest_hub(self, pincode: str) -> Dict:
        """Find nearest hub with caching"""
        # Check cache first
        if pincode in self._nearest_hub_cache:
            return self._nearest_hub_cache[pincode]
        
        # If hub data not loaded, return placeholder
        if not self.hub_data_loaded:
            return {
                "nearest_hub": "Loading...",
                "hub_pincode": "000000",
                "distance_km": 0,
                "pincode_coordinates": {"lat": 0, "lon": 0}
            }
        
        # Simple implementation - use mapping
        hub_code = self.pincode_hub_mapping.get(str(pincode))
        if hub_code and hub_code in self.hubs_df.index:
            hub_info = self.hubs_df.loc[hub_code]
            result = {
                "nearest_hub": hub_info.get('location', 'Unknown'),
                "hub_pincode": hub_info.get('pincode', '000000'),
                "distance_km": 0,
                "pincode_coordinates": {"lat": hub_info.get('latitude', 0), "lon": hub_info.get('longitude', 0)}
            }
        else:
            result = {
                "nearest_hub": "Not Found",
                "hub_pincode": "000000",
                "distance_km": 0,
                "pincode_coordinates": {"lat": 0, "lon": 0}
            }
        
        # Cache result
        self._nearest_hub_cache[pincode] = result
        return result
    
    def get_compliance_stats(self) -> Dict:
        """Get current compliance statistics"""
        if not self.compliance_calculated:
            return {
                "status": "calculating",
                "message": "Compliance calculation in progress...",
                "total_orders": 0,
                "compliant_orders": 0,
                "compliance_rate": 0,
                "data_loaded": self.csv_data_loaded,
                "hub_data_loaded": self.hub_data_loaded
            }
        
        try:
            compliant_orders = self.compliance_df['is_compliant'].sum()
            total_orders = len(self.compliance_df)
            compliance_rate = (compliant_orders / total_orders * 100) if total_orders > 0 else 0
            
            return {
                "status": "success",
                "total_orders": total_orders,
                "compliant_orders": int(compliant_orders),
                "non_compliant_orders": int(total_orders - compliant_orders),
                "compliance_rate": compliance_rate,
                "data_loaded": self.csv_data_loaded,
                "hub_data_loaded": self.hub_data_loaded,
                "calculation_completed": self.compliance_calculated
            }
        except Exception as e:
            logger.error(f"Error getting compliance stats: {e}")
            return {
                "status": "error",
                "message": str(e),
                "total_orders": 0,
                "compliant_orders": 0,
                "compliance_rate": 0
            }
    
    def get_service_status(self) -> Dict:
        """Get overall service status"""
        return {
            "status": "healthy" if self.hub_data_loaded else "initializing",
            "hub_data_loaded": self.hub_data_loaded,
            "csv_data_loaded": self.csv_data_loaded,
            "compliance_calculated": self.compliance_calculated,
            "startup_completed": self._startup_completed,
            "background_loading": self._background_loading,
            "cache_sizes": {
                "compliance_cache": len(self._compliance_cache),
                "nearest_hub_cache": len(self._nearest_hub_cache)
            },
            "data_volumes": {
                "total_orders": len(self.order_data) if self.csv_data_loaded else 0,
                "total_picks": len(self.pick_data) if self.csv_data_loaded else 0,
                "hub_mappings": len(self.pincode_hub_mapping),
                "hub_locations": len(self.hubs_df)
            }
        }

# Global optimized service instance
optimized_service = None

def get_optimized_service() -> OptimizedNetworkDesignService:
    """Get or create optimized service instance"""
    global optimized_service
    if optimized_service is None:
        optimized_service = OptimizedNetworkDesignService()
    return optimized_service
