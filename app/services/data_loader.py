import pandas as pd
import logging
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class DataLoader:
    """Service for loading real supply chain data from CSV files"""
    
    def __init__(self, data_directory: str = "data"):
        self.data_dir = Path(data_directory)
        self.orders_data = None
        self.warehouses_data = None
        
        # Hardcoded hub locations and pincodes
        self.hub_locations = {
            '781101': 'Village Koraibari, P.S. Kamalpu, Assam',
            '387570': 'Village Hariyala, Ta & District Kheda, Gujarat',
            '563130': 'Malur, Kolar District, Karnataka',
            '560067': 'Bengaluru – Whitefield region, Karnataka',
            '580011': 'Dharwad, Karnataka',
            '600055': 'Chennai – Koduvalli, Tamil Nadu',
            '600067': 'Chennai – Thiruvallur High Road, Tamil Nadu',
            '641032': 'Palladam Road, Othakalmandapam, Tamil Nadu',
            '302026': 'Jaipur – Bagru, Rajasthan',
            '124103': 'Farukhnagar, Haryana',
            '122413': 'Binola, Gurgaon, Haryana',
            '711316': 'Uluberia / Howrah, West Bengal',
            '734007': 'Jalpaiguri Road, West Bengal',
            '501401': 'Medchal (Gundlapochampally Village), Telangana'
        }
        
        # Hardcoded warehouse locations (mapping hubs to warehouse IDs)
        self.warehouse_mapping = {
            'Bengaluru Whitefield (560067)': {'pincode': '560067', 'location': 'Bengaluru Whitefield, Karnataka', 'latitude': 12.9698, 'longitude': 77.7500, 'type': 'Warehouse'},
            'Chennai Koduvalli (600055)': {'pincode': '600055', 'location': 'Chennai Koduvalli, Tamil Nadu', 'latitude': 13.0827, 'longitude': 80.2707, 'type': 'Warehouse'},
            'Mumbai (400001)': {'pincode': '400001', 'location': 'Mumbai, Maharashtra', 'latitude': 19.0760, 'longitude': 72.8777, 'type': 'Warehouse'},
            'Delhi NCR (110001)': {'pincode': '110001', 'location': 'Delhi NCR', 'latitude': 28.6139, 'longitude': 77.2090, 'type': 'Warehouse'},
            'Bengaluru (560068)': {'pincode': '560068', 'location': 'Bengaluru, Karnataka', 'latitude': 12.9716, 'longitude': 77.5946, 'type': 'Warehouse'},
            'Kolkata (700001)': {'pincode': '700001', 'location': 'Kolkata, West Bengal', 'latitude': 22.5726, 'longitude': 88.3639, 'type': 'Warehouse'},
            'Hyderabad (500001)': {'pincode': '500001', 'location': 'Hyderabad, Telangana', 'latitude': 17.3850, 'longitude': 78.4867, 'type': 'Warehouse'},
            'Jaipur (302020)': {'pincode': '302020', 'location': 'Jaipur, Rajasthan', 'latitude': 26.9124, 'longitude': 75.7873, 'type': 'Warehouse'},
            'Gurgaon (122001)': {'pincode': '122001', 'location': 'Gurgaon, Haryana', 'latitude': 28.4595, 'longitude': 77.0266, 'type': 'Warehouse'},
            'Ahmedabad (380001)': {'pincode': '380001', 'location': 'Ahmedabad, Gujarat', 'latitude': 23.0225, 'longitude': 72.5714, 'type': 'Warehouse'}
        }
        
    def load_orders_from_csv(self, csv_file_path: str) -> List[Dict[str, Any]]:
        """Load orders data from CSV and convert to expected format"""
        try:
            full_path = self.data_dir / csv_file_path
            if not full_path.exists():
                logger.warning(f"CSV file not found: {full_path}")
                return self._get_fallback_orders()
            
            # Read CSV with error handling for malformed data
            # For large files, read in chunks and limit to sample size
            if full_path.stat().st_size > 50 * 1024 * 1024:  # 50MB threshold
                logger.info(f"Large file detected ({full_path.stat().st_size/1024/1024:.1f}MB), loading sample...")
                df = pd.read_csv(full_path, on_bad_lines='skip', nrows=10000)
            else:
                df = pd.read_csv(full_path, on_bad_lines='skip')
            logger.info(f"Loaded {len(df)} rows from {csv_file_path}")
            
            # Standardize column names (case insensitive)
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Map CSV columns to expected format
            orders = []
            for index, row in df.iterrows():
                try:
                    # Skip rows with missing essential data
                    if pd.isna(row.get('order_no')) and pd.isna(row.get('order_id')):
                        continue
                        
                    order = {
                        'order_id': row.get('order_no', row.get('order_id', f'ORD{len(orders)+1}')),
                        'sku': row.get('sku', 'SKU001'),
                        'customer_code': row.get('customer_code', f'CUST{len(orders)+1}'),
                        'pincode': str(row.get('pincode', '400001')),
                        'quantity': int(row.get('qty', row.get('quantity', 1))),
                        'order_date': self._safe_parse_date(row.get('order_date', row.get('order_pick_date', '2023-12-25'))),
                        'warehouse_id': self.get_warehouse_by_pincode(str(row.get('pincode', '400001')))
                    }
                    orders.append(order)
                except Exception as row_error:
                    logger.warning(f"Skipping malformed row {index}: {row_error}")
                    continue
            
            logger.info(f"Processed {len(orders)} orders from CSV")
            return orders
            
        except Exception as e:
            logger.error(f"Error processing CSV orders: {e}")
            return self._get_fallback_orders()
    
    def load_warehouses_from_csv(self, csv_file_path: str) -> List[Dict[str, Any]]:
        """Load warehouse data from CSV"""
        try:
            full_path = self.data_dir / csv_file_path
            if not full_path.exists():
                logger.warning(f"CSV file not found: {full_path}")
                return self._get_fallback_warehouses()
            
            df = pd.read_csv(full_path)
            if df.empty:
                return self._get_fallback_warehouses()
            
            # Standardize column names
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Map CSV columns to expected format
            warehouses = []
            for _, row in df.iterrows():
                warehouse = {
                    'warehouse_id': row.get('hub', row.get('warehouse_id', f'W{len(warehouses)+1}')),
                    'latitude': float(row.get('latitude', 19.0760)),
                    'longitude': float(row.get('longitude', 72.8777)),
                    'capacity': int(row.get('capacity', 10000)),
                    'current_utilization': float(row.get('utilization', 0.5))
                }
                warehouses.append(warehouse)
            
            logger.info(f"Processed {len(warehouses)} warehouses from CSV")
            return warehouses
            
        except Exception as e:
            logger.error(f"Error processing CSV warehouses: {e}")
            return self._get_fallback_warehouses()
    
    def _get_fallback_orders(self) -> List[Dict[str, Any]]:
        """Fallback orders if CSV loading fails"""
        logger.warning("Using fallback mock orders")
        return [
            {
                'order_id': 'ORD1001',
                'sku': 'SKU001',
                'warehouse_id': 'W1',
                'pincode': '400001',
                'return_probability': 0.15,
                'on_time': True,
                'order_date': '2025-12-01'
            },
            {
                'order_id': 'ORD1002',
                'sku': 'SKU002',
                'warehouse_id': 'W2',
                'pincode': '400002',
                'return_probability': 0.20,
                'on_time': False,
                'order_date': '2025-12-02'
            }
        ]
    
    def load_csv_data(self, csv_file_path: str) -> pd.DataFrame:
        """Load data from CSV file"""
        try:
            full_path = self.data_dir / csv_file_path
            if not full_path.exists():
                logger.warning(f"CSV file not found: {full_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(full_path)
            logger.info(f"Loaded {len(df)} rows from {csv_file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV file {csv_file_path}: {e}")
            return pd.DataFrame()
    
    def get_available_pincodes(self) -> List[str]:
        """Get all available hub pincodes"""
        return list(self.hub_locations.keys())
    
    def get_pincode_location(self, pincode: str) -> str:
        """Get location description for a pincode"""
        return self.hub_locations.get(pincode, "Unknown Location")
    
    def get_warehouse_by_pincode(self, pincode: str) -> str:
        """Get warehouse ID for a pincode"""
        for warehouse_id, info in self.warehouse_mapping.items():
            if info['pincode'] == pincode:
                return warehouse_id
        return "W1"  # Default fallback
    
    def get_pincode_details(self, pincode: str) -> Dict[str, Any]:
        """Get detailed information about a pincode"""
        if pincode in self.hub_locations:
            return {
                'pincode': pincode,
                'location_name': self.hub_locations[pincode],
                'type': 'Hub',
                'description': self.hub_locations[pincode]
            }
        else:
            # Check if it's a warehouse pincode
            for warehouse_id, info in self.warehouse_mapping.items():
                if info['pincode'] == pincode:
                    return {
                        'pincode': pincode,
                        'location_name': info['location'],
                        'type': 'Warehouse',
                        'description': f"{info['location']} - {info['type']}"
                    }
        
        return {
            'pincode': pincode,
            'location_name': 'Unknown Location',
            'type': 'Unknown',
            'description': 'Location not found in database'
        }
    
    def get_all_locations_with_details(self) -> List[Dict[str, Any]]:
        """Get all locations (hubs and warehouses) with detailed information"""
        locations = []
        seen_pincodes = set()
        
        # Add hubs first
        for pincode, location_name in self.hub_locations.items():
            if pincode not in seen_pincodes:
                locations.append({
                    'pincode': pincode,
                    'display_name': f"{pincode} - {location_name}",
                    'location_name': location_name,
                    'type': 'Hub',
                    'description': location_name
                })
                seen_pincodes.add(pincode)
        
        # Add warehouses only if pincode not already added
        for warehouse_id, info in self.warehouse_mapping.items():
            pincode = info['pincode']
            if pincode not in seen_pincodes:
                locations.append({
                    'pincode': pincode,
                    'display_name': f"{pincode} - {info['location']}",
                    'location_name': info['location'],
                    'type': 'Warehouse',
                    'description': f"{info['location']} - Warehouse"
                })
                seen_pincodes.add(pincode)
        
        return locations
    
    def get_nearest_warehouse(self, pincode: str) -> str:
        """Get nearest warehouse to a pincode (simplified logic)"""
        # For now, return warehouse with same pincode if exists
        warehouse = self.get_warehouse_by_pincode(pincode)
        if warehouse != "W1":
            return warehouse
        
        # Fallback to W1 (Bengaluru) as default
        return "W1"
    
    def _get_fallback_warehouses(self) -> List[Dict[str, Any]]:
        """Fallback warehouses if Excel loading fails"""
        logger.warning("Using fallback mock warehouses")
        return [
            {
                'warehouse_id': 'W1',
                'latitude': 19.0760,
                'longitude': 72.8777,
                'capacity': 1000,
                'current_utilization': 0.75
            },
            {
                'warehouse_id': 'W2',
                'latitude': 28.6139,
                'longitude': 77.2090,
                'capacity': 800,
                'current_utilization': 0.60
            }
        ]
    
    def _safe_parse_date(self, date_value):
        """Safely parse date values with error handling"""
        try:
            # Handle if date_value is a pandas Series or numpy array
            if hasattr(date_value, 'iloc'):
                # It's a pandas Series, take the first value
                date_value = date_value.iloc[0] if len(date_value) > 0 else '2023-12-25'
            
            if pd.isna(date_value) or date_value == '' or str(date_value).strip() == '':
                return pd.to_datetime('2023-12-25')
            
            # Clean the date value - remove .csv and other invalid characters
            date_str = str(date_value).strip()
            if '.csv' in date_str:
                date_str = date_str.replace('.csv', '').strip()
            
            # Handle case where we have extra data like "(01-08-25).csv"
            if '(' in date_str and ')' in date_str:
                # Extract the date part before the parentheses
                date_str = date_str.split('(')[0].strip()
            
            return pd.to_datetime(date_str, errors='coerce')
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_value}': {e}, using default")
            return pd.to_datetime('2023-12-25')

# Global instance
data_loader = DataLoader()
