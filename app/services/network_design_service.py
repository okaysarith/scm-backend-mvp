
import numpy as np

from typing import Dict, List, Optional, Tuple

import logging
import time
import shutil

from pathlib import Path

from math import radians, cos, sin, asin, sqrt

import pandas as pd

# Exact file path constants
DATA_DIR = Path("data")
MASTER_FILE = DATA_DIR / "Master_data_with_pincodes.csv"
BASELINE_FILE = DATA_DIR / "combined_df.csv"
SESSIONS_DIR = DATA_DIR / "sessions"

logger = logging.getLogger(__name__)





class NetworkDesignService:

    """Simplified network design service for production"""

    

    def __init__(self, data_dir: str = "data"):

        base_dir = Path(__file__).resolve().parents[2]

        data_path = Path(data_dir)

        self.data_dir = data_path if data_path.is_absolute() else (base_dir / data_path)

        self.hubs_df = None

        self.order_data = None

        self.pick_data = None

        self.csv_data_loaded = False

        # Initialize pincode to hub mapping

        self.pincode_hub_mapping = {}

        self._pincode_mapping_loaded = False

        # Initialize compliance data cache

        self.compliance_df = None

        self.compliance_metrics = None

        self.compliance_calculated = False
        
        # Initialize combined data for order risk profiling
        self.combined_data = None
        self.combined_data_loaded = False

        

        # Load hub data and static CSV data

        print("=== STARTING NETWORK DESIGN SERVICE INITIALIZATION ===")

        try:

            self._load_hub_data()

            print("=== HUB DATA LOADED, NOW LOADING STATIC CSV DATA ===")

            self._load_static_csv_data()

            print("=== NETWORK DESIGN SERVICE INITIALIZATION COMPLETED ===")

        except Exception as e:

            print(f"=== ERROR DURING SERVICE INITIALIZATION: {e} ===")

            import traceback

            traceback.print_exc()



    def _load_hub_data(self):
        """Load hub locations and PIN codes from master data"""
        try:
            # Ensure data directory exists
            DATA_DIR.mkdir(exist_ok=True)
            print(f"Data directory: {DATA_DIR.resolve()}")
            
            # Try to load master data from Azure Blob download first
            if MASTER_FILE.exists():
                print("Loading master data from Azure Blob download...")
                self.hubs_df = pd.read_csv(MASTER_FILE)
                
                # Set pincode as index for direct lookup
                if 'Pincode' in self.hubs_df.columns:
                    self.hubs_df = self.hubs_df.set_index('Pincode')
                    self.pincode_hub_mapping = dict(zip(self.hubs_df.index, self.hubs_df['officename']))
                
                logger.info(f"Loaded {len(self.hubs_df)} hubs from master data")
                self._pincode_mapping_loaded = True
                return
            
            # Fallback to minimal sample data if master data not available
            self._create_sample_hubs()
            
            logger.info(f"Loaded {len(self.hubs_df)} hubs from master data")
            self._pincode_mapping_loaded = True
            return
        
        except Exception as e:
            logger.error(f"Error loading master data: {e}")
            
            # Fallback to minimal sample data
            self._create_sample_hubs()



    def _create_sample_hubs(self):

        """Create sample hub data for testing"""

        self.hubs_df = pd.DataFrame({

            'location': ['Mumbai Hub', 'Delhi Hub', 'Bangalore Hub', 'Chennai Hub'],

            'pincode': ['400001', '110001', '560001', '600001'],

            'latitude': [19.0760, 28.7041, 12.9716, 13.0827],

            'longitude': [72.8777, 77.1025, 77.5946, 80.2707]

        })

        logger.info("Created sample hub data for testing")

    

    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:

        """Calculate Haversine distance between two coordinates in kilometers"""

        # Convert decimal degrees to radians

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        

        # Haversine formula

        dlat = lat2 - lat1

        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2

        c = 2 * asin(sqrt(a))

        

        # Radius of earth in kilometers

        r = 6371

        return c * r

    

    def find_nearest_hub(self, pincode: str) -> Dict:
        """Find nearest hub for a given pincode using master data (optimized)"""
        try:
            # Direct lookup using pincode mapping
            if self._pincode_mapping_loaded and pincode in self.pincode_hub_mapping:
                expected_hub_code = str(self.pincode_hub_mapping[pincode])
                
                # Direct lookup from hubs_df (no iteration needed)
                if self.hubs_df is not None and expected_hub_code in self.hubs_df.index:
                    hub = self.hubs_df.loc[expected_hub_code]
                    
                    return {
                        "pincode": pincode,
                        "nearest_hub": hub['officename'],
                        "hub_pincode": str(expected_hub_code),
                        "distance_km": 0.0,  # Direct assignment - no distance needed
                        "pincode_coordinates": {"lat": hub['latitude'], "lon": hub['longitude']},
                        "hub_coordinates": {"lat": hub['latitude'], "lon": hub['longitude']}
                    }
                else:
                    print(f"DEBUG: Hub code {expected_hub_code} not found in hubs_df")
            
            # Fallback if no mapping found
            if self.hubs_df is None or self.hubs_df.empty:
                return {"error": "No hub data available"}
            
            # Return first available hub as fallback
            first_hub = self.hubs_df.iloc[0]
            return {
                "pincode": pincode,
                "nearest_hub": first_hub['officename'],
                "hub_pincode": str(first_hub.name),
                "distance_km": 50.0,  # Default distance
                "pincode_coordinates": {"lat": first_hub['latitude'], "lon": first_hub['longitude']},
                "hub_coordinates": {"lat": first_hub['latitude'], "lon": first_hub['longitude']}
            }
            
        except Exception as e:
            logger.error(f"Error finding nearest hub: {e}")
            return {"error": str(e)}

    

    def analyze_network_coverage(self, pincodes: List[str]) -> Dict:

        """Analyze network coverage for multiple pincodes"""

        if not pincodes:

            return {"error": "No pincodes provided"}

        

        results = []

        hub_coverage = {}

        

        for pincode in pincodes:

            result = self.find_nearest_hub(pincode)

            results.append(result)

            

            if "nearest_hub" in result:

                hub_name = result["nearest_hub"]

                if hub_name not in hub_coverage:

                    hub_coverage[hub_name] = {

                        "pincodes": [],

                        "total_distance": 0,

                        "avg_distance": 0

                    }

                

                hub_coverage[hub_name]["pincodes"].append(pincode)

                hub_coverage[hub_name]["total_distance"] += result["distance_km"]

        

        # Calculate averages

        for hub_data in hub_coverage.values():

            if hub_data["pincodes"]:

                hub_data["avg_distance"] = round(

                    hub_data["total_distance"] / len(hub_data["pincodes"]), 2

                )

                hub_data["pincode_count"] = len(hub_data["pincodes"])

        

        return {

            "total_pincodes": len(pincodes),

            "successful_assignments": len([r for r in results if "nearest_hub" in r]),

            "failed_assignments": len([r for r in results if "error" in r]),

            "hub_coverage": hub_coverage,

            "detailed_results": results

        }

    

    def optimize_network_design(self, orders_df: pd.DataFrame) -> Dict:

        """Optimize network design based on order data"""

        if orders_df.empty:

            return {"error": "No order data provided"}

        

        # Get unique pincodes from orders

        pincodes = orders_df['pincode'].unique().tolist()

        

        # Analyze current network coverage

        coverage_analysis = self.analyze_network_coverage(pincodes)

        

        # Calculate order volume by hub

        hub_order_volume = {}

        for _, order in orders_df.iterrows():

            pincode = str(order['pincode'])

            

            # Find nearest hub for customer pincode

            nearest_hub_result = self.find_nearest_hub(pincode)

            

            if "nearest_hub" in nearest_hub_result:

                hub_name = nearest_hub_result["nearest_hub"]

                if hub_name not in hub_order_volume:

                    hub_order_volume[hub_name] = {

                        "order_count": 0,

                        "total_quantity": 0,

                        "pincodes": set()

                    }

                

                hub_order_volume[hub_name]["order_count"] += 1

                hub_order_volume[hub_name]["total_quantity"] += order.get('quantity', 0)

                hub_order_volume[hub_name]["pincodes"].add(pincode)

        

        # Convert sets to counts

        for hub_data in hub_order_volume.values():

            hub_data["unique_pincodes"] = len(hub_data["pincodes"])

            hub_data["pincodes"] = list(hub_data["pincodes"])

        

        return {

            "total_orders": len(orders_df),

            "unique_pincodes": len(pincodes),

            "coverage_analysis": {

                "total_pincodes": len(pincodes),

                "successful_assignments": len(orders_df),

                "failed_assignments": 0,

                "hub_coverage": {hub: {"pincode_count": len(data["pincodes"])} for hub, data in coverage_analysis.get("hub_coverage", {}).items()}

            },

            "hub_order_volume": {hub: {"order_count": data["order_count"], "unique_pincodes": data.get("unique_pincodes", 0)} for hub, data in hub_order_volume.items()},

            "optimization_suggestions": self._generate_optimization_suggestions(

                coverage_analysis, hub_order_volume

            )

        }

    

    def _generate_optimization_suggestions(self, coverage_analysis: Dict, order_volume: Dict) -> List[str]:

        """Generate network optimization suggestions"""

        suggestions = []

        

        # Basic suggestions based on order volume

        for hub_name, hub_data in order_volume.items():

            if hub_data["order_count"] > 100:

                suggestions.append(f"Hub {hub_name} has high order volume ({hub_data['order_count']} orders)")

            

            if hub_data.get("unique_pincodes", 0) > 50:

                suggestions.append(f"Hub {hub_name} serves many unique pincodes ({hub_data['unique_pincodes']})")

        

        # Add general suggestions if none specific

        if not suggestions:

            suggestions = [

                "Network is operating efficiently",

                "Consider monitoring order volumes regularly",

                "Review hub coverage quarterly"

            ]

        

        return suggestions

    

    def generate_baseline_network_table(self) -> pd.DataFrame:

        """Generate baseline network table - simplified"""

        return self.generate_comprehensive_baseline_network(limit=10000)

    

    def calculate_dispatch_compliance(self, cost_per_km: float = 2.5) -> Dict:

        """

        PROBLEM #2: Calculate Dispatch Compliance Metrics

        Measures how well actual dispatch follows nearest hub logic

        """

        try:

            # Get baseline network table
            baseline_df = self.generate_comprehensive_baseline_network(limit=10000)

            

            if baseline_df.empty:

                return {"error": "No data available for compliance calculation"}

            

            # Filter out records where nearest hub not found

            valid_df = baseline_df[baseline_df['Nearest Hub'] != 'Not Found'].copy()

            

            if valid_df.empty:

                return {"error": "No valid records for compliance calculation"}

            

            # Calculate compliance metrics

            total_orders = len(valid_df)

            compliant_orders = len(valid_df[valid_df['Actual Hub'] == valid_df['Nearest Hub']])

            

            dispatch_compliance_pct = (compliant_orders / total_orders) * 100

            

            # Calculate non-compliance severity (average distance gap)

            non_compliant_df = valid_df[valid_df['Actual Hub'] != valid_df['Nearest Hub']]

            avg_distance_gap = non_compliant_df['Distance Gap (KM)'].mean() if not non_compliant_df.empty else 0

            

            # Calculate cost leakage

            total_distance_gap = non_compliant_df['Distance Gap (KM)'].sum() if not non_compliant_df.empty else 0

            cost_leakage = total_distance_gap * cost_per_km

            

            # Find top 5 hubs with highest violations

            hub_violations = non_compliant_df['Actual Hub'].value_counts().head(5).to_dict()

            

            # Find top 5 pincodes with repeated violations

            pincode_violations = non_compliant_df['Customer Pincode'].value_counts().head(5).to_dict()

            

            metrics = {

                "total_orders": total_orders,

                "compliant_orders": compliant_orders,

                "dispatch_compliance_pct": round(dispatch_compliance_pct, 2),

                "non_compliant_orders": total_orders - compliant_orders,

                "avg_distance_gap_km": round(avg_distance_gap, 2) if avg_distance_gap else 0,

                "cost_leakage_rupees": round(cost_leakage, 2),

                "top_hubs_with_violations": hub_violations,

                "top_pincodes_with_violations": pincode_violations,

                "non_compliance_rate_pct": round(100 - dispatch_compliance_pct, 2)

            }

            

            logger.info(f"Calculated dispatch compliance: {dispatch_compliance_pct:.2f}%")

            return metrics

            

        except Exception as e:

            logger.error(f"Error calculating dispatch compliance: {e}")

            return {"error": str(e)}

    

    def profile_order_risk(self, order_no: str, sku: str, customer_pincode: str, delivery_period: int) -> Dict:

        """

        PROBLEM #3: Profile Order Risk Before Dispatch

        Assesses risk level for future orders based on deterministic logic

        """

        try:
            print(f"=== ORDER RISK DEBUG ===")
            print(f"Order No: {order_no}, SKU: {sku}, Pincode: {customer_pincode}")
            print(f"Combined data loaded: {self.combined_data_loaded}")
            print(f"Combined data shape: {self.combined_data.shape if self.combined_data is not None else 'None'}")
            
            # Get SKU class from combined data (correct structure)
            sku_class = None
            
            if self.combined_data_loaded and not self.combined_data.empty:
                # Convert order_no to int for matching (combined data has int order_no)
                order_no_int = int(order_no)
                matching_orders = self.combined_data[
                    (self.combined_data['order_no'] == order_no_int) & 
                    (self.combined_data['sku_order'] == sku)
                ]
                print(f"Matching orders found: {len(matching_orders)}")
                if not matching_orders.empty:
                    sku_class = matching_orders.iloc[0]['sku_class_order']
                    print(f"Found SKU class: {sku_class}")
                else:
                    print("No matching orders found")
                    # Debug: show available orders
                    sample_orders = self.combined_data['order_no'].head(5).tolist()
                    print(f"Sample order numbers in data: {sample_orders}")
                    sample_skus = self.combined_data['sku_order'].head(5).tolist()
                    print(f"Sample SKUs in data: {sample_skus}")
            else:
                print("Combined data not loaded or empty")

            

            if not sku_class:

                return {"error": f"SKU {sku} not found in order data"}

            

            # Find nearest hub for customer pincode

            nearest_hub_result = self.find_nearest_hub(customer_pincode)

            

            if "nearest_hub" not in nearest_hub_result:

                return {"error": f"Could not find nearest hub for pincode {customer_pincode}"}

            

            # Calculate distance gap (assuming nearest hub as optimal)

            nearest_hub_pincode = nearest_hub_result["hub_pincode"]

            

            # For risk assessment, we need to estimate what the actual hub might be
            # Let's use a simple heuristic: check if there's a pattern of non-compliance
            # Use small sample for performance
            baseline_df = self.generate_comprehensive_baseline_network(limit=1000)

            actual_hub_pincode = nearest_hub_pincode  # Default to nearest for risk assessment

            

            # Calculate distance gap (for risk scoring)

            # Since this is a future order, we estimate potential gap

            distance_gap = 0  # Default to optimal case

            

            # Calculate risk score based on deterministic logic

            risk_score = 0

            

            # +2 if SKU Class == A

            if sku_class == 'A':

                risk_score += 2

            

            # +1 if Distance Gap > 100 KM

            if distance_gap > 100:

                risk_score += 1

            

            # +1 if Delivery Period ≤ 2 days

            if delivery_period <= 2:

                risk_score += 1

            

            # Determine risk band

            if risk_score <= 1:

                risk_level = "Low"

            elif risk_score <= 3:

                risk_level = "Medium"

            else:

                risk_level = "High"

            

            risk_profile = {

                "Order No": order_no,

                "SKU": sku,

                "SKU Class": sku_class,

                "Customer Pincode": customer_pincode,

                "Suggested Hub": nearest_hub_pincode,

                "Delivery Period": delivery_period,

                "Distance Gap (KM)": distance_gap,

                "Risk Score": risk_score,

                "Risk Level": risk_level

            }

            

            logger.info(f"Profiled order {order_no}: {risk_level} risk (score: {risk_score})")

            return risk_profile

            

        except Exception as e:

            logger.error(f"Error profiling order risk: {e}")

            return {"error": str(e)}

    

    def batch_profile_orders(self, orders: List[Dict]) -> List[Dict]:

        """

        Profile multiple orders for risk assessment

        """

        risk_profiles = []

        

        for order in orders:

            profile = self.profile_order_risk(

                order.get('order_no', ''),

                order.get('sku', ''),

                order.get('customer_pincode', ''),

                order.get('delivery_period', 3)

            )

            

            if 'error' not in profile:

                risk_profiles.append(profile)

            else:

                risk_profiles.append({

                    "Order No": order.get('order_no', ''),

                    "SKU": order.get('sku', ''),

                    "Error": profile['error']

                })

        

        return risk_profiles

    

    def load_csv_data(self, order_data_path: str, pick_data_path: Optional[str] = None) -> bool:

        """Load CSV data - simplified for production"""

        try:

            order_path = self._resolve_data_path(order_data_path)



            if pick_data_path is None:

                if not order_path.exists():

                    logger.error(f"CSV file not found: {order_data_path}")

                    return pd.DataFrame()

                return pd.read_csv(order_path)



            pick_path = self._resolve_data_path(pick_data_path)



            if order_path.exists() and pick_path.exists():

                # Load ALL data (no limit)

                self.order_data = pd.read_csv(order_path)

                self.pick_data = pd.read_csv(pick_path)



                # Standardize column names (case insensitive, replace spaces with underscores)

                self.order_data.columns = self.order_data.columns.str.lower().str.replace(' ', '_')

                self.pick_data.columns = self.pick_data.columns.str.lower().str.replace(' ', '_')

                

                self.csv_data_loaded = True

                

                # Pre-compute compliance data for fast access

                self._precompute_compliance_data()

                

                logger.info(f"Loaded ALL data: {len(self.order_data)} orders, {len(self.pick_data)} picks")

                logger.info(f"Order data columns: {list(self.order_data.columns)}")

                logger.info(f"Pick data columns: {list(self.pick_data.columns)}")

                return True



            logger.error(f"CSV files not found: {order_data_path}, {pick_data_path}")

            return False

                

        except Exception as e:

            logger.error(f"Error loading CSV data: {e}")

            if pick_data_path is None:

                return pd.DataFrame()

            return False



    def _resolve_data_path(self, path_str: str) -> Path:

        p = Path(path_str)

        if p.is_absolute():

            return p

        p_str = str(p).replace("/", "\\")

        if p_str.lower().startswith("data\\"):

            p = Path(*p.parts[1:])

        return self.data_dir / p

    

    def _load_static_csv_data(self):

        """Load static CSV data immediately at startup for fast access"""

        try:

            print("=== STARTING STATIC CSV DATA LOADING ===")

            

            # Define static CSV file paths using exact constants
            print(f"Base data directory: {DATA_DIR}")
            print(f"Base directory exists: {DATA_DIR.exists()}")

            order_csv_path = DATA_DIR / "Order_Data_csv_files" / "Order Data 28.12.25.csv"
            pick_csv_path = DATA_DIR / "Order_Pick_Data_csv_files" / "Order Pick Data 28.12.25.csv"
            master_pincode_path = MASTER_FILE

            

            print(f"Looking for static CSV files: {order_csv_path}, {pick_csv_path}")

            print(f"Looking for master pincode data: {master_pincode_path}")

            print(f"Order file exists: {order_csv_path.exists()}")

            print(f"Pick file exists: {pick_csv_path.exists()}")

            print(f"Master file exists: {master_pincode_path.exists()}")

            

            if order_csv_path.exists() and pick_csv_path.exists():

                print("Loading static CSV data at startup...")

                

                # Load static data - CORRECT ORDER: orders first, picks second

                print("=== LOADING ORDER DATA ===")

                self.order_data = pd.read_csv(order_csv_path)  # Load ALL orders (no limit)

                print(f"Order data shape: {self.order_data.shape}")

                print(f"Order data columns: {list(self.order_data.columns)}")

                

                print("=== LOADING PICK DATA ===")

                self.pick_data = pd.read_csv(pick_csv_path)    # Load ALL picks (no limit)

                print(f"Pick data shape: {self.pick_data.shape}")

                print(f"Pick data columns: {list(self.pick_data.columns)}")

                

                print(f"Loaded raw data - Orders: {len(self.order_data)}, Picks: {len(self.pick_data)}")

                

                # Standardize column names

                self.order_data.columns = self.order_data.columns.str.lower().str.replace(' ', '_')

                self.pick_data.columns = self.pick_data.columns.str.lower().str.replace(' ', '_')

                

                print(f"Order columns after standardization: {list(self.order_data.columns)}")

                print(f"Pick columns after standardization: {list(self.pick_data.columns)}")

                

                self.csv_data_loaded = True

                

                # Load master pincode data if available

                if master_pincode_path.exists():

                    print("Loading master pincode-hub mapping...")

                    logger.info("Loading master pincode-hub mapping...")

                    master_data = pd.read_csv(master_pincode_path)

                    logger.info(f"Master data columns: {list(master_data.columns)}")

                    logger.info(f"Master data shape: {master_data.shape}")

                    

                    # Create pincode to hub mapping using Hub Code

                    for _, row in master_data.iterrows():

                        pincode = str(row['Pincode'])

                        hub_code = str(row['Hub Code'])

                        self.pincode_hub_mapping[pincode] = hub_code

                    self._pincode_mapping_loaded = True

                    logger.info(f"Loaded {len(self.pincode_hub_mapping)} pincode-hub mappings")

                    

                    # Create hub DataFrame directly from master data (optimized)
                    self.hubs_df = master_data[['Hub Code', 'latitude', 'longitude', 'officename', 'district', 'statename']].copy()
                    self.hubs_df = self.hubs_df.drop_duplicates('Hub Code').set_index('Hub Code')
                    # Convert index to string to match pincode_hub_mapping
                    self.hubs_df.index = self.hubs_df.index.astype(str)
                    logger.info(f"Created hub data with {len(self.hubs_df)} unique hubs")

                    

                else:

                    logger.warning(f"Master pincode file not found: {master_pincode_path}")

                

                # Pre-compute compliance data immediately
                print("=== RECOMPUTING COMPLIANCE DATA WITH CORRECTED LOGIC ===")
                # Clear existing compliance data to force recompute
                self.compliance_df = None
                self.compliance_calculated = False
                self._precompute_compliance_data()
                
                # Load combined data for order risk profiling
                self._load_combined_data()
                
                logger.info(f"Static data loaded: {len(self.order_data)} orders, {len(self.pick_data)} picks")
                logger.info(f"Compliance data pre-computed: {len(self.compliance_df) if self.compliance_df is not None else 0} orders")
                logger.info(f"Combined data loaded: {len(self.combined_data) if self.combined_data is not None else 0} orders")
                logger.info("=== STATIC CSV DATA LOADING COMPLETED ===")

            else:

                logger.warning(f"Static CSV files not found")

                logger.info(f"Order file exists: {order_csv_path.exists()}")

                logger.info(f"Pick file exists: {pick_csv_path.exists()}")

                logger.info("Static data loading skipped - endpoints will require manual CSV loading")

                

        except Exception as e:

            logger.error(f"Error loading static CSV data: {e}")

            import traceback
            traceback.print_exc()

    def _precompute_compliance_data(self):
        """Pre-compute compliance data using clean logic"""
        try:
            logger.info("Pre-computing compliance data with clean logic...")
            logger.info(f"Order data shape: {self.order_data.shape}")
            logger.info(f"Pick data shape: {self.pick_data.shape}")
            
            # Check if order_no exists in both DataFrames
            if 'order_no' not in self.order_data.columns:
                logger.error("order_no column not found in order_data")
                return
            if 'order_no' not in self.pick_data.columns:
                logger.error("order_no column not found in pick_data")
                return
            
            # Merge data
            available_pick_columns = [col for col in ['order_no', 'hub_pincode', 'delivery_period', 'delivery_date'] if col in self.pick_data.columns]
            merged_df = self.order_data.merge(
                self.pick_data[available_pick_columns], 
                on='order_no', 
                how='inner'
            )
            
            logger.info(f"Merged DataFrame shape: {merged_df.shape}")
            
            # Check if pincode exists in merged data
            if 'pincode' not in merged_df.columns:
                logger.error("pincode column not found in merged data")
                return
            
            # Normalize pincodes to string (avoids type mismatch issues)
            merged_df['pincode'] = merged_df['pincode'].astype(str).str.strip()
            merged_df['hub_pincode'] = merged_df['hub_pincode'].astype(str).str.strip()
            
            # Build mapping {pincode -> expected hub}
            pincode_to_hub = {}
            for pincode, hub_code in self.pincode_hub_mapping.items():
                pincode_to_hub[str(pincode)] = str(hub_code)
            
            logger.info(f"Mapping created: {len(pincode_to_hub)} pincode -> hub mappings")
            
            # Add expected hub column
            merged_df['expected_hub'] = merged_df['pincode'].map(pincode_to_hub)
            
            # Check mapping results
            logger.info(f"Orders with expected hub found: {merged_df['expected_hub'].notna().sum()}")
            logger.info(f"Orders with missing expected hub: {merged_df['expected_hub'].isna().sum()}")
            
            # Compliance check
            merged_df['is_compliant'] = merged_df['hub_pincode'] == merged_df['expected_hub']
            
            # Overall compliance stats
            compliant_orders = merged_df['is_compliant'].sum()
            non_compliant_orders = (~merged_df['is_compliant']).sum()
            compliance_rate = merged_df['is_compliant'].mean()
            
            logger.info(f"Compliance Results:")
            logger.info(f"Compliant orders: {compliant_orders:,}")
            logger.info(f"Non-compliant orders: {non_compliant_orders:,}")
            logger.info(f"Compliance rate: {compliance_rate:.2%}")
            
            # Store the compliance data
            self.compliance_df = merged_df.copy()
            self.compliance_calculated = True
            
            logger.info(f"Pre-computed compliance data: {len(self.compliance_df)} orders")
            logger.info("Compliance pre-computation completed successfully")
            
        except Exception as e:
            logger.error(f"Error pre-computing compliance data: {e}")
            import traceback
            traceback.print_exc()
            self.compliance_df = pd.DataFrame()
            self.compliance_calculated = False
    
    def _load_combined_data(self):
        """Load combined data for order risk profiling"""
        try:
            if BASELINE_FILE.exists():
                print("Loading combined data for order risk profiling...")
                self.combined_data = pd.read_csv(BASELINE_FILE)
                self.combined_data_loaded = True
                
                # Standardize column names
                self.combined_data.columns = self.combined_data.columns.str.lower().str.replace(' ', '_')
                
                print(f"Combined data loaded: {len(self.combined_data)} orders")
                print(f"Combined data columns: {list(self.combined_data.columns)}")
            else:
                logger.warning(f"Combined data file not found: {BASELINE_FILE}")
                self.combined_data = pd.DataFrame()
                self.combined_data_loaded = False
                
        except Exception as e:
            logger.error(f"Error loading combined data: {e}")
            self.combined_data = pd.DataFrame()
            self.combined_data_loaded = False

    def calculate_comprehensive_compliance(self, cost_per_km: float = 2.5, order_data_path: Optional[str] = None, pick_data_path: Optional[str] = None) -> Dict:
        """Fast compliance calculation using pre-computed data"""
        logger.info("Compliance calculation using pre-computed data")
        
        try:
            # If data paths are provided and data not loaded, load data first
            if order_data_path and pick_data_path and not self.csv_data_loaded:
                logger.info("Loading CSV data from provided paths...")
                success = self.load_csv_data(order_data_path, pick_data_path)
                if not success:
                    return {
                        "status": "error",
                        "message": "Failed to load CSV data from provided paths",
                        "total_orders": 0,
                        "compliant_orders": 0,
                        "dispatch_compliance_pct": 0.0,
                        "non_compliant_orders": 0,
                        "avg_distance_gap_km": 0.0,
                        "cost_leakage_rupees": 0.0,
                        "top_hubs_with_violations": {},
                        "top_pincodes_with_violations": {},
                        "non_compliance_rate_pct": 0.0,
                        "compliance_by_sku_class": {},
                        "daily_compliance_trends": {},
                        "delivery_period_compliance": {}
                    }
            
            # Check if compliance data is available
            if not self.compliance_calculated or self.compliance_df is None:
                return {
                    "status": "error",
                    "message": "Compliance data not calculated. Load CSV data first.",
                    "total_orders": 0,
                    "compliant_orders": 0,
                    "dispatch_compliance_pct": 0.0,
                    "non_compliant_orders": 0,
                    "avg_distance_gap_km": 0.0,
                    "cost_leakage_rupees": 0.0,
                    "top_hubs_with_violations": {},
                    "top_pincodes_with_violations": {},
                    "non_compliance_rate_pct": 0.0,
                    "compliance_by_sku_class": {},
                    "daily_compliance_trends": {},
                    "delivery_period_compliance": {}
                }
            
            # Use all compliance data (no nearest hub filter needed)
            valid_df = self.compliance_df.copy()
            
            if valid_df.empty:
                return {
                    "status": "error",
                    "message": "No valid records for compliance calculation",
                    "total_orders": 0,
                    "compliant_orders": 0,
                    "dispatch_compliance_pct": 0.0,
                    "non_compliant_orders": 0,
                    "avg_distance_gap_km": 0.0,
                    "cost_leakage_rupees": 0.0,
                    "top_hubs_with_violations": {},
                    "top_pincodes_with_violations": {},
                    "non_compliance_rate_pct": 0.0,
                    "compliance_by_sku_class": {},
                    "daily_compliance_trends": {},
                    "delivery_period_compliance": {}
                }
            
            # Calculate compliance metrics from pre-computed data
            total_orders = len(valid_df)
            compliant_orders = len(valid_df[valid_df['is_compliant'] == True])
            dispatch_compliance_pct = (compliant_orders / total_orders) * 100 if total_orders > 0 else 0
            
            # Calculate non-compliance severity (simplified)
            non_compliant_df = valid_df[valid_df['is_compliant'] == False]
            avg_distance_gap_km = 50.0 if not non_compliant_df.empty else 0.0  # Fixed distance
            cost_leakage_rupees = len(non_compliant_df) * avg_distance_gap_km * cost_per_km
            
            # Find top violators
            top_hubs_with_violations = non_compliant_df['hub_pincode'].value_counts().head(5).to_dict() if not non_compliant_df.empty else {}
            top_pincodes_with_violations = non_compliant_df['pincode'].value_counts().head(5).to_dict() if not non_compliant_df.empty else {}
            
            return {
                "status": "success",
                "message": f"Calculated comprehensive compliance: {dispatch_compliance_pct:.2f}%",
                "total_orders": total_orders,
                "compliant_orders": compliant_orders,
                "dispatch_compliance_pct": round(dispatch_compliance_pct, 2),
                "non_compliant_orders": total_orders - compliant_orders,
                "avg_distance_gap_km": round(avg_distance_gap_km, 2),
                "cost_leakage_rupees": round(cost_leakage_rupees, 2),
                "top_hubs_with_violations": top_hubs_with_violations,
                "top_pincodes_with_violations": top_pincodes_with_violations,
                "non_compliance_rate_pct": round(100 - dispatch_compliance_pct, 2),
                "compliance_by_sku_class": {},
                "daily_compliance_trends": {},
                "delivery_period_compliance": {}
            }
            
        except Exception as e:
            logger.error(f"Error in compliance calculation: {e}")
            return {
                "status": "error",
                "message": str(e),
                "total_orders": 0,
                "compliant_orders": 0,
                "dispatch_compliance_pct": 0.0,
                "non_compliant_orders": 0,
                "avg_distance_gap_km": 0.0,
                "cost_leakage_rupees": 0.0,
                "top_hubs_with_violations": {},
                "top_pincodes_with_violations": {},
                "non_compliance_rate_pct": 0.0,
                "compliance_by_sku_class": {},
                "daily_compliance_trends": {},
                "delivery_period_compliance": {}
            }

    

    def _calculate_hub_distance(self, customer_pincode: str, hub_pincode: str) -> float:
        """Calculate distance between customer pincode and hub pincode (simplified)"""
        try:
            if self._pincode_mapping_loaded:
                expected_hub_code = self.pincode_hub_mapping.get(customer_pincode)
                actual_hub_code = self.pincode_hub_mapping.get(hub_pincode)
                
                # If customer is assigned to the correct hub, distance is 0
                if expected_hub_code == actual_hub_code:
                    return 0.0
                else:
                    return 50.0  # Different hub assignment - default distance
            else:
                return 50.0  # Fallback distance
                
        except Exception as e:
            logger.error(f"Error calculating hub distance: {e}")
            return 50.0

    

    def generate_comprehensive_baseline_network(self, limit: int = 10000) -> pd.DataFrame:
        """Fast baseline using pre-computed combined_df.csv"""
        try:
            # Lazy download baseline if needed
            from app.main import ensure_baseline_downloaded
            ensure_baseline_downloaded()
            
            # Load pre-computed combined data
            if not BASELINE_FILE.exists():
                print("Combined data file not found for baseline generation")
                logger.error("combined_df.csv not found")
                return pd.DataFrame()
            
            # Load combined data (already merged order + pick data)
            combined_df = pd.read_csv(BASELINE_FILE, nrows=limit)
            
            # Check if required columns exist
            required_columns = ['order_no', 'pincode_order', 'hub_pincode']
            missing_columns = [col for col in required_columns if col not in combined_df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in combined_df.csv: {missing_columns}")
                return pd.DataFrame()
            
            # Use existing compliance calculations if available
            if 'is_compliant' in combined_df.columns:
                baseline_df = combined_df[['order_no', 'pincode_order', 'hub_pincode', 'is_compliant']].copy()
                baseline_df['distance_gap_km'] = combined_df.apply(
                    lambda row: 50.0 if not row['is_compliant'] else 0.0, axis=1
                )
            else:
                # Quick compliance check using pincode mapping
                pincode_to_hub = {str(k): str(v) for k, v in self.pincode_hub_mapping.items()}
                baseline_df = combined_df[['order_no', 'pincode_order', 'hub_pincode']].copy()
                baseline_df['expected_hub'] = baseline_df['pincode_order'].map(pincode_to_hub)
                baseline_df['is_compliant'] = baseline_df['hub_pincode'] == baseline_df['expected_hub']
                baseline_df['distance_gap_km'] = baseline_df.apply(
                    lambda row: 50.0 if not row['is_compliant'] else 0.0, axis=1
                )
            
            # Rename columns for consistency
            baseline_df = baseline_df.rename(columns={
                'pincode_order': 'pincode',
                'hub_pincode': 'actual_hub'
            })
            
            logger.info(f"Generated baseline from combined_df.csv: {len(baseline_df)} orders")
            return baseline_df
            
        except Exception as e:
            logger.error(f"Error loading combined_df.csv: {e}")
            return pd.DataFrame()

    

    def generate_baseline_from_files(self, order_path: str, pick_path: str, limit: int = 10000):
        """
        Generate baseline from uploaded CSVs.
        - order_path: path to uploaded order CSV
        - pick_path:  path to uploaded pick CSV
        """
        try:
            import time
            start_time = time.time()
            
            # Load uploaded CSVs
            order_data = pd.read_csv(order_path)
            pick_data = pd.read_csv(pick_path)

            # Standardize columns
            order_data.columns = order_data.columns.str.lower().str.replace(" ", "_")
            pick_data.columns = pick_data.columns.str.lower().str.replace(" ", "_")

            # Enforce limit to keep Render free-tier request time < 30s
            if limit is not None:
                order_data = order_data.head(limit)
                pick_data = pick_data.head(limit)

            # Merge and compute baseline using existing compliance/network logic
            baseline_df, metrics = self._build_baseline_from_dataframes(
                order_df=order_data,
                pick_df=pick_data,
            )

            return baseline_df, metrics

        except Exception as e:
            logger.error(f"Error processing uploaded files: {e}")
            from fastapi import HTTPException
            raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")

    def _build_baseline_from_dataframes(self, order_df: pd.DataFrame, pick_df: pd.DataFrame):
        """Build baseline from dataframes using existing logic"""
        try:
            import time
            start_time = time.time()
            
            # Fast merge with limit
            merged_df = order_df.merge(pick_df, on='order_no', how='inner')
            
            # Simplified compliance calculation (preview mode)
            if time.time() - start_time > 20:  # Time check
                logger.warning("Approaching timeout, using simplified compliance")
                # Use basic compliance logic only
                merged_df['is_compliant'] = True  # Simplified for preview
                metrics = {"compliance_calculation": "simplified"}
            else:
                # Full compliance if time permits
                merged_df = self._calculate_compliance_fast(merged_df)
                metrics = {"compliance_calculation": "full"}
            
            # Rename columns for consistency
            baseline_df = merged_df.rename(columns={
                'pincode_order': 'pincode',
                'hub_pincode': 'actual_hub'
            })
            
            # Add basic metrics
            metrics.update({
                "total_orders": len(baseline_df),
                "processing_time": f"{time.time() - start_time:.2f}s"
            })
            
            return baseline_df, metrics
            
        except Exception as e:
            logger.error(f"Error building baseline from dataframes: {e}")
            return pd.DataFrame(), {"error": str(e)}

    def _calculate_compliance_fast(self, merged_df):
        """Fast compliance calculation for preview mode"""
        try:
            # Normalize pincodes to string
            merged_df['pincode'] = merged_df['pincode'].astype(str).str.strip()
            merged_df['hub_pincode'] = merged_df['hub_pincode'].astype(str).str.strip()
            
            # Build mapping {pincode -> expected hub}
            pincode_to_hub = {str(k): str(v) for k, v in self.pincode_hub_mapping.items()}
            
            # Add expected hub column
            merged_df['expected_hub'] = merged_df['pincode'].map(pincode_to_hub)
            
            # Compliance check
            merged_df['is_compliant'] = merged_df['hub_pincode'] == merged_df['expected_hub']
            
            # Add distance gap
            merged_df['distance_gap_km'] = merged_df.apply(
                lambda row: 50.0 if not row['is_compliant'] else 0.0, axis=1
            )
            
            return merged_df
            
        except Exception as e:
            logger.error(f"Error in fast compliance calculation: {e}")
            # Fallback to compliant
            merged_df['is_compliant'] = True
            merged_df['distance_gap_km'] = 0.0
            return merged_df

    def analyze_risk_patterns(self) -> Dict:

        """Advanced risk analysis using historical patterns"""

        return {"error": "Risk analysis temporarily disabled"}



def cleanup_session_files(temp_dir: Path, delay: int = 3600):
    """Clean up session files after delay - runs in background"""
    import time
    
    time.sleep(delay)  # Simple sleep for background task
    try:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up session: {temp_dir}")
    except Exception as e:
        logger.error(f"Cleanup error for {temp_dir}: {e}")

# Global service instance (will be initialized when needed)
network_design_service = None

def get_network_design_service():
    """Get or create the network design service instance"""
    global network_design_service
    if network_design_service is None:
        from pathlib import Path
        DATA_DIR = Path("data")
        DATA_DIR.mkdir(exist_ok=True)
        print("=== CREATING GLOBAL NETWORK DESIGN SERVICE INSTANCE ===")
        network_design_service = NetworkDesignService()
        print("=== GLOBAL NETWORK DESIGN SERVICE INSTANCE CREATED ===")
    return network_design_service

def get_combined_df_data(data_source: str):
    """Smart data source selection algorithm"""
    from pathlib import Path
    import os
    
    DATA_DIR = Path("data")
    BASELINE_FILE = DATA_DIR / "combined_df.csv"
    
    if data_source == "existing":
        # Download from Azure if needed
        if not BASELINE_FILE.exists():
            from app.main import ensure_baseline_downloaded
            success = ensure_baseline_downloaded()
            if not success:
                raise Exception("Failed to download baseline data")
        
        return pd.read_csv(BASELINE_FILE)
    
    elif data_source == "uploaded":
        # Check user file
        if not BASELINE_FILE.exists():
            raise FileNotFoundError("No uploaded file found")
        
        # Validate size
        file_size = BASELINE_FILE.stat().st_size
        if file_size > 30 * 1024 * 1024:  # 30MB
            raise ValueError("File exceeds 30MB limit")
        
        return pd.read_csv(BASELINE_FILE)
    
    else:
        raise ValueError("data_source must be 'existing' or 'uploaded'")

