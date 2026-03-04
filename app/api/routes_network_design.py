from fastapi import APIRouter, HTTPException, BackgroundTasks, File, UploadFile, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
import tempfile
import shutil
import re
import sys
import os

import logging

from app.services.network_design_service import get_network_design_service
from app.models.network_models import (
    NearestHubRequest,
    NearestHubResponse,
    NetworkCoverageRequest,
    NetworkCoverageResponse,
    NetworkOptimizationRequest,
    NetworkOptimizationResponse,
    DispatchAnalysisRequest,
    DispatchAnalysisResponse,
    ComprehensiveComplianceRequest,
    ComprehensiveComplianceResponse
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(tags=["network-design"])

@router.post("/nearest-hub", response_model=NearestHubResponse)
def find_nearest_hub(request: NearestHubRequest):
    """
    Find the nearest hub for a given PIN code with real distance calculation.
    
    This endpoint:
    - Geocodes the input PIN code to coordinates
    - Calculates distance to all available hubs using Haversine formula
    - Returns the nearest hub with distance information
    """
    try:
        result = get_network_design_service().find_nearest_hub(request.pincode)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return NearestHubResponse(**result)
        
    except Exception as e:
        logger.error(f"Error finding nearest hub for {request.pincode}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/network-coverage", response_model=NetworkCoverageResponse)
def analyze_network_coverage(request: NetworkCoverageRequest):
    """
    Analyze network coverage for multiple PIN codes.
    
    This endpoint:
    - Processes multiple PIN codes
    - Assigns each to the nearest hub
    - Provides coverage statistics by hub
    - Calculates average distances and service areas
    """
    try:
        if not request.pincodes:
            raise HTTPException(status_code=400, detail="No PIN codes provided")
        
        result = get_network_design_service().analyze_network_coverage(request.pincodes)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return NetworkCoverageResponse(**result)
        
    except Exception as e:
        logger.error(f"Error analyzing network coverage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/network-optimization", response_model=NetworkOptimizationResponse)
def optimize_network_design(
    request: NetworkOptimizationRequest,
    background_tasks: BackgroundTasks
):
    """
    Optimize network design based on order data analysis.
    
    This endpoint:
    - Loads order data from Excel files
    - Analyzes current network performance
    - Identifies optimization opportunities
    - Provides actionable recommendations
    """
    try:
        # Handle data loading based on request
        if request.use_existing_data and get_network_design_service().csv_data_loaded:
            orders_df = get_network_design_service().order_data
        else:
            # Load fresh data
            orders_df = get_network_design_service().load_csv_data("Order Data 28.12.25.csv")
        
        if orders_df.empty:
            raise HTTPException(
                status_code=404, 
                detail="No order data available. Please ensure CSV files are in the data directory."
            )
        
        # Apply filters if specified
        if request.limit_orders:
            orders_df = orders_df.head(request.limit_orders)
        
        if request.sku_filter:
            orders_df = orders_df[orders_df['sku'].isin(request.sku_filter)]
        
        if request.pincode_filter:
            orders_df = orders_df[orders_df['pincode'].isin(request.pincode_filter)]
        
        # Run network optimization
        result = get_network_design_service().optimize_network_design(orders_df)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return NetworkOptimizationResponse(**result)
        
    except Exception as e:
        logger.error(f"Error optimizing network design: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/dispatch-analysis", response_model=DispatchAnalysisResponse)
def analyze_dispatch_data(request: DispatchAnalysisRequest):
    """
    Analyze dispatch data from order pick information.
    
    This endpoint:
    - Loads order pick data
    - Applies date and SKU/PIN code filters
    - Analyzes dispatch patterns and trends
    - Provides performance metrics
    """
    try:
        # Load order data
        orders_df = get_network_design_service().load_csv_data("Order_Data_csv_files/Order Pick Data 28.12.25.csv")
        
        if orders_df.empty:
            raise HTTPException(
                status_code=404,
                detail="No order data available for dispatch analysis"
            )
        
        # Apply date filters
        if 'order_date' in orders_df.columns:
            if request.start_date:
                orders_df = orders_df[orders_df['order_date'] >= request.start_date]
            
            if request.end_date:
                orders_df = orders_df[orders_df['order_date'] <= request.end_date]
        
        # Apply SKU filter
        if request.sku_filter:
            orders_df = orders_df[orders_df['sku'].isin(request.sku_filter)]
        
        # Apply PIN code filter
        if request.pincode_filter:
            orders_df = orders_df[orders_df['pincode'].isin(request.pincode_filter)]
        
        # Generate analysis
        analysis = _generate_dispatch_analysis(orders_df)
        
        return DispatchAnalysisResponse(**analysis)
        
    except Exception as e:
        logger.error(f"Error in dispatch analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/network-status")
def get_network_status():
    """
    Get current network design service status.
    
    Returns information about:
    - Available hubs
    - Data sources
    - Service health
    """
    try:
        # Get hub information
        hubs_info = {}
        if get_network_design_service().hubs_df is not None:
            hubs_info = {
                "total_hubs": len(get_network_design_service().hubs_df),
                "hub_locations": get_network_design_service().hubs_df['officename'].tolist() if 'officename' in get_network_design_service().hubs_df.columns else [],
                "pincode_mapping_size": len(get_network_design_service().pincode_hub_mapping)
            }
        
        # Check data availability using existing flags (no file loading)
        data_status = {
            "order_data_available": get_network_design_service().csv_data_loaded,
            "return_data_available": False,  # No return data loaded in current implementation
            "master_data_available": get_network_design_service()._pincode_mapping_loaded
        }
        
        return {
            "status": "healthy",
            "hubs_info": hubs_info,
            "data_status": data_status,
            "services": {
                "network_design": "active",
                "csv_processing": "active",
                "dispatch_analysis": "active"
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting network status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _generate_dispatch_analysis(orders_df: pd.DataFrame) -> Dict[str, Any]:
    """Generate dispatch analysis from order data"""
    analysis = {
        "total_orders": len(orders_df),
        "date_range": {},
        "top_skus": [],
        "top_pincodes": [],
        "hub_performance": {},
        "daily_trends": []
    }
    
    if orders_df.empty:
        return analysis
    
    # Date range
    if 'order_date' in orders_df.columns:
        analysis["date_range"] = {
            "start": orders_df['order_date'].min().strftime('%Y-%m-%d'),
            "end": orders_df['order_date'].max().strftime('%Y-%m-%d')
        }
        
        # Daily trends
        daily_orders = orders_df.groupby(orders_df['order_date'].dt.date).size().reset_index(name='order_count')
        analysis["daily_trends"] = [
            {"date": str(row['order_date']), "orders": row['order_count']}
            for _, row in daily_orders.tail(30).iterrows()  # Last 30 days
        ]
    
    # Top SKUs
    if 'sku' in orders_df.columns:
        top_skus = orders_df['sku'].value_counts().head(10)
        analysis["top_skus"] = [
            {"sku": sku, "order_count": count}
            for sku, count in top_skus.items()
        ]
    
    # Top PIN codes
    if 'pincode' in orders_df.columns:
        top_pincodes = orders_df['pincode'].value_counts().head(10)
        analysis["top_pincodes"] = [
            {"pincode": pincode, "order_count": count}
            for pincode, count in top_pincodes.items()
        ]
    
    # Hub performance (if we can assign hubs)
    if 'pincode' in orders_df.columns:
        hub_assignments = {}
        for pincode in orders_df['pincode'].unique():
            nearest_hub = get_network_design_service().find_nearest_hub(str(pincode))
            if "nearest_hub" in nearest_hub:
                hub_name = nearest_hub["nearest_hub"]
                if hub_name not in hub_assignments:
                    hub_assignments[hub_name] = 0
                hub_assignments[hub_name] += 1
        
        analysis["hub_performance"] = hub_assignments
    
    return analysis

# New Request/Response Models for Dispatch Intelligence
class DispatchComplianceRequest(BaseModel):
    """Request for dispatch compliance analysis"""
    cost_per_km: float = 2.5

class DispatchComplianceResponse(BaseModel):
    """Response containing dispatch compliance metrics"""
    status: str
    message: str
    total_orders: int
    compliant_orders: int
    dispatch_compliance_pct: float
    non_compliant_orders: int
    avg_distance_gap_km: float
    cost_leakage_rupees: float
    top_hubs_with_violations: Dict[str, int]
    top_pincodes_with_violations: Dict[str, int]
    non_compliance_rate_pct: float
    error: Optional[str] = None

class OrderRiskRequest(BaseModel):
    """Request for single order risk profiling"""
    order_no: str
    sku: str
    customer_pincode: str
    delivery_period: int

class OrderRiskResponse(BaseModel):
    """Response containing order risk profile"""
    status: str
    message: str
    risk_profile: Dict[str, Any]
    error: Optional[str] = None

# PROBLEM #2: Dispatch Compliance Endpoint
@router.post("/dispatch-compliance", response_model=DispatchComplianceResponse)
def calculate_dispatch_compliance(request: DispatchComplianceRequest):
    """
    PROBLEM #2: Calculate Dispatch Compliance Metrics
    
    This endpoint:
    - Measures how well actual dispatch follows nearest hub logic
    - Calculates compliance percentage and severity metrics
    - Identifies top violators by hub and pincode
    """
    try:
        metrics = get_network_design_service().calculate_dispatch_compliance(request.cost_per_km)
        
        if "error" in metrics:
            return DispatchComplianceResponse(
                status="error",
                message="Failed to calculate dispatch compliance",
                total_orders=0,
                compliant_orders=0,
                dispatch_compliance_pct=0.0,
                non_compliant_orders=0,
                avg_distance_gap_km=0.0,
                cost_leakage_rupees=0.0,
                top_hubs_with_violations={},
                top_pincodes_with_violations={},
                non_compliance_rate_pct=0.0,
                error=metrics["error"]
            )
        
        return DispatchComplianceResponse(
            status="success",
            message=f"Calculated dispatch compliance: {metrics['dispatch_compliance_pct']}%",
            total_orders=metrics["total_orders"],
            compliant_orders=metrics["compliant_orders"],
            dispatch_compliance_pct=metrics["dispatch_compliance_pct"],
            non_compliant_orders=metrics["non_compliant_orders"],
            avg_distance_gap_km=metrics["avg_distance_gap_km"],
            cost_leakage_rupees=metrics["cost_leakage_rupees"],
            top_hubs_with_violations=metrics["top_hubs_with_violations"],
            top_pincodes_with_violations=metrics["top_pincodes_with_violations"],
            non_compliance_rate_pct=metrics["non_compliance_rate_pct"]
        )
        
    except Exception as e:
        logger.error(f"Error calculating dispatch compliance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# PROBLEM #3: Single Order Risk Profiling Endpoint
@router.post("/order-risk", response_model=OrderRiskResponse)
def profile_order_risk(request: OrderRiskRequest):
    """
    PROBLEM #3: Profile Order Risk Before Dispatch
    
    This endpoint:
    - Assesses risk level for future orders
    - Uses deterministic scoring logic
    - Returns risk profile for planning decisions
    """
    try:
        risk_profile = get_network_design_service().profile_order_risk(
            request.order_no,
            request.sku,
            request.customer_pincode,
            request.delivery_period
        )
        
        if "error" in risk_profile:
            return OrderRiskResponse(
                status="error",
                message="Failed to profile order risk",
                risk_profile={},
                error=risk_profile["error"]
            )
        
        return OrderRiskResponse(
            status="success",
            message=f"Profiled order {request.order_no}: {risk_profile['Risk Level']} risk",
            risk_profile=risk_profile
        )
        
    except Exception as e:
        logger.error(f"Error profiling order risk: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# New Request/Response Models for CSV-based Analysis
class ComprehensiveBaselineRequest(BaseModel):
    """Request for comprehensive baseline network analysis"""
    limit: Optional[int] = Field(default=10000, description="Number of orders to return (default: 10000)")

class ComprehensiveBaselineResponse(BaseModel):
    """Response containing comprehensive baseline network table"""
    status: str
    message: str
    total_orders: int
    baseline_data: List[Dict[str, Any]]
    error: Optional[str] = None

class ComprehensiveComplianceRequest(BaseModel):
    """Request for comprehensive compliance analysis"""
    cost_per_km: float = 2.5
    order_data_path: str = "data/Order_Data_csv_files/Order Data 28.12.25.csv"
    pick_data_path: str = "data/Order_Pick_Data_csv_files/Order Pick Data 28.12.25.csv"

class ComprehensiveComplianceResponse(BaseModel):
    """Response containing comprehensive compliance metrics"""
    status: str
    message: str
    total_orders: int
    compliant_orders: int
    dispatch_compliance_pct: float
    non_compliant_orders: int
    avg_distance_gap_km: float
    cost_leakage_rupees: float
    top_hubs_with_violations: Dict[str, int]
    top_pincodes_with_violations: Dict[str, int]
    non_compliance_rate_pct: float
    compliance_by_sku_class: Dict[str, Dict[str, Any]]
    daily_compliance_trends: Dict[str, Dict[str, Any]]
    delivery_period_compliance: Dict[str, Dict[str, Any]]
    error: Optional[str] = None

class RiskAnalysisRequest(BaseModel):
    """Request for advanced risk pattern analysis"""
    pass

class RiskAnalysisResponse(BaseModel):
    """Response containing risk pattern analysis"""
    status: str
    message: str
    high_risk_pincodes: Dict[str, float]
    sku_class_performance: Dict[str, Dict[str, Any]]
    time_based_risk: Dict[str, float]
    delivery_risk_analysis: Dict[str, Dict[str, Any]]
    error: Optional[str] = None

# Comprehensive Baseline Network Endpoint
@router.post("/comprehensive-baseline")
async def generate_comprehensive_baseline_network(
    background_tasks: BackgroundTasks,
    data_source: str = Form("preloaded"),
    order_file: Optional[UploadFile] = File(None),
    pick_file: Optional[UploadFile] = File(None),
    limit: int = Form(10000),
):
    """
    Generate comprehensive baseline network using real CSV data
    
    This endpoint:
    - Creates enhanced baseline network with real operational data
    - Includes detailed compliance analysis
    - Provides foundation for advanced analytics
    - Supports both preloaded data and custom uploads
    """
    try:
        import time
        start_time = time.time()
        
        # Normalize and validate data_source
        data_source = data_source.lower().strip()
        if data_source not in ["preloaded", "custom"]:
            raise HTTPException(
                status_code=400,
                detail="data_source must be 'preloaded' or 'custom'"
            )
        
        if data_source == "preloaded":
            # Use existing preloaded data
            baseline_df = get_network_design_service().generate_comprehensive_baseline_network(limit=limit)
            processing_time = f"{time.time() - start_time:.2f}s"
            
            return {
                "status": "success",
                "data_source": "preloaded",
                "records_processed": len(baseline_df) if not baseline_df.empty else 0,
                "baseline_network": baseline_df.to_dict('records') if not baseline_df.empty else [],
                "compliance_metrics": {},
                "processing_time": processing_time
            }
            
        elif data_source == "custom":
            # Validate uploaded files
            if not order_file or not pick_file:
                raise HTTPException(
                    status_code=400, 
                    detail="Both order and pick files required for custom data"
                )
            
            # Enforce size limit (30 MB each)
            MAX_SIZE = 30 * 1024 * 1024  # 30 MB
            if hasattr(order_file, 'size') and order_file.size > MAX_SIZE:
                raise HTTPException(
                    status_code=400,
                    detail="Order file size exceeds 30 MB limit"
                )
            if hasattr(pick_file, 'size') and pick_file.size > MAX_SIZE:
                raise HTTPException(
                    status_code=400,
                    detail="Pick file size exceeds 30 MB limit"
                )
            
            # Create session-specific directory
            from app.services.network_design_service import DATA_DIR, SESSIONS_DIR
            session_id = f"session_{int(time.time())}_{hash(order_file.filename) & 0xffff}"
            temp_dir = SESSIONS_DIR / session_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            # Save uploads to disk
            temp_order_path = temp_dir / "order_data.csv"
            temp_pick_path = temp_dir / "pick_data.csv"
            
            try:
                # Read and save files (async operations inside async function)
                order_bytes = await order_file.read()
                with open(temp_order_path, "wb") as f:
                    f.write(order_bytes)
                
                pick_bytes = await pick_file.read()
                with open(temp_pick_path, "wb") as f:
                    f.write(pick_bytes)
                
                # Process uploaded files
                baseline_df, metrics = get_network_design_service().generate_baseline_from_files(
                    order_path=str(temp_order_path),
                    pick_path=str(temp_pick_path),
                    limit=limit
                )
                
                processing_time = f"{time.time() - start_time:.2f}s"
                
                # Schedule cleanup in background
                background_tasks.add_task(cleanup_session_files, temp_dir, 3600)
                
                return {
                    "status": "success",
                    "data_source": "custom",
                    "records_processed": len(baseline_df) if not baseline_df.empty else 0,
                    "baseline_network": baseline_df.to_dict('records') if not baseline_df.empty else [],
                    "compliance_metrics": metrics or {},
                    "processing_time": processing_time
                }
                
            except Exception as e:
                # Immediate cleanup on error
                import shutil
                if temp_dir.exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)
                raise HTTPException(
                    status_code=500, 
                    detail=f"Error processing files: {str(e)}"
                )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating comprehensive baseline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Comprehensive Compliance Analysis Endpoint
@router.post("/comprehensive-compliance", response_model=ComprehensiveComplianceResponse)
def calculate_comprehensive_compliance(request: ComprehensiveComplianceRequest):
    """
    Calculate comprehensive dispatch compliance with advanced analytics
    
    This endpoint:
    - Provides detailed compliance metrics across multiple dimensions
    - Analyzes SKU class performance, daily trends, delivery periods
    - Identifies top violators and cost leakage
    """
    try:
        # Use existing data if already loaded, otherwise use provided paths
        if get_network_design_service().csv_data_loaded:
            metrics = get_network_design_service().calculate_comprehensive_compliance(
                cost_per_km=request.cost_per_km,
                order_data_path=None,
                pick_data_path=None
            )
        else:
            metrics = get_network_design_service().calculate_comprehensive_compliance(
                cost_per_km=request.cost_per_km,
                order_data_path=request.order_data_path,
                pick_data_path=request.pick_data_path
            )
        
        if "error" in metrics and metrics["error"] is not None:
            return ComprehensiveComplianceResponse(
                status="error",
                message="Failed to calculate comprehensive compliance",
                total_orders=0,
                compliant_orders=0,
                dispatch_compliance_pct=0.0,
                non_compliant_orders=0,
                avg_distance_gap_km=0.0,
                cost_leakage_rupees=0.0,
                top_hubs_with_violations={},
                top_pincodes_with_violations={},
                non_compliance_rate_pct=0.0,
                compliance_by_sku_class={},
                daily_compliance_trends={},
                delivery_period_compliance={},
                error=metrics["error"]
            )
        
        return ComprehensiveComplianceResponse(
            status="success",
            message=f"Calculated comprehensive compliance: {metrics['dispatch_compliance_pct']}%",
            total_orders=metrics["total_orders"],
            compliant_orders=metrics["compliant_orders"],
            dispatch_compliance_pct=metrics["dispatch_compliance_pct"],
            non_compliant_orders=metrics["non_compliant_orders"],
            avg_distance_gap_km=metrics["avg_distance_gap_km"],
            cost_leakage_rupees=metrics["cost_leakage_rupees"],
            top_hubs_with_violations=metrics["top_hubs_with_violations"],
            top_pincodes_with_violations=metrics["top_pincodes_with_violations"],
            non_compliance_rate_pct=metrics["non_compliance_rate_pct"],
            compliance_by_sku_class=metrics["compliance_by_sku_class"],
            daily_compliance_trends=metrics["daily_compliance_trends"],
            delivery_period_compliance=metrics["delivery_period_compliance"]
        )
        
    except Exception as e:
        logger.error(f"Error calculating comprehensive compliance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Advanced Risk Analysis Endpoint
@router.post("/risk-analysis", response_model=RiskAnalysisResponse)
def analyze_risk_patterns(request: RiskAnalysisRequest):
    """
    Advanced risk pattern analysis using historical data
    
    This endpoint:
    - Identifies high-risk pincodes based on violation history
    - Analyzes SKU class performance and time-based risks
    - Provides predictive intelligence for order routing
    """
    try:
        risk_analysis = get_network_design_service().analyze_risk_patterns()
        
        if "error" in risk_analysis:
            return RiskAnalysisResponse(
                status="error",
                message="Failed to analyze risk patterns",
                high_risk_pincodes={},
                sku_class_performance={},
                time_based_risk={},
                delivery_risk_analysis={},
                error=risk_analysis["error"]
            )
        
        return RiskAnalysisResponse(
            status="success",
            message="Completed advanced risk pattern analysis",
            high_risk_pincodes=risk_analysis["high_risk_pincodes"],
            sku_class_performance=risk_analysis["sku_class_performance"],
            time_based_risk=risk_analysis["time_based_risk"],
            delivery_risk_analysis=risk_analysis["delivery_risk_analysis"]
        )
        
    except Exception as e:
        logger.error(f"Error analyzing risk patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload-csv")
async def upload_csv_files(
    order_data: UploadFile = File(..., description="Order data CSV file"),
    pick_data: UploadFile = File(..., description="Pick data CSV file")
):
    """
    Upload CSV files for network analysis.
    
    This endpoint:
    - Accepts order data and pick data CSV files
    - Validates file format and structure
    - Uses per-request temp storage for processing
    - Loads data into in-memory service state
    - Deletes temp files after processing
    - Returns success status with file information
    """
    MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024
    temp_dir: Optional[Path] = None
    try:
        # Validate file types
        if not order_data.filename.endswith('.csv') or not pick_data.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Both files must be CSV format")

        order_filename = Path(order_data.filename).name
        pick_filename = Path(pick_data.filename).name

        temp_dir = Path(tempfile.gettempdir()) / "scm_uploads" / str(uuid.uuid4())
        temp_dir.mkdir(parents=True, exist_ok=True)

        order_data_path = temp_dir / order_filename
        pick_data_path = temp_dir / pick_filename
        
        # Write files to disk
        order_bytes = await order_data.read()
        if len(order_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(status_code=413, detail="Order data file exceeds 20MB limit")
        with open(order_data_path, "wb") as f:
            f.write(order_bytes)
            
        pick_bytes = await pick_data.read()
        if len(pick_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(status_code=413, detail="Pick data file exceeds 20MB limit")
        with open(pick_data_path, "wb") as f:
            f.write(pick_bytes)
        
        # Validate CSV structure
        try:
            order_df = pd.read_csv(order_data_path)
            pick_df = pd.read_csv(pick_data_path)
            
            # Check for required columns
            required_order_cols = ['Order No', 'Pincode']
            required_pick_cols = ['Order No', 'Hub Pincode']
            
            missing_order_cols = [col for col in required_order_cols if col not in order_df.columns]
            missing_pick_cols = [col for col in required_pick_cols if col not in pick_df.columns]
            
            if missing_order_cols:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Order data missing required columns: {missing_order_cols}"
                )
                
            if missing_pick_cols:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Pick data missing required columns: {missing_pick_cols}"
                )
                
        except Exception as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid CSV format: {str(e)}"
            )

        loaded = get_network_design_service().load_csv_data(str(order_data_path), str(pick_data_path))
        if not loaded:
            raise HTTPException(status_code=400, detail="Failed to load uploaded CSV data")
        
        logger.info(f"Successfully processed CSV files: {order_filename}, {pick_filename}")
        
        return {
            "status": "success",
            "message": "CSV files processed successfully (temp files deleted)",
            "order_data": {
                "filename": order_filename,
                "rows": len(order_df),
                "columns": list(order_df.columns)
            },
            "pick_data": {
                "filename": pick_filename,
                "rows": len(pick_df),
                "columns": list(pick_df.columns)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading CSV files: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            if temp_dir is not None and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass

# CSV Merge Helper Functions (from merge_orders_picks.py)
def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def _map_columns(df: pd.DataFrame, expected_map: dict) -> pd.DataFrame:
    col_map = {}
    norm_to_col = { _norm(c): c for c in df.columns }
    for target, candidates in expected_map.items():
        for cand in candidates:
            if cand in norm_to_col:
                col_map[norm_to_col[cand]] = target
                break
    if col_map:
        return df.rename(columns=col_map)
    return df

ORDERS_EXPECTED = {
    'order_date': ['orderdate','order_date','orderdate','orderdate'],
    'order_time': ['ordertime','order_time','time'],
    'order_no': ['orderno','order_no','ordernumber','orderid','order id','order number'],
    'customer_code': ['customercode','customer_code','customer id','customerid'],
    'pincode': ['pincode','pin code','postalcode'],
    'sku': ['sku'],
    'sku_class': ['skuclass','sku_class','sku class'],
    'qty': ['qty','quantity']
}

PICKS_EXPECTED = {
    'order_no': ['orderno','order_no','ordernumber','orderid','order id'],
    'order_pickdate': ['orderpickdate','order_pickdate','pickdate','pick date','order pickdate'],
    'order_pick_hour': ['orderpickhour','order_pick_hour','pickhour','pick hour'],
    'order_pick_time': ['orderpicktime','order_pick_time','picktime','pick time','time'],
    'customer_code': ['customercode','customer_code','customer id','customerid'],
    'pincode': ['pincode','pin code','postalcode'],
    'sku': ['sku'],
    'sku_class': ['skuclass','sku_class','sku class'],
    'qty': ['qty','quantity'],
    'hub_pincode': ['hubpincode','hub_pincode','hub pincode','hubcode'],
    'delivery_period': ['deliveryperiod','delivery_period','delivery period'],
    'delivery_date': ['deliverydate','delivery_date','delivery date']
}

def _choose_col(df: pd.DataFrame, base: str, prefer_suffix: str = None):
    if prefer_suffix:
        key = f"{base}{prefer_suffix}"
        if key in df.columns:
            return df[key]
    if base in df.columns:
        return df[base]
    for sfx in ['_order','_pick']:
        k = f"{base}{sfx}"
        if k in df.columns:
            return df[k]
    return pd.Series([None] * len(df))

def _parse_day_pick(series: pd.Series) -> pd.Series:
    dates = pd.to_datetime(series.astype(str).replace('nan',''), dayfirst=True, errors='coerce')
    fmt = dates.dt.strftime('%d-%m-%y')
    return fmt.fillna('').apply(lambda x: f"{x}.csv" if x else '')

def merge_orders_and_picks(orders_df: pd.DataFrame, picks_df: pd.DataFrame) -> pd.DataFrame:
    """Merge orders and picks DataFrames following the merge_orders_picks.py logic"""
    orders = _map_columns(orders_df, ORDERS_EXPECTED)
    picks = _map_columns(picks_df, PICKS_EXPECTED)

    # Ensure we have minimal keys
    if 'order_no' not in orders.columns and 'order_no' not in picks.columns:
        raise ValueError('Could not detect `order_no` in either file. Check column names.')

    # Merge: left join picks into orders on order_no
    merged = pd.merge(orders, picks, left_on='order_no', right_on='order_no', how='left', suffixes=('_order','_pick'))

    # Build final dataframe with exact schema requested
    out = pd.DataFrame()
    out['order_date'] = _choose_col(merged, 'order_date', prefer_suffix='_order')
    out['order_time'] = _choose_col(merged, 'order_time', prefer_suffix='_order')
    out['order_no'] = merged['order_no']

    out['customer_code_order'] = _choose_col(merged, 'customer_code', prefer_suffix='_order')
    out['pincode_order'] = _choose_col(merged, 'pincode', prefer_suffix='_order')
    out['sku_order'] = _choose_col(merged, 'sku', prefer_suffix='_order')
    out['sku_class_order'] = _choose_col(merged, 'sku_class', prefer_suffix='_order')
    out['qty_order'] = _choose_col(merged, 'qty', prefer_suffix='_order')

    out['order_pickdate'] = _choose_col(merged, 'order_pickdate', prefer_suffix='_pick')
    out['order_pick_hour'] = _choose_col(merged, 'order_pick_hour', prefer_suffix='_pick')
    out['order_pick_time'] = _choose_col(merged, 'order_pick_time', prefer_suffix='_pick')
    out['customer_code_pick'] = _choose_col(merged, 'customer_code', prefer_suffix='_pick')
    out['pincode_pick'] = _choose_col(merged, 'pincode', prefer_suffix='_pick')
    out['sku_pick'] = _choose_col(merged, 'sku', prefer_suffix='_pick')
    out['sku_class_pick'] = _choose_col(merged, 'sku_class', prefer_suffix='_pick')
    out['qty_pick'] = _choose_col(merged, 'qty', prefer_suffix='_pick')

    out['hub_pincode'] = _choose_col(merged, 'hub_pincode', prefer_suffix='_pick')
    out['delivery_period'] = _choose_col(merged, 'delivery_period', prefer_suffix='_pick')
    out['delivery_date'] = _choose_col(merged, 'delivery_date', prefer_suffix='_pick')

    out['day_pick'] = _parse_day_pick(out['order_pickdate'])

    return out

# Request/Response Models for CSV Merge
class MergeCSVRequest(BaseModel):
    """Request for CSV merge operation"""
    pass  # Files are uploaded via Form data

class MergeCSVResponse(BaseModel):
    """Response for CSV merge operation"""
    status: str
    message: str
    total_rows: int
    output_filename: str
    columns: List[str]
    sample_data: List[Dict[str, Any]]
    error: Optional[str] = None

@router.post("/merge-csv")
async def merge_csv_files(
    order_data: UploadFile = File(..., description="Order data CSV file"),
    pick_data: UploadFile = File(..., description="Pick data CSV file"),
    output_filename: str = Form(default="combined_df.csv", description="Output filename"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Merge Order and Pick CSV files into combined format and download immediately.
    
    This endpoint:
    - Accepts order data and pick data CSV files
    - Automatically detects and maps column names (flexible casing/spaces)
    - Merges data on order_no with proper suffix handling
    - Returns merged CSV file for immediate download
    - Also saves merged file for use in other endpoints
    - Follows same logic as merge_orders_picks.py script
    
    Output schema:
    order_date,order_time,order_no,customer_code_order,pincode_order,sku_order,sku_class_order,qty_order,
    order_pickdate,order_pick_hour,order_pick_time,customer_code_pick,pincode_pick,sku_pick,sku_class_pick,qty_pick,
    hub_pincode,delivery_period,delivery_date,day_pick
    """
    MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024  # 20MB per file
    temp_dir: Optional[Path] = None
    
    try:
        # Validate file types
        if not order_data.filename.endswith('.csv') or not pick_data.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Both files must be CSV format")
        
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp())
        order_data_path = temp_dir / "orders.csv"
        pick_data_path = temp_dir / "picks.csv"
        output_path = temp_dir / output_filename
        
        # Save uploaded files
        order_bytes = await order_data.read()
        if len(order_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(status_code=413, detail="Order data file exceeds 20MB limit")
        with open(order_data_path, "wb") as f:
            f.write(order_bytes)
            
        pick_bytes = await pick_data.read()
        if len(pick_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(status_code=413, detail="Pick data file exceeds 20MB limit")
        with open(pick_data_path, "wb") as f:
            f.write(pick_bytes)
        
        # Load and merge CSV files
        try:
            orders_df = pd.read_csv(order_data_path)
            picks_df = pd.read_csv(pick_data_path)
            
            logger.info(f"Loaded orders: {len(orders_df)} rows, picks: {len(picks_df)} rows")
            logger.info(f"Orders columns: {list(orders_df.columns)}")
            logger.info(f"Picks columns: {list(picks_df.columns)}")
            
            # Merge the data
            merged_df = merge_orders_and_picks(orders_df, picks_df)
            
            # Save merged data to temp file for download
            merged_df.to_csv(output_path, index=False)
            
            # Also save to main data directory for use by other endpoints
            main_data_dir = Path("D:/Digital twin/Project Main/Web App/backend/data")
            main_output_path = main_data_dir / output_filename
            main_data_dir.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(main_output_path, index=False)
            
            logger.info(f"Successfully merged {len(orders_df)} orders and {len(picks_df)} picks into {len(merged_df)} combined records")
            logger.info(f"Merged file saved to: {output_path}")
            logger.info(f"Main file saved to: {main_output_path}")
            
            # Return the merged CSV file for download
            return FileResponse(
                path=output_path,
                filename=output_filename,
                media_type='text/csv',
                headers={
                    "Content-Disposition": f"attachment; filename={output_filename}",
                    "X-Merge-Stats": f"orders={len(orders_df)},picks={len(picks_df)},merged={len(merged_df)}"
                }
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Column mapping error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing CSV files: {str(e)}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error merging CSV files: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            if temp_dir is not None and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass

@router.get("/merge-preview/{filename}", response_model=MergeCSVResponse)
async def get_merge_preview(filename: str):
    """
    Get preview and statistics of a previously merged CSV file.
    
    This endpoint:
    - Returns merge statistics and sample data
    - Shows column information
    - Provides preview of merged data structure
    """
    try:
        # Check if file exists in data directory
        main_data_dir = Path("D:/Digital twin/Project Main/Web App/backend/data")
        file_path = main_data_dir / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Merged file '{filename}' not found")
        
        # Load the merged file
        merged_df = pd.read_csv(file_path)
        
        # Prepare sample data (first 5 rows)
        sample_data = merged_df.head(5).to_dict('records')
        
        # Convert NaN values to None for JSON serialization
        for row in sample_data:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
        
        return MergeCSVResponse(
            status="success",
            message=f"Preview of merged file '{filename}' with {len(merged_df)} records",
            total_rows=len(merged_df),
            output_filename=filename,
            columns=list(merged_df.columns),
            sample_data=sample_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting merge preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Cleanup function for session files
async def cleanup_session_files(temp_dir: Path, delay: int = 3600):
    """Clean up session files after delay"""
    import asyncio
    import shutil
    import logging
    
    logger = logging.getLogger(__name__)
    
    await asyncio.sleep(delay)
    try:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up session files: {temp_dir}")
    except Exception as e:
        logger.error(f"Error cleaning up session files {temp_dir}: {e}")
