from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class NearestHubRequest(BaseModel):
    pincode: str = Field(..., description="PIN code to find nearest hub for")

class NearestHubResponse(BaseModel):
    pincode: str
    nearest_hub: Optional[str] = None
    hub_pincode: Optional[str] = None
    distance_km: Optional[float] = None
    pincode_coordinates: Optional[Dict[str, float]] = None
    hub_coordinates: Optional[Dict[str, float]] = None
    error: Optional[str] = None

class NetworkCoverageRequest(BaseModel):
    pincodes: List[str] = Field(..., description="List of PIN codes to analyze")

class NetworkCoverageResponse(BaseModel):
    total_pincodes: int
    successful_assignments: int
    failed_assignments: int
    hub_coverage: Dict[str, Any]
    detailed_results: List[Dict[str, Any]]

class NetworkOptimizationRequest(BaseModel):
    use_existing_data: bool = Field(True, description="Use existing order data for optimization")
    limit_orders: Optional[int] = Field(None, description="Limit number of orders for analysis")
    pincodes: Optional[List[str]] = Field(None, description="List of PIN codes to optimize for")
    optimization_type: Optional[str] = Field("cost", description="Type of optimization: cost, coverage, or efficiency")
    constraints: Optional[Dict[str, Any]] = Field(None, description="Optimization constraints like max_distance, min_coverage")
    sku_filter: Optional[List[str]] = Field(None, description="Filter by specific SKUs")
    pincode_filter: Optional[List[str]] = Field(None, description="Filter by specific PIN codes")

class NetworkOptimizationResponse(BaseModel):
    total_orders: int
    unique_pincodes: int
    coverage_analysis: Dict[str, Any]
    hub_order_volume: Dict[str, Any]
    optimization_suggestions: List[str]

class ExcelToCSVRequest(BaseModel):
    filename: str = Field("master_data.csv", description="Output CSV filename")
    create_master_dataframe: bool = Field(True, description="Create combined master dataframe")

class ExcelToCSVResponse(BaseModel):
    status: str
    message: str
    file_path: Optional[str] = None
    data_summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class DispatchAnalysisRequest(BaseModel):
    start_date: Optional[datetime] = Field(None, description="Start date for dispatch analysis")
    end_date: Optional[datetime] = Field(None, description="End date for dispatch analysis")
    sku_filter: Optional[List[str]] = Field(None, description="Filter by specific SKUs")
    pincode_filter: Optional[List[str]] = Field(None, description="Filter by specific PIN codes")

class DispatchAnalysisResponse(BaseModel):
    total_orders: int
    date_range: Dict[str, str]
    top_skus: List[Dict[str, Any]]
    top_pincodes: List[Dict[str, Any]]
    hub_performance: Dict[str, Any]
    daily_trends: List[Dict[str, Any]]

# Comprehensive Compliance Models
class ComprehensiveComplianceRequest(BaseModel):
    cost_per_km: float = Field(2.5, description="Cost per kilometer for compliance calculation")
    order_data_path: Optional[str] = Field(None, description="Path to order data CSV file")
    pick_data_path: Optional[str] = Field(None, description="Path to pick data CSV file")

class ComprehensiveComplianceResponse(BaseModel):
    status: str
    message: str
    total_orders: int
    compliant_orders: int
    dispatch_compliance_pct: float
    non_compliant_orders: int
    avg_distance_gap_km: float
    cost_leakage_rupees: float
    top_hubs_with_violations: Dict[str, Any]
    top_pincodes_with_violations: Dict[str, Any]
    non_compliance_rate_pct: float
    compliance_by_sku_class: Dict[str, Any]
    daily_compliance_trends: Dict[str, Any]
    delivery_period_compliance: Dict[str, Any]
    error: Optional[str] = None
