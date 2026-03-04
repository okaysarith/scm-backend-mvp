from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum

class ScenarioComparisonRequest(BaseModel):
    """Request model for comparing multiple scenarios"""
    scenarios: List[Dict[str, Any]] = Field(..., description="List of scenario configurations")

class ScenarioScore(BaseModel):
    """Scenario scoring metrics"""
    return_rate_improvement: float
    ontime_improvement: float
    cost_impact: float
    risk_score: float
    overall_score: float

class ScenarioComparisonResult(BaseModel):
    """Result model for scenario comparison"""
    scenario_id: int
    scenario_config: Dict[str, Any]
    results: Dict[str, Any]
    score: ScenarioScore
    rank: int

class ScenarioComparisonResponse(BaseModel):
    """Response model for scenario comparison"""
    status: str
    scenarios: List[ScenarioComparisonResult] = []
    recommendation: Optional[ScenarioComparisonResult] = None
    total_scenarios: int
    message: Optional[str] = None

class WhatIfOptionsResponse(BaseModel):
    """Response model for what-if analysis options"""
    available_skus: List[str]
    available_warehouses: List[str]
    available_pincodes: List[str]
    warehouse_locations: Dict[str, Dict[str, Any]]
    order_summary: Dict[str, Any]

class WarehouseRelocationRequest(BaseModel):
    sku: str = Field(..., description="The SKU to be relocated")
    pincodes: List[str] = Field(..., description="List of pincodes to apply this change to")
    from_warehouse: str = Field(..., alias="from_warehouse", description="Source warehouse ID")
    to_warehouse: str = Field(..., alias="to_warehouse", description="Target warehouse ID")

class SKUPriorityRequest(BaseModel):
    sku: str = Field(..., description="The SKU to prioritize")
    warehouse_id: str = Field(..., description="Warehouse ID to prioritize the SKU in")

class ScenarioResult(BaseModel):
    baseline_avg_return_prob: float = Field(..., description="Average return probability in baseline scenario")
    scenario_avg_return_prob: float = Field(..., description="Average return probability in what-if scenario")
    return_prob_delta: float = Field(..., description="Change in return probability (negative is better)")
    baseline_on_time_pct: float = Field(..., description="On-time delivery percentage in baseline")
    scenario_on_time_pct: float = Field(..., description="On-time delivery percentage in scenario")
    on_time_delta_pct: float = Field(..., description="Change in on-time percentage")
    order_count_affected: int = Field(..., description="Number of orders affected by this change")
    total_orders: int = Field(..., description="Total number of orders in the system")
    recommendation: str = Field(..., description="Recommended action based on the analysis")
