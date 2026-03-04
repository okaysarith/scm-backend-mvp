from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class SKUWarehouseWhatIfRequest(BaseModel):
    """Request model for SKU-Warehouse impact analysis"""
    sku_code: str
    pincodes: List[str]
    current_warehouse: str
    proposed_warehouse: str

class CostMetrics(BaseModel):
    """Cost-related metrics"""
    avg_distance_km: float
    cost_per_order: float
    total_cost: float

class ServiceMetrics(BaseModel):
    """Service-related metrics"""
    order_count: int
    daily_avg_orders: float
    avg_delivery_days: float
    on_time_rate: float

class ReturnMetrics(BaseModel):
    """Return-related metrics"""
    return_rate: float
    return_count: int
    return_cost_impact: float

class RiskMetrics(BaseModel):
    """Risk-related metrics"""
    risk_score: float
    abc_weight: float

class WhatIfMetrics(BaseModel):
    """Complete metrics for baseline or scenario"""
    cost: CostMetrics
    service: ServiceMetrics
    returns: ReturnMetrics
    risk: RiskMetrics

class DeltaMetrics(BaseModel):
    """Delta comparison between scenario and baseline"""
    cost_per_order_delta: float
    total_cost_delta: float
    avg_delivery_days_delta: float
    return_rate_delta: float
    risk_score_delta: float

class DecisionMetrics(BaseModel):
    """Decision recommendation"""
    recommendation: str  # "MOVE" or "DO_NOT_MOVE"
    confidence: str  # "HIGH", "MEDIUM", "LOW"
    decision_score: float
    reasoning: str

class SKUWarehouseWhatIfResponse(BaseModel):
    """Complete response for SKU-Warehouse impact analysis"""
    baseline_metrics: WhatIfMetrics
    scenario_metrics: WhatIfMetrics
    delta_metrics: DeltaMetrics
    decision: DecisionMetrics
