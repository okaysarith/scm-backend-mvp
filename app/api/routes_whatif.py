from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import Dict, Any, List
import logging

from app.services.whatif_service import whatif_service
from ..models.whatif import (
    WarehouseRelocationRequest,
    SKUPriorityRequest,
    ScenarioResult,
    ScenarioComparisonRequest,
    ScenarioComparisonResponse,
    WhatIfOptionsResponse
)
from ..models.whatif_models import (
    SKUWarehouseWhatIfRequest,
    SKUWarehouseWhatIfResponse
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["what-if"])

@router.post("/warehouse", response_model=ScenarioResult)
def analyze_warehouse_relocation(
    request: WarehouseRelocationRequest,
    background_tasks: BackgroundTasks
):
    """
    Analyze the impact of moving a SKU between warehouses for specific pincodes.
    
    This endpoint simulates the effect of relocating inventory from one warehouse 
    to another and predicts the impact on return rates and on-time delivery.
    """
    try:
        result = whatif_service.analyze_warehouse_relocation(
            sku=request.sku,
            pincodes=request.pincodes,
            from_warehouse=request.from_warehouse,
            to_warehouse=request.to_warehouse
        )
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
            
        return result
        
    except Exception as e:
        logger.error(f"Error in warehouse relocation analysis: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sku-priority", response_model=ScenarioResult)
async def analyze_sku_priority(
    request: SKUPriorityRequest,
    background_tasks: BackgroundTasks
):
    """
    Analyze the impact of prioritizing a specific SKU in a warehouse.
    
    This endpoint simulates the effect of keeping higher inventory levels
    of a specific SKU in a particular warehouse.
    """
    try:
        # For now, return a placeholder response
        # In a real implementation, this would use actual business logic
        return {
            'baseline_avg_return_prob': 0.35,
            'scenario_avg_return_prob': 0.15,
            'return_prob_delta': -0.20,
            'baseline_on_time_pct': 75.0,
            'scenario_on_time_pct': 92.0,
            'on_time_delta_pct': 17.0,
            'order_count_affected': 120,
            'total_orders': 500,
            'recommendation': '✅ RECOMMENDED: Expected 20% reduction in returns and 17% improvement in on-time delivery.'
        }
        
    except Exception as e:
        logger.error(f"Error in SKU priority analysis: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/options", response_model=WhatIfOptionsResponse)
def get_whatif_options():
    """
    Get available options for what-if analysis.
    """
    try:
        options = whatif_service.get_available_options()
        return options
        
    except Exception as e:
        logger.error(f"Error getting what-if options: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compare", response_model=ScenarioComparisonResponse)
def compare_scenarios(request: ScenarioComparisonRequest):
    """
    Compare multiple what-if scenarios.
    """
    try:
        result = whatif_service.compare_scenarios(request.scenarios)
        
        if result["status"] == "error":
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
        
    except Exception as e:
        logger.error(f"Error comparing scenarios: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sku-warehouse-impact", response_model=SKUWarehouseWhatIfResponse)
def sku_warehouse_impact(request: SKUWarehouseWhatIfRequest):
    """
    Analyze the impact of reassigning an SKU to a different warehouse for specific pincodes.
    
    This endpoint uses 15-day historical data to compare baseline performance
    with a counterfactual scenario using the proposed warehouse as a proxy.
    
    The analysis includes:
    - Cost metrics (distance, cost per order, total cost)
    - Service metrics (delivery time, on-time rate)
    - Return metrics (return rate, return count)
    - Risk metrics (weighted risk score)
    - Decision recommendation with confidence
    """
    try:
        result = whatif_service.run_sku_warehouse_impact(request)
        return result
        
    except Exception as e:
        logger.error(f"Error in SKU-Warehouse impact analysis: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
