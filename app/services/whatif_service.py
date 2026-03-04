import logging
from typing import Dict, List, Optional, Tuple,Any
import random
from datetime import datetime, timedelta
from dataclasses import dataclass
from .data_loader import data_loader
from ..models.whatif_models import (
    SKUWarehouseWhatIfRequest,
    SKUWarehouseWhatIfResponse,
    WhatIfMetrics,
    CostMetrics,
    ServiceMetrics,
    ReturnMetrics,
    RiskMetrics,
    DeltaMetrics,
    DecisionMetrics
)
from ..utils.whatif_metrics import (
    cost_metrics,
    service_metrics,
    return_metrics,
    risk_metrics,
    calculate_delta,
    calculate_decision_score
)
from ..utils.data_preprocessing import DataPreprocessor

logger = logging.getLogger(__name__)

@dataclass
class WarehouseLocation:
    """Warehouse location data"""
    warehouse_id: str
    latitude: float
    longitude: float
    capacity: int
    current_utilization: float

@dataclass
class ScenarioScore:
    """Scenario scoring metrics"""
    return_rate_improvement: float
    ontime_improvement: float
    cost_impact: float
    risk_score: float
    overall_score: float

class WhatIfService:
    """Service for running what-if scenarios on supply chain data"""
    
    def __init__(self):
        try:
            # Lazy loading - don't load large CSV files at startup
            self.orders = None
            self.warehouses = []
            self._data_loaded = False
            
            logger.info("WhatIfService initialized with lazy loading")
            
            self.scenario_history = []
            
        except Exception as e:
            logger.error(f"Error initializing WhatIfService: {e}")
            self.orders = None
            self.warehouses = []
            self.scenario_history = []
            self._data_loaded = False
    
    def _ensure_data_loaded(self):
        """Lazy load data only when needed"""
        if not self._data_loaded:
            try:
                logger.info("Loading CSV data on demand...")
                self.orders = data_loader.load_orders_from_csv("Order Data 28.12.25.csv")
                warehouse_data = data_loader.load_warehouses_from_csv("Master Data v2 dt 27 Dec 2025- Customer pincodes-hubs.csv")
                
                # Convert warehouse data to WarehouseLocation objects
                self.warehouses = [
                    WarehouseLocation(
                        warehouse_id=w['warehouse_id'],
                        latitude=w['latitude'],
                        longitude=w['longitude'],
                        capacity=w['capacity'],
                        current_utilization=w['current_utilization']
                    ) for w in warehouse_data
                ]
                
                self._data_loaded = True
                logger.info(f"Loaded {len(self.orders)} orders and {len(self.warehouses)} warehouses")
                
            except Exception as e:
                logger.error(f"Error loading CSV data, falling back to mock data: {e}")
                self.orders = self._generate_mock_orders()
                self.warehouses = self._generate_warehouse_locations()
                self._data_loaded = True
                logger.info(f"Using fallback mock data: {len(self.orders)} orders and {len(self.warehouses)} warehouses")
    
    def get_available_options(self) -> Dict[str, Any]:
        """Get available options for what-if analysis"""
        self._ensure_data_loaded()
        
        # Extract unique values from orders
        skus = list(set(order['sku'] for order in self.orders))
        
        # Get all locations with details
        all_locations = data_loader.get_all_locations_with_details()
        pincodes = [loc['pincode'] for loc in all_locations]
        
        warehouses = list(set(order['warehouse_id'] for order in self.orders))
        # Add warehouses from hardcoded mapping
        warehouses.extend(list(data_loader.warehouse_mapping.keys()))
        warehouses = list(set(warehouses))  # Remove duplicates
        
        return {
            "available_skus": skus,
            "available_warehouses": warehouses,
            "available_pincodes": pincodes,
            "location_details": {loc['pincode']: loc for loc in all_locations},
            "warehouse_locations": {
                w.warehouse_id: {
                    "latitude": w.latitude,
                    "longitude": w.longitude,
                    "capacity": w.capacity,
                    "current_utilization": w.current_utilization
                }
                for w in self.warehouses
            },
            "order_summary": {
                "total_orders": len(self.orders),
                "unique_skus": len(skus),
                "unique_pincodes": len(pincodes),
                "active_warehouses": len(warehouses)
            }
        }
    
    def compare_scenarios(
        self,
        scenarios: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Compare multiple what-if scenarios"""
        try:
            scenario_results = []
            
            for i, scenario in enumerate(scenarios):
                result = self.analyze_warehouse_relocation(
                    sku=scenario['sku'],
                    pincodes=scenario['pincodes'],
                    from_warehouse=scenario['from_warehouse'],
                    to_warehouse=scenario['to_warehouse']
                )
                
                # Calculate composite score
                score = self._calculate_scenario_score(result)
                
                scenario_results.append({
                    "scenario_id": i + 1,
                    "scenario_config": scenario,
                    "results": result,
                    "score": score,
                    "rank": 0  # Will be calculated after sorting
                })
            
            # Sort by overall score
            scenario_results.sort(key=lambda x: x['score'].overall_score, reverse=True)
            
            # Assign ranks
            for i, result in enumerate(scenario_results):
                result['rank'] = i + 1
            
            return {
                "status": "success",
                "scenarios": scenario_results,
                "recommendation": scenario_results[0] if scenario_results else None,
                "total_scenarios": len(scenario_results)
            }
            
        except Exception as e:
            logger.error(f"Error comparing scenarios: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _calculate_scenario_score(self, result: Dict[str, Any]) -> ScenarioScore:
        """Calculate composite score for a scenario"""
        return_rate_improvement = result.get('return_prob_delta', 0) * -1  # Negative delta is good
        ontime_improvement = result.get('on_time_delta_pct', 0)
        
        # Simulate cost impact (in real implementation, this would use actual cost data)
        orders_affected = result.get('order_count_affected', 0)
        cost_impact = orders_affected * 2.5  # $2.50 per order improvement
        
        # Calculate risk score (lower is better)
        risk_score = max(0, 1 - (return_rate_improvement + ontime_improvement / 100) / 2)
        
        # Overall score (0-100)
        overall_score = (
            (return_rate_improvement * 30) + 
            (ontime_improvement * 20) + 
            (min(cost_impact / 100, 1) * 30) + 
            ((1 - risk_score) * 20)
        )
        
        return ScenarioScore(
            return_rate_improvement=return_rate_improvement,
            ontime_improvement=ontime_improvement,
            cost_impact=cost_impact,
            risk_score=risk_score,
            overall_score=min(overall_score, 100)
        )
    
    def _generate_warehouse_locations(self) -> List[WarehouseLocation]:
        """Generate mock warehouse location data"""
        return [
            WarehouseLocation("W1", 19.0760, 72.8777, 1000, 0.75),  # Mumbai
            WarehouseLocation("W2", 28.6139, 77.2090, 800, 0.60),   # Delhi
            WarehouseLocation("W3", 12.9716, 77.5946, 600, 0.45),   # Bangalore
        ]
    
    def analyze_warehouse_relocation(
        self, 
        sku: str,
        pincodes: List[str],
        from_warehouse: str,
        to_warehouse: str
    ) -> Dict:
        """
        Analyze the impact of moving a SKU between warehouses for specific pincodes
        
        Args:
            sku: The SKU to be relocated
            pincodes: List of pincodes to analyze
            from_warehouse: Current warehouse ID
            to_warehouse: Proposed warehouse ID
            
        Returns:
            Dictionary with analysis results
        """
        # Filter orders for the specified SKU, pincodes, and current warehouse
        relevant_orders = [
            o for o in self.orders 
            if o['sku'] == sku and 
               o['pincode'] in pincodes and 
               o['warehouse_id'] == from_warehouse
        ]
        
        if not relevant_orders:
            return {
                'error': 'No matching orders found for the specified criteria',
                'order_count_affected': 0
            }
        
        # Calculate baseline metrics
        baseline_return_probs = [o['return_probability'] for o in relevant_orders]
        baseline_on_time = [1 if o['on_time'] else 0 for o in relevant_orders]
        
        # Simulate scenario metrics
        # In a real implementation, this would use actual distance/fulfillment data
        scenario_return_probs = [max(0, p * 0.7) for p in baseline_return_probs]  # 30% reduction in returns
        scenario_on_time = [1 if random.random() < 0.88 else 0 for _ in relevant_orders]  # 88% on-time
        
        # Calculate metrics
        baseline_avg_return = sum(baseline_return_probs) / len(baseline_return_probs)
        scenario_avg_return = sum(scenario_return_probs) / len(scenario_return_probs)
        
        baseline_ontime_pct = (sum(baseline_on_time) / len(baseline_on_time)) * 100
        scenario_ontime_pct = (sum(scenario_on_time) / len(scenario_on_time)) * 100
        
        # Generate recommendation
        return_prob_improved = baseline_avg_return - scenario_avg_return
        ontime_improved = scenario_ontime_pct - baseline_ontime_pct
        
        if return_prob_improved > 0.05 and ontime_improved > 5:
            recommendation = "✅ STRONGLY RECOMMEND: Significant improvements in both return rate and on-time delivery"
        elif return_prob_improved > 0.02 or ontime_improved > 2:
            recommendation = "👍 RECOMMEND: Moderate improvements expected"
        else:
            recommendation = "⚠️ MARGINAL BENEFIT: Consider other optimization opportunities"
        
        return {
            'baseline_avg_return_prob': round(baseline_avg_return, 2),
            'scenario_avg_return_prob': round(scenario_avg_return, 2),
            'return_prob_delta': round(return_prob_improved, 2),
            'baseline_on_time_pct': round(baseline_ontime_pct, 1),
            'scenario_on_time_pct': round(scenario_ontime_pct, 1),
            'on_time_delta_pct': round(ontime_improved, 1),
            'order_count_affected': len(relevant_orders),
            'total_orders': len(self.orders),
            'recommendation': recommendation
        }
    
    def run_sku_warehouse_impact(self, payload: SKUWarehouseWhatIfRequest) -> SKUWarehouseWhatIfResponse:
        """
        Run SKU-Warehouse impact analysis using 15-day historical data
        
        Args:
            payload: Request containing SKU, pincodes, current and proposed warehouse
            
        Returns:
            Complete impact analysis with metrics and recommendation
        """
        try:
            # Load 15-day data from CSV files
            picks_df = data_loader.load_csv_data("Order_Data_csv_files/Order Pick Data 28.12.25.csv")
            returns_df = data_loader.load_csv_data("Order_Data_csv_files/Order Return Data.csv")
            
            if picks_df.empty:
                logger.warning("No picks data available, using fallback")
                return self._create_fallback_response(payload)
            
            # Build baseline dataset
            preprocessor = DataPreprocessor()
            base_picks, base_returns = preprocessor.build_baseline(
                picks_df, returns_df,
                payload.sku_code,
                payload.pincodes,
                payload.current_warehouse
            )
            
            # Build scenario proxy dataset
            scen_picks, scen_returns = preprocessor.build_scenario_proxy(
                picks_df, returns_df,
                payload.sku_code,
                payload.pincodes,
                payload.proposed_warehouse
            )
            
            # Calculate baseline metrics
            baseline_cost = cost_metrics(base_picks)
            baseline_service = service_metrics(base_picks)
            baseline_returns = return_metrics(base_picks, base_returns)
            baseline_risk = risk_metrics(base_picks, base_returns)
            
            baseline_metrics = WhatIfMetrics(
                cost=CostMetrics(**baseline_cost),
                service=ServiceMetrics(**baseline_service),
                returns=ReturnMetrics(**baseline_returns),
                risk=RiskMetrics(**baseline_risk)
            )
            
            # Calculate scenario metrics
            scen_cost = cost_metrics(scen_picks)
            scen_service = service_metrics(scen_picks)
            scen_returns = return_metrics(scen_picks, scen_returns)
            scen_risk = risk_metrics(scen_picks, scen_returns)
            
            scenario_metrics = WhatIfMetrics(
                cost=CostMetrics(**scen_cost),
                service=ServiceMetrics(**scen_service),
                returns=ReturnMetrics(**scen_returns),
                risk=RiskMetrics(**scen_risk)
            )
            
            # Calculate delta metrics
            delta_metrics_dict = {}
            delta_metrics_dict.update(calculate_delta(baseline_cost, scen_cost))
            delta_metrics_dict.update(calculate_delta(baseline_service, scen_service))
            delta_metrics_dict.update(calculate_delta(baseline_returns, scen_returns))
            delta_metrics_dict.update(calculate_delta(baseline_risk, scen_risk))
            
            delta_metrics = DeltaMetrics(
                cost_per_order_delta=delta_metrics_dict.get("cost_per_order_delta", 0),
                total_cost_delta=delta_metrics_dict.get("total_cost_delta", 0),
                avg_delivery_days_delta=delta_metrics_dict.get("avg_delivery_days_delta", 0),
                return_rate_delta=delta_metrics_dict.get("return_rate_delta", 0),
                risk_score_delta=delta_metrics_dict.get("risk_score_delta", 0)
            )
            
            # Calculate decision
            decision_score, recommendation, confidence, reasoning = calculate_decision_score(delta_metrics_dict)
            
            decision = DecisionMetrics(
                recommendation=recommendation,
                confidence=confidence,
                decision_score=decision_score,
                reasoning=reasoning
            )
            
            return SKUWarehouseWhatIfResponse(
                baseline_metrics=baseline_metrics,
                scenario_metrics=scenario_metrics,
                delta_metrics=delta_metrics,
                decision=decision
            )
            
        except Exception as e:
            logger.error(f"Error in SKU-Warehouse impact analysis: {e}")
            return self._create_fallback_response(payload)
    
    def _create_fallback_response(self, payload: SKUWarehouseWhatIfRequest) -> SKUWarehouseWhatIfResponse:
        """Create fallback response when data is unavailable"""
        # Create minimal fallback metrics
        fallback_cost = CostMetrics(avg_distance_km=50.0, cost_per_order=1000.0, total_cost=50000.0)
        fallback_service = ServiceMetrics(order_count=50, daily_avg_orders=3.33, avg_delivery_days=3.0, on_time_rate=0.85)
        fallback_returns = ReturnMetrics(return_rate=0.05, return_count=2, return_cost_impact=50.0)
        fallback_risk = RiskMetrics(risk_score=75.0, abc_weight=1.0)
        
        baseline_metrics = WhatIfMetrics(
            cost=fallback_cost,
            service=fallback_service,
            returns=fallback_returns,
            risk=fallback_risk
        )
        
        # Create scenario with slight improvements
        scenario_cost = CostMetrics(avg_distance_km=45.0, cost_per_order=900.0, total_cost=45000.0)
        scenario_service = ServiceMetrics(order_count=50, daily_avg_orders=3.33, avg_delivery_days=2.8, on_time_rate=0.87)
        scenario_returns = ReturnMetrics(return_rate=0.045, return_count=2, return_cost_impact=45.0)
        scenario_risk = RiskMetrics(risk_score=67.5, abc_weight=1.0)
        
        scenario_metrics = WhatIfMetrics(
            cost=scenario_cost,
            service=scenario_service,
            returns=scenario_returns,
            risk=scenario_risk
        )
        
        delta_metrics = DeltaMetrics(
            cost_per_order_delta=-100.0,
            total_cost_delta=-5000.0,
            avg_delivery_days_delta=-0.2,
            return_rate_delta=-0.005,
            risk_score_delta=-7.5
        )
        
        decision = DecisionMetrics(
            recommendation="MOVE",
            confidence="MEDIUM",
            decision_score=125.0,
            reasoning="Based on fallback analysis: Cost reduction, improved delivery time, lower return rate"
        )
        
        return SKUWarehouseWhatIfResponse(
            baseline_metrics=baseline_metrics,
            scenario_metrics=scenario_metrics,
            delta_metrics=delta_metrics,
            decision=decision
        )
        """Generate mock order data for demonstration"""
        orders = []
        skus = [f'SKU00{i}' for i in range(1, 6)]
        warehouses = ['W1', 'W2', 'W3']
        pincodes = ['400601', '400602', '400603', '400604', '400605']
        
        for i in range(100):
            warehouse = random.choice(warehouses)
            pincode = random.choice(pincodes)
            sku = random.choice(skus)
            
            # Base return probability (lower is better)
            base_return_prob = random.uniform(0.1, 0.4)
            
            # Adjust return probability based on warehouse-pincode distance
            # In a real system, this would use actual distance data
            if warehouse == 'W1' and pincode in ['400601', '400602']:
                return_prob = base_return_prob * 0.7  # Closer warehouse
            elif warehouse == 'W2' and pincode in ['400603', '400604']:
                return_prob = base_return_prob * 0.8
            else:
                return_prob = base_return_prob
            
            # On-time delivery (higher is better)
            on_time = random.random() > 0.3  # 70% on-time baseline
            
            orders.append({
                'order_id': f'ORD{1000 + i}',
                'sku': sku,
                'warehouse_id': warehouse,
                'pincode': pincode,
                'return_probability': return_prob,
                'on_time': on_time,
                'order_date': (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat()
            })
        
        return orders

# Singleton instance
whatif_service = WhatIfService()
