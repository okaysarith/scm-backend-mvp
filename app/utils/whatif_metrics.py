"""
What-If Analysis Metrics Engine
Pure functions for calculating supply chain metrics
"""

import pandas as pd
from typing import Dict, Tuple, Any

# Constants
COST_PER_KM = 20
ABC_WEIGHTS = {"A": 1.5, "B": 1.0, "C": 0.6}
DECISION_WEIGHTS = {
    "cost_saving": 0.5,
    "service_improvement": 0.3,
    "risk": 0.2
}

def cost_metrics(picks_df: pd.DataFrame, hub_map: Dict[str, Any] = None) -> Dict[str, float]:
    """Calculate cost-related metrics"""
    if picks_df.empty:
        return {
            "avg_distance_km": 0.0,
            "cost_per_order": 0.0,
            "total_cost": 0.0
        }
    
    # Use Distance_KM if available, otherwise estimate
    if "Distance_KM" in picks_df.columns:
        avg_distance_km = picks_df["Distance_KM"].mean()
    else:
        # Fallback: estimate based on warehouse-pincode mapping
        avg_distance_km = 50.0  # Default estimate
    
    cost_per_order = avg_distance_km * COST_PER_KM
    total_cost = cost_per_order * len(picks_df)
    
    return {
        "avg_distance_km": round(avg_distance_km, 2),
        "cost_per_order": round(cost_per_order, 2),
        "total_cost": round(total_cost, 2)
    }

def service_metrics(picks_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate service-related metrics"""
    if picks_df.empty:
        return {
            "order_count": 0,
            "daily_avg_orders": 0.0,
            "avg_delivery_days": 0.0,
            "on_time_rate": 0.0
        }
    
    order_count = len(picks_df)
    daily_avg_orders = order_count / 15  # 15-day period
    
    # Calculate delivery metrics
    if "Delivery_Days" in picks_df.columns:
        avg_delivery_days = picks_df["Delivery_Days"].mean()
    else:
        avg_delivery_days = 3.0  # Default estimate
    
    # Calculate on-time delivery rate
    if "On_Time" in picks_df.columns:
        on_time_rate = picks_df["On_Time"].mean()
    elif "on_time" in picks_df.columns:
        on_time_rate = picks_df["on_time"].mean()
    else:
        on_time_rate = 0.85  # Default estimate
    
    return {
        "order_count": order_count,
        "daily_avg_orders": round(daily_avg_orders, 2),
        "avg_delivery_days": round(avg_delivery_days, 2),
        "on_time_rate": round(on_time_rate, 3)
    }

def return_metrics(picks_df: pd.DataFrame, returns_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate return-related metrics"""
    if picks_df.empty:
        return {
            "return_rate": 0.0,
            "return_count": 0,
            "return_cost_impact": 0.0
        }
    
    return_count = len(returns_df)
    total_orders = len(picks_df)
    return_rate = return_count / max(total_orders, 1)
    
    # Estimate return cost impact
    if "Order_Value" in picks_df.columns:
        avg_order_value = picks_df["Order_Value"].mean()
    else:
        avg_order_value = 1000.0  # Default estimate
    
    return_cost_impact = return_rate * avg_order_value
    
    return {
        "return_rate": round(return_rate, 4),
        "return_count": return_count,
        "return_cost_impact": round(return_cost_impact, 2)
    }

def risk_score(return_rate: float, avg_order_value: float, abc_class: str = "B") -> float:
    """Calculate weighted risk score"""
    abc_weight = ABC_WEIGHTS.get(abc_class, 1.0)
    return return_rate * avg_order_value * abc_weight

def risk_metrics(picks_df: pd.DataFrame, returns_df: pd.DataFrame, abc_class: str = "B") -> Dict[str, float]:
    """Calculate complete risk metrics"""
    if picks_df.empty:
        return {
            "risk_score": 0.0,
            "abc_weight": ABC_WEIGHTS.get(abc_class, 1.0)
        }
    
    return_rate = len(returns_df) / max(len(picks_df), 1)
    
    if "Order_Value" in picks_df.columns:
        avg_order_value = picks_df["Order_Value"].mean()
    else:
        avg_order_value = 1000.0  # Default estimate
    
    score = risk_score(return_rate, avg_order_value, abc_class)
    
    return {
        "risk_score": round(score, 2),
        "abc_weight": ABC_WEIGHTS.get(abc_class, 1.0)
    }

def calculate_delta(baseline: Dict[str, float], scenario: Dict[str, float]) -> Dict[str, float]:
    """Calculate delta between scenario and baseline metrics"""
    delta = {}
    for key in baseline.keys():
        baseline_val = baseline.get(key, 0.0)
        scenario_val = scenario.get(key, 0.0)
        delta[f"{key}_delta"] = round(scenario_val - baseline_val, 4)
    return delta

def calculate_decision_score(delta_metrics: Dict[str, float]) -> Tuple[float, str, str]:
    """Calculate decision score and recommendation"""
    # Extract key deltas
    cost_saving = -delta_metrics.get("total_cost_delta", 0)  # Negative delta = saving
    delivery_improvement = -delta_metrics.get("avg_delivery_days_delta", 0)  # Negative = faster
    risk_increase = delta_metrics.get("risk_score_delta", 0)  # Positive = higher risk
    
    # Calculate weighted score
    decision_score = (
        DECISION_WEIGHTS["cost_saving"] * cost_saving +
        DECISION_WEIGHTS["service_improvement"] * delivery_improvement -
        DECISION_WEIGHTS["risk"] * risk_increase
    )
    
    # Decision logic
    cost_delta_ok = delta_metrics.get("total_cost_delta", 0) < 0  # Cost reduction
    return_delta_ok = delta_metrics.get("return_rate_delta", 0) <= 0.02  # <= 2% increase
    delivery_delta_ok = delta_metrics.get("avg_delivery_days_delta", 0) <= 0  # No slower
    
    if cost_delta_ok and return_delta_ok and delivery_delta_ok:
        recommendation = "MOVE"
        confidence = "HIGH" if decision_score > 100 else "MEDIUM"
    else:
        recommendation = "DO_NOT_MOVE"
        confidence = "HIGH" if decision_score < -100 else "MEDIUM"
    
    # Generate reasoning
    reasoning_parts = []
    if cost_delta_ok:
        reasoning_parts.append(f"Cost reduction: {abs(delta_metrics.get('total_cost_delta', 0)):.2f}")
    else:
        reasoning_parts.append(f"Cost increase: {delta_metrics.get('total_cost_delta', 0):.2f}")
    
    if return_delta_ok:
        reasoning_parts.append(f"Return rate acceptable: {delta_metrics.get('return_rate_delta', 0):.3%}")
    else:
        reasoning_parts.append(f"Return rate too high: {delta_metrics.get('return_rate_delta', 0):.3%}")
    
    if delivery_delta_ok:
        reasoning_parts.append(f"Delivery time maintained: {delta_metrics.get('avg_delivery_days_delta', 0):.1f} days")
    else:
        reasoning_parts.append(f"Delivery slower: {delta_metrics.get('avg_delivery_days_delta', 0):.1f} days")
    
    reasoning = "; ".join(reasoning_parts)
    
    return round(decision_score, 2), recommendation, confidence, reasoning
