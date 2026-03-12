"""
Network layer validation utilities
"""
from fastapi import HTTPException
from typing import Dict, Any

class NetworkValidationError(Exception):
    """Custom validation error for network endpoints"""
    pass

def validate_data_source(data_source: str) -> str:
    """Validate and normalize data source"""
    valid_sources = ["existing", "uploaded", "custom"]
    if data_source not in valid_sources:
        raise NetworkValidationError(f"Invalid data_source: {data_source}. Must be one of: {valid_sources}")
    return data_source.lower().strip()

def validate_pincode(pincode: str) -> str:
    """Validate pincode format"""
    if not pincode or not pincode.strip():
        raise NetworkValidationError("Pincode cannot be empty")
    if not pincode.strip().isdigit():
        raise NetworkValidationError("Pincode must contain only digits")
    return pincode.strip()

def validate_limit(limit: int) -> int:
    """Validate limit parameter"""
    if limit <= 0:
        raise NetworkValidationError("Limit must be positive")
    if limit > 100000:
        raise NetworkValidationError("Limit too large (max 100000)")
    return limit
