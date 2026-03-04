from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum

class TwinType(str, Enum):
    FACTORY = "factory"
    SHIPMENT = "shipment"
    RETAILER = "retailer"
    DEVICE = "device"  


class TelemetryData(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    temperature: Optional[float] = Field(None, ge=-50, le=100)
    humidity: Optional[float] = Field(None, ge=0, le=100)
    pressure: Optional[float] = Field(None, ge=0)
    efficiency: Optional[float] = Field(None, ge=0, le=100)

class DigitalTwinBase(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    type: TwinType
    name: str = Field(..., min_length=1, max_length=200)
    location: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class DigitalTwinCreate(DigitalTwinBase):
    model_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)

class DigitalTwin(DigitalTwinBase):
    model_id: str
    properties: Dict[str, Any]
    telemetry: Optional[TelemetryData] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class TwinUpdate(BaseModel):
    properties: Dict[str, Any]
    telemetry: Optional[TelemetryData] = None

class DashboardResponse(BaseModel):
    factories: List[DigitalTwin]
    shipments: List[DigitalTwin]
    retailers: List[DigitalTwin]
    total_count: int
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class FactoryMetrics(BaseModel):
    efficiency: float = Field(..., ge=0, le=100)
    production_rate: float = Field(..., ge=0)
    quality_score: float = Field(..., ge=0, le=100)
    downtime_hours: float = Field(default=0, ge=0)
    energy_consumption: float = Field(default=0, ge=0)

class ShipmentStatus(str, Enum):
    PENDING = "pending"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    DELAYED = "delayed"

class ShipmentTracking(BaseModel):
    shipment_id: str
    status: ShipmentStatus
    current_location: str
    estimated_arrival: datetime
    route_optimization_score: float = Field(..., ge=0, le=100)