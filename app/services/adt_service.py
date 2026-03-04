# backend/app/services/adt_service.py

from azure.identity import ClientSecretCredential
from azure.digitaltwins.core import DigitalTwinsClient
from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime
from app.config import settings
#from models import DigitalTwin, TwinType

logger = logging.getLogger(__name__)

class ADTService:
    """Service for interacting with Azure Digital Twins"""
    
    def __init__(self):
        self.client: Optional[DigitalTwinsClient] = None
        self._initialized = False
        self._use_mock = False
        self._mock_twins = []
    
    async def initialize(self):
        """Set up connection to Azure Digital Twins"""
        if self._initialized:
            return
        
        # Check if credentials are placeholders
        if (settings.AZURE_TENANT_ID == "placeholder-tenant-id" or
            not settings.AZURE_TENANT_ID or
            not settings.AZURE_CLIENT_ID or
            not settings.AZURE_CLIENT_SECRET):
            logger.warning("⚠️  Azure credentials not configured - using MOCK DATA")
            await self._initialize_mock_data()
            return
        
        try:
            # Try to connect to real Azure Digital Twins
            credential = ClientSecretCredential(
            tenant_id=settings.AZURE_TENANT_ID,
            client_id=settings.AZURE_CLIENT_ID,
            client_secret=settings.AZURE_CLIENT_SECRET
            )

            self.client = DigitalTwinsClient(
                settings.ADT_SERVICE_URL,  # Changed from AZURE_DIGITAL_TWINS_URL
                credential
            )
            
            # Test connection
            await asyncio.to_thread(self.client.list_models)
            
            self._initialized = True
            self._use_mock = False
            logger.info("✅ Connected to REAL Azure Digital Twins")
            
        except Exception as e:
            logger.error(f"❌ Azure connection failed: {str(e)}")
            logger.warning("⚠️  Falling back to MOCK DATA")
            await self._initialize_mock_data()
    
    async def _initialize_mock_data(self):
        """Initialize with mock data for development"""
        self._mock_twins = [
            {
                "id": "CM44210",
                "type": TwinType.FACTORY,
                "name": "Component Manufacturing",
                "model_id": "dtmi:supplychaindt:Factory;1",
                "properties": {
                    "efficiency": 91,
                    "openorders": 63,
                    "reliability": 92,
                    "location": "Pakistan"
                },
                "telemetry": {
                    "temperature": 28.0,
                    "humidity": 50.0,
                    "efficiency": 91.0,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "CNC90322",
                "type": TwinType.DEVICE,
                "name": "CNC Machine",
                "model_id": "dtmi:supplychaindt:CNCMachine;1",
                "properties": {
                    "status": "operational",
                    "efficiency": 89,
                    "location": "Factory Floor A"
                },
                "telemetry": {
                    "vibration": 2.3,
                    "temperature": 65.0,
                    "speed": 1200,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "AP89125-17",
                "type": TwinType.FACTORY,
                "name": "Assembly Plant",
                "model_id": "dtmi:supplychaindt:AssemblyPlant;1",
                "properties": {
                    "efficiency": 87,
                    "openorders": 45,
                    "reliability": 90,
                    "location": "South Asia"
                },
                "telemetry": {
                    "temperature": 26.0,
                    "humidity": 48.0,
                    "efficiency": 87.0,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "CV90372",
                "type": TwinType.DEVICE,
                "name": "Conveyor Belt",
                "model_id": "dtmi:supplychaindt:Conveyor;1",
                "properties": {
                    "status": "operational",
                    "speed": 1.5,
                    "location": "Warehouse A"
                },
                "telemetry": {
                    "speed": 1.5,
                    "items_count": 234,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "LT44901-22",
                "type": TwinType.SHIPMENT,
                "name": "Logistics Transport #1",
                "model_id": "dtmi:supplychaindt:Transport;1",
                "properties": {
                    "status": "in_transit",
                    "current_location": "Mumbai Port",
                    "destination": "Singapore",
                    "cargo_temp": 22.0
                },
                "telemetry": {
                    "latitude": 18.9220,
                    "longitude": 72.8347,
                    "speed": 45.0,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "QCS90447",
                "type": TwinType.DEVICE,
                "name": "Quality Check Station",
                "model_id": "dtmi:supplychaindt:QualityStation;1",
                "properties": {
                    "status": "operational",
                    "pass_rate": 97.5,
                    "location": "Assembly Line 3"
                },
                "telemetry": {
                    "pass_rate": 97.5,
                    "items_checked": 1024,
                    "timestamp": datetime.utcnow()
                }
            },
            {
                "id": "RMS90636",
                "type": TwinType.RETAILER,
                "name": "Raw Material Storage",
                "model_id": "dtmi:supplychaindt:Storage;1",
                "properties": {
                    "capacity": 10000,
                    "current_stock": 7850,
                    "location": "Warehouse B"
                },
                "telemetry": {
                    "temperature": 20.0,
                    "humidity": 40.0,
                    "stock_level": 7850,
                    "timestamp": datetime.utcnow()
                }
            }
        ]
        
        self._initialized = True
        self._use_mock = True
        logger.info(f"✅ Mock data initialized with {len(self._mock_twins)} twins")
    
    async def get_dashboard_twins(self) -> Dict[str, List[DigitalTwin]]:
        """Get all twins organized by type"""
        if not self._initialized:
            await self.initialize()
        
        try:
            if self._use_mock:
                # Using mock data
                factories = [t for t in self._mock_twins if t["type"] == TwinType.FACTORY]
                shipments = [t for t in self._mock_twins if t["type"] == TwinType.SHIPMENT]
                retailers = [t for t in self._mock_twins if t["type"] == TwinType.RETAILER]
                
                return {
                    "factories": [self._parse_twin_data(f) for f in factories],
                    "shipments": [self._parse_twin_data(s) for s in shipments],
                    "retailers": [self._parse_twin_data(r) for r in retailers]
                }
            else:
                # Using real Azure Digital Twins
                factory_query = "SELECT * FROM digitaltwins WHERE IS_OF_MODEL('dtmi:supplychaindt:Factory;1')"
                results = await asyncio.to_thread(
                    lambda: list(self.client.query_twins(factory_query))
                )
                # Parse and return real data
                return {"factories": results, "shipments": [], "retailers": []}
                
        except Exception as e:
            logger.error(f"Error retrieving dashboard twins: {str(e)}")
            raise
    
    async def get_twin(self, twin_id: str) -> Optional[DigitalTwin]:
        """Get single twin by ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            if self._use_mock:
                # Search mock data
                for twin_data in self._mock_twins:
                    if twin_data["id"] == twin_id:
                        return self._parse_twin_data(twin_data)
                return None
            else:
                # Query real Azure
                twin = await asyncio.to_thread(
                    self.client.get_digital_twin,
                    twin_id
                )
                return self._parse_twin_data(twin)
                
        except Exception as e:
            logger.error(f"Failed to get twin {twin_id}: {str(e)}")
            raise
    
    def _parse_twin_data(self, twin_data: dict) -> DigitalTwin:
        """Convert raw twin data into DigitalTwin model"""
        return DigitalTwin(
            id=twin_data["id"],
            type=twin_data["type"],
            name=twin_data["name"],
            model_id=twin_data["model_id"],
            properties=twin_data["properties"],
            telemetry=twin_data.get("telemetry")
        )

    async def query_twins(self, model_filter=None):
        try:
            if self._use_mock:
                return self._mock_twins

            query = f"SELECT * FROM DIGITALTWINS WHERE IS_OF_MODEL('{model_filter}')"
            results = self.client.query_twins(query)
            return list(results)

        except Exception as e:
            logger.error(f"ADT query failed: {e}")
            raise

# Create singleton instance
adt_service = ADTService()
