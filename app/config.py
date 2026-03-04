# backend/app/config.py
"""Production-ready settings for Supply Chain Digital Twin Backend"""

import os
from typing import List, Dict
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(env_path)

class Settings:
    """Simple environment-based settings (no Pydantic overhead)"""
    
    # ===== Azure Digital Twins =====
    ADT_SERVICE_URL: str = os.getenv("ADT_SERVICE_URL", "https://supplychaindt.api.sea.digitaltwins.azure.net")
    AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")
    AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID", "")
    AZURE_CLIENT_SECRET: str = os.getenv("AZURE_CLIENT_SECRET", "")
    
    # ===== Azure SignalR =====
    AZURE_SIGNALR_CONNECTION_STRING: str = os.getenv("AZURE_SIGNALR_CONNECTION_STRING", "")
    
    # ===== IoT Hub Device Connections =====
    IOT_DEVICES: Dict[str, str] = {
        "actual_iot_sender": os.getenv("IOT_HUB_CONNECTION_actual_iot_sender", ""),
        "cnc_machine": os.getenv("IOT_HUB_CONNECTION_cnc_machine_sensor", ""),
        "after_sales": os.getenv("IOT_HUB_CONNECTION_after_sales", ""),
        "assembly_plant": os.getenv("IOT_HUB_CONNECTION_assembly_plant_sensor", ""),
        "consumer_use": os.getenv("IOT_HUB_CONNECTION_consumer_use_sensor", ""),
        "conveyor": os.getenv("IOT_HUB_CONNECTION_conveyor_sensor", ""),
        "logistics": os.getenv("IOT_HUB_CONNECTION_logistics_sensor", ""),
        "packaging": os.getenv("IOT_HUB_CONNECTION_packaging_sensor", ""),
        "quality_check": os.getenv("IOT_HUB_CONNECTION_quality_check_sensor", ""),
        "raw_material": os.getenv("IOT_HUB_CONNECTION_raw_material_sensor", ""),
        "retail_distribution": os.getenv("IOT_HUB_CONNECTION_retail_distribution_sensor", ""),
        "storage_room": os.getenv("IOT_HUB_CONNECTION_storage_room_sensor", ""),
    }
    
    # Extract IoT Hub hostname
    IOT_HUB_HOSTNAME: str = ""
    for cs in IOT_DEVICES.values():
        if cs and "HostName=" in cs:
            IOT_HUB_HOSTNAME = cs.split("HostName=")[1].split(";")[0]
            break
    
    # ===== Google AI =====
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")
    
    # ===== App Settings =====
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    MONITORING_INTERVAL_SECONDS: int = int(os.getenv("MONITORING_INTERVAL_SECONDS", "300"))
    ALERT_THRESHOLD_MULTIPLIER: float = float(os.getenv("ALERT_THRESHOLD_MULTIPLIER", "1.0"))
    
    # ===== CORS & Security =====
    CORS_ORIGINS: List[str] = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
        "https://iottodigitaltwins.azurewebsites.net",
    ]
    ALLOWED_HOSTS: List[str] = ["*"]
    
    # ===== Feature Flags =====
    USE_MOCK_DATA: bool = not bool(AZURE_CLIENT_ID and AZURE_CLIENT_SECRET)
    ENABLE_IOT_TELEMETRY: bool = bool(IOT_HUB_HOSTNAME)
    ENABLE_SIGNALR: bool = bool(AZURE_SIGNALR_CONNECTION_STRING)

# Create singleton instance
settings = Settings()
