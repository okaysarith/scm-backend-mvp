# app/jobs/telemetry_sync_job.py
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from app.services.cosmos_service import CosmosService
from app.services.file_service import FileService
from app.services.telemetry_decoder import decode_iothub_message

logger = logging.getLogger(__name__)

class TelemetrySyncJob:
    def __init__(self, interval: int = 300):  # 5 minutes by default
        self.interval = interval
        self._is_running = False
        self.cosmos_service = CosmosService()
        self.file_service = FileService()
        
    async def run(self):
        """Main job loop"""
        self._is_running = True
        logger.info("Starting telemetry sync job")
        
        while self._is_running:
            try:
                await self.sync_telemetry()
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error in telemetry sync: {e}")
                await asyncio.sleep(60)  # Wait a minute before retry
                
    async def sync_telemetry(self):
        """Sync telemetry from source to destination"""
        # 1. Fetch raw telemetry
        raw_items = self.cosmos_service.fetch_raw_telemetry()
        
        # 2. Decode telemetry
        decoded_items = []
        for item in raw_items:
            try:
                decoded = decode_iothub_message(item)
                decoded_items.extend(decoded)
            except Exception as e:
                logger.error(f"Error decoding item {item.get('id')}: {e}")
        
        if not decoded_items:
            logger.warning("No telemetry items to process")
            return
            
        # 3. Save to Cosmos DB
        self.cosmos_service.save_decoded_telemetry(decoded_items)
        
        # 4. Save to local file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.file_service.save_json(decoded_items, f"telemetry_{timestamp}")
        
        logger.info(f"Synced {len(decoded_items)} telemetry items")
        
    def stop(self):
        """Stop the sync job"""
        self._is_running = False
        logger.info("Stopped telemetry sync job")