import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from pathlib import Path

from app.services.cosmos_service import CosmosService
from app.services.file_service import FileService
from app.services.telemetry_decoder import decode_iothub_message

logger = logging.getLogger(__name__)

class BackgroundDecoderJob:
    def __init__(self, interval: int = 30):
        self.interval = interval
        self.is_running = False
        self._task = None
        self.cosmos_service = None
        self.file_service = None
        self._last_processed_time = datetime.utcnow().isoformat()  # Initialize with current time
        self._last_file_creation_time = None  # Track last file creation time

    def _should_create_file(self) -> bool:
        """Check if a new file should be created (only if no file created in last 4 hours)"""
        if self._last_file_creation_time is None:
            # Check for existing telemetry files in data directory
            data_dir = Path('data')
            if data_dir.exists():
                telemetry_files = list(data_dir.glob('telemetry_export_*.json'))
                if telemetry_files:
                    # Get the most recent file modification time
                    latest_file = max(telemetry_files, key=lambda f: f.stat().st_mtime)
                    latest_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
                    if datetime.utcnow() - latest_time < timedelta(hours=4):
                        self._last_file_creation_time = latest_time
                        logger.info(f"ℹ️  Recent file found: {latest_file.name}, skipping new file creation")
                        return False
            return True
        
        # Check if 4 hours have passed since last file creation
        time_since_last = datetime.utcnow() - self._last_file_creation_time
        return time_since_last >= timedelta(hours=4)

    async def start(self):
        if self.is_running:
            logger.warning("Background job is already running")
            return
        
        try:
            self.cosmos_service = CosmosService()
            self.file_service = FileService()
            
            # Ensure data directory exists
            import os
            os.makedirs('data', exist_ok=True)
            logger.info("✅ Services initialized and data directory ready")
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
        
        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"🚀 Started background job (interval: {self.interval}s)")

    async def stop(self):
        if not self.is_running:
            return
        
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Stopped background job")

    async def _run_loop(self):
        while self.is_running:
            try:
                await self._process_telemetry()
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background job error: {e}", exc_info=True)
                await asyncio.sleep(min(60, self.interval * 2))

    async def _process_telemetry(self):
        try:
            logger.info("📥 Fetching raw telemetry...")
            
            # Convert timestamp to string if it's a datetime
            since = self._last_processed_time
            
            raw_items = self.cosmos_service.fetch_raw_telemetry(since=since)
            
            if not raw_items:
                logger.info("ℹ️  No new telemetry to process")
                return
            
            logger.info(f"✅ Fetched {len(raw_items)} records")
            decoded_items = []
            
            # Update last processed time to the latest timestamp
            if raw_items and '_ts' in raw_items[0]:
                self._last_processed_time = str(raw_items[0]['_ts'])
                logger.info(f"⏱️  Updated last processed time to: {self._last_processed_time}")
            
            for item in raw_items:
                try:
                    decoded = decode_iothub_message(item)
                    decoded_items.extend(decoded)
                except Exception as e:
                    logger.error(f"Error decoding item: {e}")
            
            if decoded_items:
                # Save to Cosmos DB
                try:
                    self.cosmos_service.save_decoded_telemetry(decoded_items)
                    logger.info(f"💾 Saved {len(decoded_items)} items to Cosmos DB")
                except Exception as e:
                    logger.error(f"Error saving to Cosmos DB: {e}")
                
                # Save to local file (optional)
                if hasattr(self, 'file_service') and self.file_service:
                    if self._should_create_file():
                        try:
                            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                            file_path = self.file_service.save_json(
                                decoded_items, 
                                f"telemetry_export_{timestamp}"
                            )
                            self._last_file_creation_time = datetime.utcnow()
                            logger.info(f"💾 Saved {len(decoded_items)} items to {file_path}")
                        except Exception as e:
                            logger.error(f"Error saving to local file: {e}")
                    else:
                        logger.info("ℹ️  Skipping file creation (recent file exists within 4-hour window)")
                else:
                    logger.debug("Skipping local file save (file service not available)")
            
        except Exception as e:
            logger.error(f"Processing error: {e}", exc_info=True)
            # Don't update last_processed_time on error to avoid missing data

# Singleton instance
background_job = BackgroundDecoderJob(interval=30)

async def start_background_job(interval: int = 30):
    global background_job
    background_job.interval = interval
    await background_job.start()

async def stop_background_job():
    global background_job
    await background_job.stop()
