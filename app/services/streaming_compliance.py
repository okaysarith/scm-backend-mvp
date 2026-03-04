"""
Streaming Compliance Service
Processes orders in real-time with continuous compliance checking
"""

import asyncio
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json
import time
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class StreamingComplianceService:
    """Real-time compliance processing with streaming"""
    
    def __init__(self, network_design_service):
        self.network_service = network_design_service
        self.compliance_cache = {}
        self.order_queue = asyncio.Queue()
        self.processing = False
        self.stats = {
            'total_processed': 0,
            'compliant_count': 0,
            'non_compliant_count': 0,
            'start_time': None,
            'last_update': None
        }
    
    async def start_streaming(self):
        """Start the streaming compliance processor"""
        if self.processing:
            logger.warning("Streaming already active")
            return
        
        self.processing = True
        self.stats['start_time'] = datetime.now()
        logger.info("🚀 Starting streaming compliance processor")
        
        # Start background processing task
        asyncio.create_task(self._process_orders_stream())
    
    async def stop_streaming(self):
        """Stop the streaming processor"""
        self.processing = False
        logger.info("🛑 Stopping streaming compliance processor")
    
    async def add_order(self, order_data: Dict):
        """Add order to processing queue"""
        await self.order_queue.put(order_data)
    
    async def add_orders_batch(self, orders: List[Dict]):
        """Add multiple orders to queue"""
        for order in orders:
            await self.order_queue.put(order)
    
    async def _process_orders_stream(self):
        """Background task to process orders continuously"""
        batch_size = 100  # Process in batches of 100
        batch = []
        
        while self.processing:
            try:
                # Collect batch of orders
                try:
                    order = await asyncio.wait_for(self.order_queue.get(), timeout=1.0)
                    batch.append(order)
                except asyncio.TimeoutError:
                    # Process any pending batch
                    if batch:
                        await self._process_batch(batch)
                        batch = []
                    continue
                
                # Process when batch is full
                if len(batch) >= batch_size:
                    await self._process_batch(batch)
                    batch = []
                    
            except Exception as e:
                logger.error(f"Error in streaming processor: {e}")
                await asyncio.sleep(1)
        
        # Process remaining orders
        if batch:
            await self._process_batch(batch)
    
    async def _process_batch(self, orders: List[Dict]):
        """Process a batch of orders for compliance"""
        try:
            start_time = time.time()
            
            # Convert to DataFrame for processing
            df = pd.DataFrame(orders)
            
            # Standardize columns
            if 'pincode' in df.columns:
                df['pincode'] = df['pincode'].astype(str).str.strip()
            if 'hub_pincode' in df.columns:
                df['hub_pincode'] = df['hub_pincode'].astype(str).str.strip()
            
            # Get pincode to hub mapping
            pincode_to_hub = {}
            for pincode, hub_code in self.network_service.pincode_hub_mapping.items():
                pincode_to_hub[str(pincode)] = str(hub_code)
            
            # Add expected hub column
            if 'pincode' in df.columns:
                df['expected_hub'] = df['pincode'].map(pincode_to_hub)
            
            # Compliance check
            if 'hub_pincode' in df.columns and 'expected_hub' in df.columns:
                df['is_compliant'] = df['hub_pincode'] == df['expected_hub']
                
                # Update statistics
                compliant_count = df['is_compliant'].sum()
                non_compliant_count = len(df) - compliant_count
                
                self.stats['total_processed'] += len(df)
                self.stats['compliant_count'] += compliant_count
                self.stats['non_compliant_count'] += non_compliant_count
                self.stats['last_update'] = datetime.now()
                
                # Cache results
                for _, row in df.iterrows():
                    order_no = row.get('order_no', 'unknown')
                    self.compliance_cache[order_no] = {
                        'is_compliant': bool(row.get('is_compliant', False)),
                        'pincode': str(row.get('pincode', '')),
                        'hub_pincode': str(row.get('hub_pincode', '')),
                        'expected_hub': str(row.get('expected_hub', '')),
                        'processed_at': datetime.now().isoformat()
                    }
                
                processing_time = time.time() - start_time
                logger.info(f"📊 Processed {len(df)} orders in {processing_time:.3f}s "
                          f"(Compliant: {compliant_count}, Non-compliant: {non_compliant_count})")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
    
    def get_compliance_stats(self) -> Dict:
        """Get current compliance statistics"""
        total = self.stats['total_processed']
        compliant = self.stats['compliant_count']
        non_compliant = self.stats['non_compliant_count']
        
        compliance_rate = (compliant / total * 100) if total > 0 else 0
        
        return {
            'total_processed': total,
            'compliant_count': compliant,
            'non_compliant_count': non_compliant,
            'compliance_rate': compliance_rate,
            'processing_active': self.processing,
            'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'last_update': self.stats['last_update'].isoformat() if self.stats['last_update'] else None,
            'cache_size': len(self.compliance_cache)
        }
    
    def get_order_compliance(self, order_no: str) -> Optional[Dict]:
        """Get compliance status for specific order"""
        return self.compliance_cache.get(order_no)
    
    async def load_existing_data_streaming(self, order_data_path: str, pick_data_path: str):
        """Load existing data in streaming fashion"""
        logger.info("🔄 Loading existing data in streaming mode...")
        
        try:
            # Load data in chunks
            chunk_size = 10000
            order_chunks = pd.read_csv(order_data_path, chunksize=chunk_size)
            pick_chunks = pd.read_csv(pick_data_path, chunksize=chunk_size)
            
            # Process first chunk to get structure
            first_order_chunk = next(order_chunks)
            first_pick_chunk = next(pick_chunks)
            
            # Standardize columns
            first_order_chunk.columns = first_order_chunk.columns.str.lower().str.replace(' ', '_')
            first_pick_chunk.columns = first_pick_chunk.columns.str.lower().str.replace(' ', '_')
            
            # Merge first chunk
            merged_chunk = first_order_chunk.merge(
                first_pick_chunk[['order_no', 'hub_pincode']], 
                on='order_no', 
                how='inner'
            )
            
            # Convert to dict and add to queue
            orders_to_process = merged_chunk.to_dict('records')
            await self.add_orders_batch(orders_to_process)
            
            logger.info(f"📊 Loaded first chunk: {len(orders_to_process)} orders")
            
            # Continue with remaining chunks
            chunk_count = 1
            for order_chunk in order_chunks:
                try:
                    pick_chunk = next(pick_chunks)
                    
                    # Standardize and merge
                    order_chunk.columns = order_chunk.columns.str.lower().str.replace(' ', '_')
                    pick_chunk.columns = pick_chunk.columns.str.lower().str.replace(' ', '_')
                    
                    merged_chunk = order_chunk.merge(
                        pick_chunk[['order_no', 'hub_pincode']], 
                        on='order_no', 
                        how='inner'
                    )
                    
                    # Add to queue
                    orders_to_process = merged_chunk.to_dict('records')
                    await self.add_orders_batch(orders_to_process)
                    
                    chunk_count += 1
                    if chunk_count % 10 == 0:
                        logger.info(f"📊 Processed {chunk_count} chunks ({chunk_count * chunk_size} orders)")
                        
                except StopIteration:
                    break
                
            logger.info(f"✅ Completed loading {chunk_count} chunks")
            
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
            raise

# Global streaming service instance
streaming_service = None

def get_streaming_service(network_design_service) -> StreamingComplianceService:
    """Get or create streaming service instance"""
    global streaming_service
    if streaming_service is None:
        streaming_service = StreamingComplianceService(network_design_service)
    return streaming_service
