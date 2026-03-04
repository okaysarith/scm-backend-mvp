import base64
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

def decode_iothub_message(cosmos_item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Decode IoT Hub message from Cosmos DB item.
    
    Args:
        cosmos_item: Raw document from Cosmos DB telemetry container
        
    Returns:
        List of normalized telemetry documents (one per metric)
    """
    try:
        # Get the base64-encoded body
        body_base64 = cosmos_item.get("Body")
        if not body_base64:
            logger.warning("No 'Body' field found in Cosmos item")
            return []
            
        # Decode base64 to string
        try:
            body_str = base64.b64decode(body_base64).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to decode base64 body: {e}")
            return []
            
        # Parse JSON payload
        try:
            payload = json.loads(body_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON payload: {e}")
            return []
        
        # Extract device ID and metadata
        device_id = payload.get("device_id", "unknown")
        telemetry = payload.get("telemetry", {})
        metadata = payload.get("metadata", {})
        
        # Get timestamp from metadata or use current time
        timestamp = metadata.get("edge_timestamp") or datetime.utcnow().isoformat()
        
        # Get IoT Hub metadata
        system_props = cosmos_item.get("SystemProperties", {})
        
        # Create one document per metric
        docs = []
        for metric, value in telemetry.items():
            doc = {
                "id": f"{device_id}_{metric}_{cosmos_item.get('_ts', '')}",
                "deviceId": device_id,
                "metric": metric,
                "value": value,
                "status": metadata.get("operational_status", "unknown"),
                "timestamp": timestamp,
                "machineId": metadata.get("machine_id"),
                "source": device_id.lower().split('_')[0] if device_id else "unknown",
                "ingestionTime": system_props.get("iothub-enqueuedtime"),
                "originalTimestamp": cosmos_item.get("_ts")
            }
            docs.append(doc)
            
        logger.debug(f"Decoded {len(docs)} metrics for device {device_id}")
        return docs
        
    except Exception as e:
        logger.error(f"Error processing telemetry: {e}", exc_info=True)
        return []