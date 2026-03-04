# app/services/cosmos_service.py
from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
from azure.core.exceptions import AzureError
from typing import List, Dict, Any, Optional
import json
from pathlib import Path
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class CosmosService:
    def __init__(self):
        """Initialize Cosmos DB client with environment variables"""
        try:
            # Get connection string from environment
            self.connection_string = os.getenv("COSMOS_CONNECTION_STRING")
            if not self.connection_string:
                error_msg = "COSMOS_CONNECTION_STRING environment variable is not set"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Get database and container names with defaults
            self.database_name = os.getenv("COSMOS_DATABASE_NAME", "scm-iot-twins")
            self.raw_container_name = os.getenv("COSMOS_RAW_CONTAINER", "telemetry")
            self.decoded_container_name = os.getenv("COSMOS_DECODED_CONTAINER", "decoded_telemetry")
            
            # Initialize Cosmos client
            logger.info(" Initializing Cosmos DB client...")
            self.client = CosmosClient.from_connection_string(self.connection_string)
            
            # Get database and container clients
            self.database = self.client.get_database_client(self.database_name)
            self.raw_container = self.database.get_container_client(self.raw_container_name)
            self.decoded_container = self.database.get_container_client(self.decoded_container_name)
            
            logger.info(f"Connected to Cosmos DB: {self.database_name}")
            logger.info(f"Containers: {self.raw_container_name}, {self.decoded_container_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize CosmosService: {str(e)}")
            raise

    def fetch_raw_telemetry(self, since: str = None) -> List[Dict[str, Any]]:
        """Fetch raw telemetry data from Cosmos DB"""
        try:
            query = "SELECT * FROM c"
            parameters = []
            
            if since:
                # Handle both string and numeric timestamps
                query += " WHERE (IS_DEFINED(c._ts) AND (IS_NUMBER(c._ts) AND c._ts > @since_num OR IS_STRING(c._ts) AND c._ts > @since_str))"
                parameters.extend([
                    {"name": "@since_num", "value": float(since) if since.replace('.', '', 1).isdigit() else 0},
                    {"name": "@since_str", "value": since}
                ])
                
            query += " ORDER BY c._ts DESC"
            
            items = list(self.raw_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            logger.info(f"Fetched {len(items)} raw telemetry records")
            return items
        except Exception as e:
            logger.error(f"Error fetching telemetry: {e}")
            raise

    def save_decoded_telemetry(self, items: List[Dict[str, Any]]) -> None:
        """Save decoded telemetry to Cosmos DB"""
        container = self.database.get_container_client("decoded_telemetry")
        for item in items:
            try:
                container.upsert_item(item)
            except Exception as e:
                logger.error(f"Error saving item {item.get('id')}: {e}")

    def get_decoded_telemetry(self, query: str = "SELECT * FROM c") -> List[Dict[str, Any]]:
        """Fetch decoded telemetry from Cosmos DB"""
        container = self.database.get_container_client("decoded_telemetry")
        try:
            return list(container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
        except Exception as e:
            logger.error(f"Error fetching decoded telemetry: {e}")
            raise