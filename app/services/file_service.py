from typing import List,Dict,Any
from pathlib import Path
import json
import logging
logger = logging.getLogger(__name__)
class FileService:
    def __init__(self,storage_dir:str = "data"):
        self.storage_dir = Path(storage_dir)
    
    def save_json(self,data:List[Dict[str,Any]],filename:str):
        """Save as json file"""
        filepath = self.storage_dir / f"{filename}.json"

        with open(filepath,"w") as f:
            json.dump(data,f , indent = 2 ,default = str)
            logger.info(f"Saved {len(data)} items items to {filepath}")
            return str(filepath)

    def read_json(self,filename:str) -> List[Dict[str,Any]]:
        """Read data from a JSON file"""
        filepath = self.storage_dir / f"{filename}.json"
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File {filepath} not found")
            return []

