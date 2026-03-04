import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def update_latest_telemetry(source_file: Path):
    """
    Copy the latest telemetry export to a stable filename
    
    Args:
        source_file: Path to the source telemetry file to copy
    """
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    target = data_dir / "telemetry_export_latest.json"
    try:
        shutil.copy2(source_file, target)
        logger.info(f"Updated {target} with {source_file}")
        return True
    except Exception as e:
        logger.error(f"Error updating telemetry file: {e}")
        return False
