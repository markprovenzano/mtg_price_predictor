# error_handler.py
import json
import os
from datetime import datetime  # Correct import
from src.utils.logger import logger

ERROR_LOG_FILE = os.path.join("logs", "error_log.json")

def log_error(error: Exception, context: str):
    """Log errors to a JSON file."""
    error_entry = {
        "timestamp": datetime.now().isoformat(),
        "context": context,
        "error": str(error)
    }
    try:
        with open(ERROR_LOG_FILE, "a") as f:
            json.dump(error_entry, f)
            f.write("\n")
        logger.info(f"Error logged to {ERROR_LOG_FILE}: {context}")
    except Exception as e:
        logger.error(f"Failed to log error: {e}")