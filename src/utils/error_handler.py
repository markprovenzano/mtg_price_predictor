# src/utils/error_handler.py
import json
import os
from datetime import datetime
from .logger import logger  # Relative import from same directory

# Get project root directory (two levels up from src/utils/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
ERROR_LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "error_log.json")

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

if __name__ == "__main__":
    try:
        raise ValueError("Test error from error_handler.py")
    except ValueError as e:
        log_error(e, "Testing error logging")  # For direct testing