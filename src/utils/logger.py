# File: src/utils/logger.py
import logging
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def setup_logger(log_file="logs/info_log.txt"):
    """
    Initialize a logger for general info and debug messages.
    Args:
        log_file (str): Path to the info log file.
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Configure logger
    logger = logging.getLogger("mtg_price_predictor")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        # File handler for info logs
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)

        # Console handler for real-time output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(console_handler)

    return logger


def log_error(error_data, error_file="logs/error_log.json"):
    """
    Log error details to a JSON file.
    Args:
        error_data (dict): Dictionary containing error details (e.g., module, error message).
        error_file (str): Path to the error log JSON file.
    """
    error_data["timestamp"] = datetime.now().isoformat()

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(error_file), exist_ok=True)

    # Append error to JSON file
    try:
        if os.path.exists(error_file):
            with open(error_file, "r") as f:
                errors = json.load(f)
        else:
            errors = []
    except json.JSONDecodeError:
        errors = []

    errors.append(error_data)
    with open(error_file, "w") as f:
        json.dump(errors, f, indent=2)


def update_project_status(status_data, file_path="project_status.json"):
    """
    Update project_status.json with current project state.
    Args:
        status_data (dict): Project status data to write.
        file_path (str): Path to project_status.json.
    """
    status_data["timestamp"] = datetime.now().isoformat()
    with open(file_path, "w") as f:
        json.dump(status_data, f, indent=2)


if __name__ == "__main__":
    # Test logger
    logger = setup_logger()
    logger.info("Logger initialized successfully")

    # Test error logging
    test_error = {
        "module": "test",
        "error": "Sample error for testing"
    }
    log_error(test_error)

    # Test project status update
    test_status = {
        "project_name": "mtg_price_predictor",
        "version": "0.1.0",
        "status": {"completed_modules": ["logger"]},
        "issues": []
    }
    update_project_status(test_status)