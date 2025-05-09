# src/utils/test_logger.py
from .logger import logger  # Relative import from same directory
from .error_handler import log_error  # Relative import from same directory

def test_logging():
    """Test logging functionality for mtg_price_predictor."""
    logger.info("Testing INFO log for mtg_price_predictor")
    try:
        raise ValueError("Simulated error for testing")
    except ValueError as e:
        logger.error(f"Testing ERROR log: {e}")
        log_error(e, "Testing error logging in test_logger.py")

if __name__ == "__main__":
    test_logging()