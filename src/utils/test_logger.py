# test_logger.py
from logger import logger
from error_handler import log_error

def test_logging():
    logger.info("Testing INFO log for mtg_price_predictor")
    try:
        raise ValueError("Simulated error for testing")
    except ValueError as e:
        logger.error(f"Testing ERROR log: {e}")
        log_error(e, "Testing error logging in test_logger.py")

if __name__ == "__main__":
    test_logging()