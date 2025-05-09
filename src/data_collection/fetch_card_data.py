# src/data_collection/fetch_card_data.py
import pandas as pd
import json
import os
from datetime import datetime
from src.utils.logger import logger
from src.utils.error_handler import log_error

# Get project root directory (two levels up from src/data_collection/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def fetch_card_data(csv_path: str = None):
    """
    Load card data from a CSV file (e.g., card_list.csv) and save as JSON to data/raw/.

    Args:
        csv_path (str): Path to CSV file (default: data/raw/card_list.csv).

    Returns:
        bool: True if successful, False if an error occurs.
    """
    try:
        if csv_path is None:
            csv_path = os.path.join(RAW_DATA_DIR, "card_list.csv")

        logger.info(f"Loading card data from {csv_path}")
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            log_error(FileNotFoundError(f"CSV file not found: {csv_path}"), "Loading card data from CSV")
            return False

        # Load CSV
        df = pd.read_csv(csv_path)
        cards = df.to_dict(orient="records")
        logger.info(f"Loaded {len(cards)} cards from {csv_path}")

        # Save as JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(RAW_DATA_DIR, f"cards_{timestamp}.json")
        with open(output_file, "w") as f:
            json.dump(cards, f, indent=2)
        logger.info(f"Saved {len(cards)} cards to {output_file}")

        logger.info("Card data processing completed successfully")
        return True

    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty or invalid CSV file: {e}")
        log_error(e, "Loading card data from CSV")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in fetch_card_data: {e}")
        log_error(e, "Unexpected error in fetch_card_data")
        return False

if __name__ == "__main__":
    fetch_card_data()