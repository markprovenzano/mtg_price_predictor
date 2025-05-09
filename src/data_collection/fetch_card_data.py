# src/data/fetch_card_data.py
import requests
import json
import os
from datetime import datetime
from src.utils.logger import logger
from src.utils.error_handler import log_error

# Get project root directory (two levels up from src/data/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)


def fetch_card_data(api_endpoint: str = "https://api.scryfall.com/cards"):
    """
    Fetch card data from the Scryfall API and save to data/raw/.

    Args:
        api_endpoint (str): Scryfall API endpoint for card data.

    Returns:
        bool: True if successful, False if an error occurs.
    """
    try:
        logger.info(f"Starting card data fetch from {api_endpoint}")
        page = 1
        while True:
            # Construct URL with pagination
            url = f"{api_endpoint}?page={page}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise exception for HTTP errors

            data = response.json()
            cards = data.get("data", [])
            if not cards:
                logger.info("No more card data to fetch")
                break

            # Save cards to a JSON file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(RAW_DATA_DIR, f"cards_page_{page}_{timestamp}.json")
            with open(output_file, "w") as f:
                json.dump(cards, f, indent=2)
            logger.info(f"Saved {len(cards)} cards to {output_file}")

            # Check for next page
            if not data.get("has_more", False):
                logger.info("Reached end of card data")
                break

            page += 1

        logger.info("Card data fetch completed successfully")
        return True

    except requests.RequestException as e:
        logger.error(f"Failed to fetch card data: {e}")
        log_error(e, "Fetching card data from Scryfall API")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in fetch_card_data: {e}")
        log_error(e, "Unexpected error in fetch_card_data")
        return False


if __name__ == "__main__":
    fetch_card_data()