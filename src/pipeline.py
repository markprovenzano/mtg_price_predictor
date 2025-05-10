# src/pipeline.py
import argparse
import logging
import os
import pandas as pd
from datetime import datetime
from src.data_collection.fetch_market_data import fetch_market_data
from src.data_preprocessing.merge_data import merge_data

# Project constants
PROJECT_ROOT = r"C:\Users\mprov\PycharmProjects\mtg_price_predictor"
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log"))
    ]
)
logger = logging.getLogger(__name__)

def load_card_attributes():
    """Load card_attributes from CSV."""
    csv_path = os.path.join(PROJECT_ROOT, "data", "raw", "card_attributes.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Card attributes CSV not found at {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} card_attributes records")
    return df

def run_pipeline(fetch_data=True, run_merge_data=True):
    """Run the data pipeline with optional module switches."""
    logger.info("Starting pipeline execution")

    # Step 1: Fetch market data
    market_data = None
    if fetch_data:
        logger.info("Fetching market data")
        try:
            market_data = fetch_market_data()
            logger.info(f"Fetched market data: {len(market_data.get('market_prices', []))} market_prices, "
                        f"{len(market_data.get('sales_history', []))} sales_history, "
                        f"{len(market_data.get('listings', []))} listings")
        except Exception as e:
            logger.error(f"Failed to fetch market data: {str(e)}")
            raise

    # Step 2: Load card attributes
    card_attributes = None
    if run_merge_data:
        logger.info("Loading card attributes")
        try:
            card_attributes = load_card_attributes()
        except Exception as e:
            logger.error(f"Failed to load card attributes: {str(e)}")
            raise

    # Step 3: Merge data
    merged_data = None
    if run_merge_data:
        logger.info("Running merge_data")
        try:
            merged_data, stats = merge_data(market_data, card_attributes)
            logger.info(f"Merged data size: {len(merged_data)}")
            logger.info(f"Merge stats: {stats}")

            # Save merged data
            processed_dir = os.path.join(PROJECT_ROOT, "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            merged_data_path = os.path.join(processed_dir, f"merged_data_{timestamp}.csv")
            merged_data.to_csv(merged_data_path, index=False)
            logger.info(f"Saved merged data to {merged_data_path}")
        except Exception as e:
            logger.error(f"Failed to merge data: {str(e)}")
            raise

    # Placeholder for future steps (e.g., clean_data, feature_engineering)
    logger.info("Pipeline execution completed")
    return merged_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MTG Price Predictor Pipeline")
    parser.add_argument("--fetch-data", action="store_true", default=True, help="Run fetch_market_data")
    parser.add_argument("--merge-data", action="store_true", default=True, help="Run merge_data")
    args = parser.parse_args()

    try:
        run_pipeline(fetch_data=args.fetch_data, run_merge_data=args.merge_data)
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise