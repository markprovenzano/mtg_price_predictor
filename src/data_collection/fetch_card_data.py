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

def fetch_card_data(card_list_path: str = None, card_attributes_path: str = None):
    """
    Load card data from card_list.csv and card_attributes.csv, merge on card_sku_id, and save as JSON to data/raw/.

    Args:
        card_list_path (str): Path to card_list CSV (default: data/raw/card_list.csv).
        card_attributes_path (str): Path to card_attributes CSV (default: data/raw/card_attributes.csv).

    Returns:
        pd.DataFrame: Merged DataFrame with card data from card_list.csv and attributes, or None if an error occurs.
    """
    try:
        # Set default paths
        if card_list_path is None:
            card_list_path = os.path.join(RAW_DATA_DIR, "card_list.csv")
        if card_attributes_path is None:
            card_attributes_path = os.path.join(RAW_DATA_DIR, "card_attributes.csv")

        # Load card_list.csv
        logger.info(f"Loading card data from {card_list_path}")
        if not os.path.exists(card_list_path):
            logger.error(f"Card list CSV not found: {card_list_path}")
            log_error(FileNotFoundError(f"Card list CSV not found: {card_list_path}"), "Loading card_list data")
            return None
        card_list_df = pd.read_csv(card_list_path)
        if "card_sku_id" not in card_list_df.columns:
            logger.error("Card list CSV missing card_sku_id column")
            log_error(ValueError("Card list CSV missing card_sku_id column"), "Loading card_list data")
            return None
        logger.info(f"Loaded {len(card_list_df)} cards from {card_list_path}")

        # Load card_attributes.csv
        logger.info(f"Loading card attributes from {card_attributes_path}")
        if not os.path.exists(card_attributes_path):
            logger.error(f"Card attributes CSV not found: {card_attributes_path}")
            log_error(FileNotFoundError(f"Card attributes CSV not found: {card_attributes_path}"), "Loading card_attributes data")
            return None
        card_attributes_df = pd.read_csv(card_attributes_path)
        if "card_sku_id" not in card_attributes_df.columns:
            logger.error("Card attributes CSV missing card_sku_id column")
            log_error(ValueError("Card attributes CSV missing card_sku_id column"), "Loading card_attributes data")
            return None
        logger.info(f"Loaded {len(card_attributes_df)} card attributes from {card_attributes_path}")

        # Merge DataFrames (left join to keep only card_list.csv card_sku_id)
        merged_df = card_list_df[["card_sku_id"]].merge(card_attributes_df, on="card_sku_id", how="left")
        logger.info(f"Merged data contains {len(merged_df)} cards with attributes")

        # Save as JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(RAW_DATA_DIR, f"card_data_{timestamp}.json")
        with open(output_file, "w") as f:
            json.dump(merged_df.to_dict(orient="records"), f, indent=2)
        logger.info(f"Saved {len(merged_df)} cards to {output_file}")

        logger.info("Card data processing completed successfully")
        return merged_df

    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty or invalid CSV file: {e}")
        log_error(e, "Loading card data")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in fetch_card_data: {e}")
        log_error(e, "Unexpected error in fetch_card_data")
        return None

if __name__ == "__main__":
    fetch_card_data()