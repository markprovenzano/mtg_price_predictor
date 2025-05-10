# src/data_preprocessing/clean_data.py
import pandas as pd
import os
from datetime import datetime
from src.utils.logger import logger
from src.utils.error_handler import log_error

# Get project root directory (two levels up from src/data_preprocessing/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def clean_data(card_df: pd.DataFrame, market_dfs: dict):
    """
    Clean and merge card and market DataFrames, normalizing timestamps, and save to data/processed/.

    Args:
        card_df (pd.DataFrame): DataFrame with card_sku_id from fetch_card_data.
        market_dfs (dict): Dictionary of DataFrames (market_prices, sales_history, listings) from fetch_market_data.

    Returns:
        bool: True if successful, False if an error occurs.
    """
    try:
        logger.info("Starting data cleaning with card and market DataFrames")

        # Validate inputs
        if not isinstance(card_df, pd.DataFrame) or "card_sku_id" not in card_df.columns:
            logger.error("Invalid card DataFrame: missing card_sku_id")
            log_error(ValueError("Invalid card DataFrame: missing card_sku_id"), "Cleaning card data")
            return False
        if not isinstance(market_dfs, dict) or not all(key in market_dfs for key in ["market_prices", "sales_history", "listings"]):
            logger.error("Invalid market DataFrames: missing required tables")
            log_error(ValueError("Invalid market DataFrames: missing required tables"), "Cleaning market data")
            return False

        # Normalize timestamps
        processed_dfs = {}
        for table, df in market_dfs.items():
            if not isinstance(df, pd.DataFrame) or "card_sku_id" not in df.columns:
                logger.error(f"Invalid DataFrame for {table}: missing card_sku_id")
                log_error(ValueError(f"Invalid DataFrame for {table}: missing card_sku_id"), f"Cleaning {table} data")
                return False
            if table == "sales_history":
                # Aggregate sales_history to daily (sum quantity, average price)
                df["date"] = pd.to_datetime(df["order_date"]).dt.date
                df = df.groupby(["card_sku_id", "date"]).agg({
                    "quantity": "sum",
                    "price": "mean",
                    "id": "count"  # Count of sales
                }).reset_index()
                df = df.rename(columns={"id": "sales_count"})
                logger.info(f"Aggregated sales_history to {len(df)} daily records")
            else:
                # Extract date from updated_at
                df["date"] = pd.to_datetime(df["updated_at"]).dt.date
                logger.info(f"Processed {table} with {len(df)} records")
            processed_dfs[table] = df

        # Merge DataFrames
        merged_df = card_df[["card_sku_id"]].copy()
        for table, df in processed_dfs.items():
            suffix = f"_{table}"
            merge_key = ["card_sku_id", "date"] if table != "market_prices" else ["card_sku_id"]
            merged_df = merged_df.merge(df, on=merge_key, how="left", suffixes=("", suffix))
            logger.info(f"Merged {table} with {len(df)} records")

        # Clean data (remove rows with all nulls except card_sku_id)
        non_sku_columns = [col for col in merged_df.columns if col != "card_sku_id"]
        merged_df = merged_df.dropna(subset=non_sku_columns, how="all")
        logger.info(f"After cleaning, {len(merged_df)} rows remain")

        # Save to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(PROCESSED_DATA_DIR, f"cleaned_data_{timestamp}.json")
        merged_df.to_json(output_file, orient="records", indent=2)
        logger.info(f"Saved cleaned data to {output_file}")

        logger.info("Data cleaning completed successfully")
        return True

    except Exception as e:
        logger.error(f"Unexpected error in clean_data: {e}")
        log_error(e, "Cleaning data")
        return False

if __name__ == "__main__":
    # Example usage
    from src.data_collection.fetch_card_data import fetch_card_data
    from src.data_collection.fetch_market_data import fetch_market_data
    card_df = pd.DataFrame(fetch_card_data())
    market_dfs = fetch_market_data()
    clean_data(card_df, market_dfs)