# src/data_preprocessing/merge_data.py
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import logging
import time
from src.utils.error_handler import log_error  # Ensure error_handler.py exists

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
        logging.FileHandler(os.path.join(LOG_DIR, "merge_data.log"))
    ]
)
logger = logging.getLogger(__name__)

def merge_data(market_data: dict, card_attributes: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Perform granular merge of market_data and card_attributes with diagnostics.

    Args:
        market_data (dict): Dictionary containing market_prices, sales_history, and listings DataFrames.
        card_attributes (pd.DataFrame): DataFrame with card attributes.

    Returns:
        tuple: Merged DataFrame and statistics dictionary.
    """
    try:
        start_time = time.time()
        logger.info("Starting merge_data execution")

        # Validate inputs
        if not market_data or not isinstance(market_data, dict):
            raise ValueError("market_data is invalid or empty")
        if card_attributes is None or card_attributes.empty:
            raise ValueError("card_attributes is invalid or empty")

        market_prices = market_data.get("market_prices")
        sales_history = market_data.get("sales_history")
        listings = market_data.get("listings")

        if not all([market_prices is not None, sales_history is not None, listings is not None]):
            raise ValueError("Missing required market_data components")

        logger.info(
            f"Received input sizes: market_prices={len(market_prices)}, sales_history={len(sales_history)}, listings={len(listings)}, card_attributes={len(card_attributes)}")

        # Diagnostic: Raw NaN counts
        raw_stats = {
            "direct_low_raw_nans": int(market_prices["direct_low"].isna().sum()),
            "market_raw_nans": int(market_prices["market"].isna().sum()),
            "low_raw_nans": int(market_prices["low"].isna().sum())
        }
        logger.info(f"Raw NaN counts: {raw_stats}")

        # Filter card_attributes to relevant card_sku_id
        relevant_sku = set(market_prices["card_sku_id"]).union(set(sales_history["card_sku_id"]))
        card_attributes = card_attributes[card_attributes["card_sku_id"].isin(relevant_sku)]
        logger.info(f"Filtered card_attributes to {len(card_attributes)} relevant records")

        # Normalize dates to Eastern Time (ET) and convert to strings
        market_prices["date"] = pd.to_datetime(market_prices["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")
        listings["date"] = pd.to_datetime(listings["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")
        sales_history["date"] = pd.to_datetime(sales_history["order_date"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")

        # Aggregate sales_history by date
        sales_agg = sales_history.groupby(["card_sku_id", "date"]).agg({
            "quantity": "sum",
            "price": ["mean", "median", "count", "max"]
        }).reset_index()
        sales_agg.columns = ["card_sku_id", "date", "sales_quantity", "sales_price_mean", "sales_price_median", "sales_count", "sales_price_max"]

        # Create complete date range and combinations
        date_range = pd.date_range(start="2025-03-11", end="2025-05-09").strftime("%Y-%m-%d").tolist()
        all_combinations = pd.DataFrame(
            [(sku, d) for sku in relevant_sku for d in date_range],
            columns=["card_sku_id", "date"]
        )

        # Merge with market_prices and carry forward
        merged = all_combinations.merge(market_prices, on=["card_sku_id", "date"], how="left")
        merged = merged.sort_values("date")
        market_cols = market_prices.columns.difference(["card_sku_id", "date", "updated_at"])
        for col in market_cols:
            merged[col] = merged.groupby("card_sku_id")[col].transform(lambda x: x.ffill())

        # Merge with listings and handle missing days
        merged = merged.merge(listings, on=["card_sku_id", "date"], how="left", suffixes=("", "_listings"))
        merged["is_missing_day"] = merged["price"].isna()  # Flag missing listings data
        merged["quantity"] = merged["quantity"].fillna(0)
        merged["direct_inventory_count"] = merged["direct_inventory_count"].fillna(0)
        # Note: 'price' remains NaN for missing days

        # Merge with sales_agg
        merged = merged.merge(sales_agg, on=["card_sku_id", "date"], how="left")
        merged[["sales_quantity", "sales_price_mean", "sales_price_median", "sales_count", "sales_price_max"]] = merged[
            ["sales_quantity", "sales_price_mean", "sales_price_median", "sales_count", "sales_price_max"]
        ].fillna(0)

        # Merge with card_attributes
        merged = merged.merge(card_attributes, on="card_sku_id", how="left")
        logger.info(f"Merged data size: {len(merged)}")

        # Flag dropshipper out-of-stock
        merged["is_dropshipper_out_of_stock"] = merged["direct_low"].isna()

        # Flag low inventory
        merged["is_low_inventory"] = merged["direct_inventory_count"] <= 5

        # Flag extreme outliers for review
        merged["is_extreme_outlier"] = (merged["direct_low"] > 0) & (merged["sales_price_max"] > 100 * merged["direct_low"])

        # Flag card_sku_ids with all direct_low NaNs
        all_nan_skus = merged.groupby("card_sku_id")["direct_low"].apply(lambda x: x.isna().all())
        all_nan_skus = all_nan_skus[all_nan_skus].index
        merged["is_all_direct_low_nan"] = merged["card_sku_id"].isin(all_nan_skus)

        # Output extreme outliers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extreme_outliers = merged[merged["is_extreme_outlier"]][[
            "card_sku_id", "date", "sales_price_max", "direct_low", "set_name", "rarity", "condition"
        ]].copy()
        extreme_outliers["multiplier"] = extreme_outliers["sales_price_max"] / extreme_outliers["direct_low"]
        if not extreme_outliers.empty:
            outlier_path = os.path.join(LOG_DIR, f"extreme_outliers_{timestamp}.csv")
            extreme_outliers.to_csv(outlier_path, index=False)
            logger.info(f"Saved {len(extreme_outliers)} extreme outliers to {outlier_path}")
            logger.info(f"Unique card_sku_ids in outliers: {len(extreme_outliers['card_sku_id'].unique())}")

        # Output random sample
        random_sample = merged.sample(n=100, random_state=42)[[
            "card_sku_id", "date", "sales_price_max", "direct_low", "set_name", "rarity", "condition"
        ]]
        sample_path = os.path.join(LOG_DIR, f"random_sample_{timestamp}.csv")
        random_sample.to_csv(sample_path, index=False)
        logger.info(f"Saved 100 random samples to {sample_path}")

        # Outlier validation against direct_low, excluding zeros
        validation_stats = {
            "25x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 25 * merged["direct_low"])).sum()),
            "50x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 50 * merged["direct_low"])).sum()),
            "100x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 100 * merged["direct_low"])).sum())
        }
        logger.info(f"Outlier validation stats: {validation_stats}")

        # Statistics
        stats = {
            "record_counts": {
                "market_prices": len(market_prices),
                "sales_history": len(sales_history),
                "sales_history_filtered": len(sales_history),
                "listings": len(listings),
                "card_attributes": len(card_attributes),
                "merged": len(merged)
            },
            "nan_counts": {
                col: int(merged[col].isna().sum()) for col in [
                    "low", "market", "direct_low", "price", "quantity",
                    "direct_inventory_count", "sales_quantity", "sales_price_max"
                ] if col in merged.columns
            },
            "low_inventory": {
                "count": int(merged["is_low_inventory"].sum()),
                "proportion": float(merged["is_low_inventory"].mean())
            },
            "outlier_stats": {
                "validation": validation_stats
            },
            "extreme_outlier_count": int(merged["is_extreme_outlier"].sum()),
            "all_direct_low_nan_count": int(merged["is_all_direct_low_nan"].sum()),
            "direct_low_zero_count": int((merged["direct_low"] == 0).sum()),
            "non_zero_sales": int(merged["sales_price_max"].gt(0).sum()),
            "correlation": merged["sales_price_max"].corr(merged["direct_low"]) if "direct_low" in merged.columns else None,
            "raw_stats": raw_stats
        }

        # Save diagnostics
        diagnostic_path = os.path.join(LOG_DIR, f"merge_data_diagnostic_{timestamp}.json")
        with open(diagnostic_path, "w") as f:
            json.dump(stats, f, indent=4)
        logger.info(f"Saved diagnostics to {diagnostic_path}")
        print(f"Saved diagnostics to {diagnostic_path}")

        logger.info(f"merge_data completed in {time.time() - start_time:.2f} seconds")
        return merged, stats
    except Exception as e:
        logger.error(f"Error in merge_data: {str(e)}")
        log_error(e, "merge_data")
        raise