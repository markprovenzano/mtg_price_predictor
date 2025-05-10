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
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

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

def load_filters():
    """Load filter configuration from config/filters.json."""
    filter_path = os.path.join(CONFIG_DIR, "filters.json")
    default_filters = {
        "outlier_thresholds": {
            "extreme_outlier_multiplier": 100,
            "validation_multipliers": [25, 50, 100]
        },
        "inventory": {
            "low_inventory_threshold": 5
        },
        "date_range": {
            "start_date": "2025-03-11",
            "end_date": "2025-05-09"
        }
    }
    if os.path.exists(filter_path):
        with open(filter_path, "r") as f:
            filters = json.load(f)
        logger.info(f"Loaded filters from {filter_path}")
    else:
        filters = default_filters
        with open(filter_path, "w") as f:
            json.dump(filters, f, indent=4)
        logger.info(f"Created default filters at {filter_path}")
    return filters

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

        # Load filters
        filters = load_filters()
        extreme_outlier_multiplier = filters["outlier_thresholds"]["extreme_outlier_multiplier"]
        validation_multipliers = filters["outlier_thresholds"]["validation_multipliers"]
        low_inventory_threshold = filters["inventory"]["low_inventory_threshold"]
        start_date = filters["date_range"]["start_date"]
        end_date = filters["date_range"]["end_date"]

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
            "low_raw_nans": int(market_prices["low"].isna().sum()),
            "lowest_list_raw_nans": int(market_prices["lowest_list"].isna().sum())
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

        # Preprocess market_prices: Forward-fill market, flag NaNs for others
        market_prices = market_prices.sort_values("date")
        market_prices["market"] = market_prices.groupby("card_sku_id")["market"].transform(lambda x: x.ffill().bfill())
        market_prices["is_direct_low_nan"] = market_prices["direct_low"].isna()
        market_prices["is_low_nan"] = market_prices["low"].isna()
        market_prices["is_lowest_list_nan"] = market_prices["lowest_list"].isna()

        # Preprocess listings: Zero-fill quantity and direct_inventory_count
        listings["is_missing_day"] = listings["price"].isna()
        listings["quantity"] = listings["quantity"].fillna(0)
        listings["direct_inventory_count"] = listings["direct_inventory_count"].fillna(0)

        # Preprocess sales_history: Flag extreme outliers
        sales_history = sales_history.merge(
            market_prices[["card_sku_id", "date", "direct_low"]],
            on=["card_sku_id", "date"],
            how="left"
        )
        sales_history["is_extreme_outlier"] = (sales_history["direct_low"] > 0) & (sales_history["price"] > extreme_outlier_multiplier * sales_history["direct_low"])
        sales_history = sales_history.drop(columns=["direct_low"])  # Avoid column conflicts

        # Create complete date range and combinations
        date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
        all_combinations = pd.DataFrame(
            [(sku, d) for sku in relevant_sku for d in date_range],
            columns=["card_sku_id", "date"]
        )

        # Merge with market_prices
        merged = all_combinations.merge(market_prices, on=["card_sku_id", "date"], how="left")

        # Merge with listings
        merged = merged.merge(listings, on=["card_sku_id", "date"], how="left", suffixes=("", "_listings"))

        # Merge with sales_history (raw granularity)
        merged = merged.merge(
            sales_history[["card_sku_id", "date", "quantity", "price", "is_extreme_outlier"]],
            on=["card_sku_id", "date"],
            how="left"
        )
        merged["sales_quantity"] = merged["quantity"].fillna(0)
        merged["sales_price"] = merged["price"].fillna(0)
        merged["is_extreme_outlier"] = merged["is_extreme_outlier"].fillna(False)
        merged = merged.drop(columns=["quantity", "price"])  # Avoid confusion

        # Merge with card_attributes
        merged = merged.merge(card_attributes, on="card_sku_id", how="left")
        logger.info(f"Merged data size: {len(merged)}")

        # Flag low inventory
        merged["is_low_inventory"] = merged["direct_inventory_count"] <= low_inventory_threshold

        # Flag no recent sales (within 7 days)
        merged["date_dt"] = pd.to_datetime(merged["date"])
        merged["has_recent_sales"] = merged.groupby("card_sku_id")["sales_quantity"].transform(
            lambda x: x.rolling(window="7D", on=merged["date_dt"]).sum() > 0
        )
        merged["is_no_recent_sales"] = ~merged["has_recent_sales"]
        merged = merged.drop(columns=["date_dt", "has_recent_sales"])

        # Flag and drop card_sku_ids with all direct_low NaNs
        all_nan_skus = merged.groupby("card_sku_id")["direct_low"].apply(lambda x: x.isna().all())
        all_nan_skus = all_nan_skus[all_nan_skus].index
        merged["is_all_direct_low_nan"] = merged["card_sku_id"].isin(all_nan_skus)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if len(all_nan_skus) > 0:
            dropped_sku_path = os.path.join(LOG_DIR, f"dropped_sku_ids_{timestamp}.txt")
            with open(dropped_sku_path, "w") as f:
                f.write("\n".join(map(str, all_nan_skus)))
            logger.info(f"Logged {len(all_nan_skus)} dropped card_sku_ids to {dropped_sku_path}")
        merged = merged[~merged["is_all_direct_low_nan"]]
        logger.info(f"Dropped {len(all_nan_skus)} card_sku_ids with all direct_low NaNs")

        # Samples for diagnostics
        sample_cols = ["card_sku_id", "date", "sales_price", "direct_low", "low", "price", "set_name", "rarity", "condition"]
        samples = {
            "direct_low_nan": merged[merged["is_direct_low_nan"]][sample_cols].sample(n=min(5, merged["is_direct_low_nan"].sum()), random_state=42).to_dict(orient="records"),
            "low_nan": merged[merged["is_low_nan"]][sample_cols].sample(n=min(5, merged["is_low_nan"].sum()), random_state=42).to_dict(orient="records"),
            "extreme_outlier": merged[merged["is_extreme_outlier"]][sample_cols].sample(n=min(5, merged["is_extreme_outlier"].sum()), random_state=42).to_dict(orient="records")
        }

        # Outlier validation
        validation_stats = {
            f"{m}x": int(((merged["direct_low"] > 0) & (merged["sales_price"] > m * merged["direct_low"])).sum())
            for m in validation_multipliers
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
                    "low", "market", "direct_low", "price", "lowest_list",
                    "direct_inventory_count", "sales_quantity", "sales_price"
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
            "non_zero_sales": int(merged["sales_price"].gt(0).sum()),
            "correlation": merged["sales_price"].corr(merged["direct_low"]) if "direct_low" in merged.columns else None,
            "raw_stats": raw_stats,
            "samples": samples
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