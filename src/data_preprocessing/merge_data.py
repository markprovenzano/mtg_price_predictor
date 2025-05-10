# src/data_preprocessing/merge_data.py
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import logging

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

def filter_outliers(df, column, low_multiplier=1.5, high_multiplier=5.0):
    """Filter outliers using asymmetric IQR method."""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - low_multiplier * IQR
    upper_bound = Q3 + high_multiplier * IQR
    filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    removed = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    stats = {
        "removed_count": len(removed),
        "removed_proportion": len(removed) / len(df) if len(df) > 0 else 0,
        "price_min": float(removed[column].min()) if not removed.empty else None,
        "price_max": float(removed[column].max()) if not removed.empty else None,
        "price_mean": float(removed[column].mean()) if not removed.empty else None,
        "price_median": float(removed[column].median()) if not removed.empty else None
    }
    logger.info(f"Filtered {stats['removed_count']} outliers from {column}")
    return filtered, stats

def merge_data(market_data: dict, card_attributes: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Perform granular merge of market_data and card_attributes with diagnostics."""
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

    # Filter card_attributes to relevant card_sku_id
    relevant_sku = set(market_prices["card_sku_id"]).union(set(sales_history["card_sku_id"]))
    card_attributes = card_attributes[card_attributes["card_sku_id"].isin(relevant_sku)]
    logger.info(f"Filtered card_attributes to {len(card_attributes)} relevant records")

    # Normalize dates to Eastern Time (ET)
    market_prices["date"] = pd.to_datetime(market_prices["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")
    listings["date"] = pd.to_datetime(listings["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")
    sales_history["date"] = pd.to_datetime(sales_history["order_date"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")

    # Filter outliers from sales_history.price
    sales_history_filtered, outlier_stats = filter_outliers(sales_history, "price", low_multiplier=1.5, high_multiplier=5.0)

    # Aggregate sales_history by date
    sales_agg = sales_history_filtered.groupby(["card_sku_id", "date"]).agg({
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
        merged[col] = merged.groupby("card_sku_id")[col].transform(lambda x: x.ffill().bfill())

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

    # Outlier validation against direct_low, excluding zeros
    validation_stats = {
        "25x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 25 * merged["direct_low"])).sum()),
        "50x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 50 * merged["direct_low"])).sum()),
        "100x": int(((merged["direct_low"] > 0) & (merged["sales_price_max"] > 100 * merged["direct_low"])).sum())
    }
    outlier_stats["validation"] = validation_stats
    logger.info(f"Outlier validation stats: {validation_stats}")

    # Statistics
    stats = {
        "record_counts": {
            "market_prices": len(market_prices),
            "sales_history": len(sales_history),
            "sales_history_filtered": len(sales_history_filtered),
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
            "count": int(merged["is_low_inventory"].sum()) if "is_low_inventory" in merged.columns else 0,
            "proportion": float(merged["is_low_inventory"].mean()) if "is_low_inventory" in merged.columns else 0
        },
        "outlier_stats": outlier_stats,
        "direct_low_zero_count": int((merged["direct_low"] == 0).sum()),  # Added to track zeros
        "non_zero_sales": int(merged["sales_price_max"].gt(0).sum()),
        "correlation": merged["sales_price_max"].corr(merged["direct_low"]) if "direct_low" in merged.columns else None
    }

    # Save diagnostics
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diagnostic_path = os.path.join(LOG_DIR, f"merge_data_diagnostic_{timestamp}.json")
    with open(diagnostic_path, "w") as f:
        json.dump(stats, f, indent=4)
    logger.info(f"Saved diagnostics to {diagnostic_path}")
    print(f"Saved diagnostics to {diagnostic_path}")

    return merged, stats