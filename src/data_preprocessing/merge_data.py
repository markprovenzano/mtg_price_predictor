# src/data_processing/merge_data.py
import pandas as pd
import numpy as np
from src.utils.logger import logger
from src.utils.error_handler import log_error


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
    logger.info(
        f"Filtered {stats['removed_count']} outliers from {column} (low_multiplier: {low_multiplier}, high_multiplier: {high_multiplier})")
    return filtered, stats


def merge_data(market_data: dict, card_attributes: pd.DataFrame) -> pd.DataFrame:
    """Merge market_data and card_attributes dataframes with carry-forward imputation."""
    try:
        market_prices = market_data.get("market_prices")
        sales_history = market_data.get("sales_history")
        listings = market_data.get("listings")

        # Filter card_attributes to relevant card_sku_id
        relevant_sku = set(market_prices["card_sku_id"]).union(set(sales_history["card_sku_id"]))
        card_attributes = card_attributes[card_attributes["card_sku_id"].isin(relevant_sku)]
        logger.info(f"Filtered card_attributes to {len(card_attributes)} relevant records")

        # Normalize dates to Eastern Time (ET)
        market_prices["date"] = pd.to_datetime(market_prices["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime(
            "%Y-%m-%d")
        listings["date"] = pd.to_datetime(listings["updated_at"]).dt.tz_localize("US/Eastern").dt.strftime("%Y-%m-%d")
        sales_history["date"] = pd.to_datetime(sales_history["order_date"]).dt.tz_localize("US/Eastern").dt.strftime(
            "%Y-%m-%d")

        # Filter outliers from sales_history.price
        sales_history_filtered, outlier_stats = filter_outliers(sales_history, "price", low_multiplier=1.5,
                                                                high_multiplier=5.0)

        # Aggregate sales_history by date
        sales_agg = sales_history_filtered.groupby(["card_sku_id", "date"]).agg({
            "quantity": "sum",
            "price": ["mean", "median", "count", "max"]
        }).reset_index()
        sales_agg.columns = ["card_sku_id", "date", "sales_quantity", "sales_price_mean", "sales_price_median",
                             "sales_count", "sales_price_max"]

        # Fill missing sales_history days
        date_range = pd.date_range(start="2025-03-11", end="2025-05-09").strftime("%Y-%m-%d").tolist()
        all_combinations = pd.DataFrame(
            [(sku, d) for sku in sales_agg["card_sku_id"].unique() for d in date_range],
            columns=["card_sku_id", "date"]
        )
        sales_agg = all_combinations.merge(
            sales_agg,
            on=["card_sku_id", "date"],
            how="left"
        ).fillna({
            "sales_quantity": 0,
            "sales_price_mean": 0,
            "sales_price_median": 0,
            "sales_count": 0,
            "sales_price_max": 0
        })

        # Granular merge
        merged = market_prices.merge(
            listings,
            on=["card_sku_id", "date"],
            how="left",
            suffixes=("_market", "_listings")
        ).merge(
            sales_agg,
            on=["card_sku_id", "date"],
            how="left"
        ).merge(
            card_attributes,
            on="card_sku_id",
            how="left"
        )

        # Carry-forward imputation
        for col in ["direct_low", "market", "low", "lowest_list", "price", "quantity", "direct_inventory_count"]:
            if col in merged.columns:
                merged[col] = merged.groupby("card_sku_id")[col].transform(lambda x: x.ffill().bfill())

        # Flag dropshipper out-of-stock
        merged["is_dropshipper_out_of_stock"] = merged["direct_low"].isna()

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
                    "direct_inventory_count", "sales_quantity", "sales_price_mean", "sales_price_max"
                ] if col in merged.columns
            },
            "low_inventory": {
                "count": int(merged["is_low_inventory"].sum()) if "is_low_inventory" in merged.columns else 0,
                "proportion": float(merged["is_low_inventory"].mean()) if "is_low_inventory" in merged.columns else 0
            },
            "outlier_stats": outlier_stats
        }
        logger.info(f"Merge stats: {stats}")
        return merged, stats

    except Exception as e:
        logger.error(f"Error in merge_data: {str(e)}")
        log_error(e, "Merging data")
        raise