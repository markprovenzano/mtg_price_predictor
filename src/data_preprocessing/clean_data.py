# src/data_processing/clean_data.py
import pandas as pd
from src.utils.logger import logger
from src.utils.error_handler import log_error


def clean_data(merged_data: pd.DataFrame) -> pd.DataFrame:
    """Clean merged data for modeling."""
    try:
        df = merged_data.copy()

        # Flag missing days for market_prices and listings
        date_range = pd.date_range(start="2025-03-11", end="2025-05-09").strftime("%Y-%m-%d").tolist()
        df["is_missing_day"] = ~df["date"].isin(df[df["market"].notna()]["date"].unique())

        # Impute listings missing days
        df["direct_inventory_count"] = df["direct_inventory_count"].fillna(0)

        # Validate outliers against direct_low
        df["is_outlier"] = (df["sales_price_max"] > 100 * df["direct_low"]).fillna(False)

        # One-hot encode categorical variables
        df = pd.get_dummies(df, columns=["rarity", "condition"], prefix=["rarity", "condition"])

        # Keep set_name raw, defer release date
        # Note: Add Scryfall set release date in feature_engineering

        # Sample for testing (1M rows)
        if len(df) > 1000000:
            df = df.sample(n=1000000, random_state=42)
            logger.info("Sampled 1M rows for testing")

        # Statistics
        stats = {
            "record_count": len(df),
            "nan_counts": {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().any()},
            "outlier_count": int(df["is_outlier"].sum())
        }
        logger.info(f"Clean data stats: {stats}")
        return df, stats

    except Exception as e:
        logger.error(f"Error in clean_data: {str(e)}")
        log_error(e, "Cleaning data")
        raise