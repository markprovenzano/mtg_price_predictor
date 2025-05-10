# src/data_preprocessing/clean_data.py
import pandas as pd
from src.utils.logger import logger
from src.utils.error_handler import log_error


def remove_outliers(df: pd.DataFrame, column: str, group_by: str, method: str = "iqr", z_threshold: float = 6,
                    percentile_lower: float = 0.01, percentile_upper: float = 0.99):
    """
    Remove outliers from a DataFrame column using specified method, grouped by a column.

    Args:
        df (pd.DataFrame): Input DataFrame.
        column (str): Column to check for outliers (e.g., 'price').
        group_by (str): Column to group by (e.g., 'card_sku_id').
        method (str): Outlier removal method ('zscore', 'iqr', 'percentile').
        z_threshold (float): Z-score threshold for zscore method (default: 6).
        percentile_lower (float): Lower percentile for percentile method (default: 0.01).
        percentile_upper (float): Upper percentile for percentile method (default: 0.99).

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    original_len = len(df)
    df = df.copy()  # Avoid modifying input DataFrame

    if method == "zscore":
        df["zscore"] = df.groupby(group_by)[column].transform(lambda x: (x - x.mean()).abs() / x.std())
        df_cleaned = df[df["zscore"] <= z_threshold].drop(columns=["zscore"])
    elif method == "iqr":
        df["Q1"] = df.groupby(group_by)[column].transform(lambda x: x.quantile(0.25))
        df["Q3"] = df.groupby(group_by)[column].transform(lambda x: x.quantile(0.75))
        df["IQR"] = df["Q3"] - df["Q1"]
        df["lower_bound"] = df["Q1"] - 1.5 * df["IQR"]
        df["upper_bound"] = df["Q3"] + 1.5 * df["IQR"]
        df_cleaned = df[(df[column] >= df["lower_bound"]) & (df[column] <= df["upper_bound"])].drop(
            columns=["Q1", "Q3", "IQR", "lower_bound", "upper_bound"])
    elif method == "percentile":
        df["lower_bound"] = df.groupby(group_by)[column].transform(lambda x: x.quantile(percentile_lower))
        df["upper_bound"] = df.groupby(group_by)[column].transform(lambda x: x.quantile(percentile_upper))
        df_cleaned = df[(df[column] >= df["lower_bound"]) & (df[column] <= df["upper_bound"])].drop(
            columns=["lower_bound", "upper_bound"])
    else:
        raise ValueError(f"Unknown outlier removal method: {method}")

    if group_by not in df_cleaned.columns:
        logger.error(f"{group_by} column missing after outlier removal")
        raise ValueError(f"{group_by} column missing after outlier removal")

    logger.info(f"Method {method}: Removed {original_len - len(df_cleaned)} outliers from {column} in sales_history")
    return df_cleaned


def clean_data(card_df: pd.DataFrame, market_dfs: dict):
    """
    Merge card and market DataFrames, normalize timestamps with enhanced sales_history aggregation, denote listing cases, filter missing card_sku_id, and return a DataFrame.

    Args:
        card_df (pd.DataFrame): DataFrame with card_sku_id and attributes from fetch_card_data.
        market_dfs (dict): Dictionary of DataFrames (market_prices, sales_history, listings) from fetch_market_data.

    Returns:
        pd.DataFrame: Cleaned DataFrame, or None if an error occurs.
    """
    try:
        logger.info("Starting data cleaning with card and market DataFrames")

        # Validate inputs
        if not isinstance(card_df, pd.DataFrame) or "card_sku_id" not in card_df.columns:
            logger.error("Invalid card DataFrame: missing card_sku_id")
            log_error(ValueError("Invalid card DataFrame: missing card_sku_id"), "Cleaning card data")
            return None
        if not isinstance(market_dfs, dict) or not all(
                key in market_dfs for key in ["market_prices", "sales_history", "listings"]):
            logger.error("Invalid market DataFrames: missing required tables")
            log_error(ValueError("Invalid market DataFrames: missing required tables"), "Cleaning market data")
            return None

        # Log input DataFrame columns and shapes
        logger.info(f"Card DataFrame columns: {list(card_df.columns)}, shape: {card_df.shape}")
        for table, df in market_dfs.items():
            logger.info(f"{table} DataFrame columns: {list(df.columns)}, shape: {df.shape}")

        # Normalize timestamps
        processed_dfs = {}
        for table, df in market_dfs.items():
            if not isinstance(df, pd.DataFrame) or "card_sku_id" not in df.columns:
                logger.error(f"Invalid DataFrame for {table}: missing card_sku_id")
                log_error(ValueError(f"Invalid DataFrame for {table}: missing card_sku_id"), f"Cleaning {table} data")
                return None
            if table == "sales_history":
                # Log original stats
                original_stats = df.groupby("card_sku_id")["price"].agg(["mean", "median", "std"]).mean()
                logger.info(
                    f"Original sales_history price stats: mean={original_stats['mean']:.2f}, median={original_stats['median']:.2f}, std={original_stats['std']:.2f}")

                # Apply outlier removal strategies
                df_zscore = remove_outliers(df, "price", "card_sku_id", method="zscore", z_threshold=6)
                df_iqr = remove_outliers(df, "price", "card_sku_id", method="iqr")
                df_percentile = remove_outliers(df, "price", "card_sku_id", method="percentile", percentile_lower=0.01,
                                                percentile_upper=0.99)

                # Log stats for each method
                for method, df_cleaned in [("zscore", df_zscore), ("iqr", df_iqr), ("percentile", df_percentile)]:
                    stats = df_cleaned.groupby("card_sku_id")["price"].agg(["mean", "median", "std"]).mean()
                    logger.info(
                        f"{method} cleaned stats: mean={stats['mean']:.2f}, median={stats['median']:.2f}, std={stats['std']:.2f}")

                # Use IQR method as default
                df = df_iqr
                # Aggregate sales_history to daily with enhanced metrics
                df["date"] = pd.to_datetime(df["order_date"]).dt.date
                df["sales_value"] = df["price"] * df["quantity"]  # For weighted average
                df = df.groupby(["card_sku_id", "date"]).agg({
                    "quantity": "sum",
                    "price": ["mean", "max", "min", "median", lambda x: x.quantile(0.75), lambda x: x.quantile(0.25),
                              "std"],
                    "id": "count",
                    "sales_value": "sum"
                }).reset_index()
                # Flatten column names
                df.columns = [
                    "card_sku_id", "date", "quantity", "price_mean", "price_max", "price_min",
                    "price_median", "price_75th", "price_25th", "price_std", "sales_count", "sales_value"
                ]
                # Calculate weighted average price
                df["price_weighted_avg"] = df["sales_value"] / df["quantity"].replace(0, pd.NA)
                df = df.drop(columns=["sales_value"])  # Drop temporary column
                logger.info(f"Aggregated sales_history to {len(df)} daily records with enhanced metrics")
            else:
                # Extract date from updated_at
                df["date"] = pd.to_datetime(df["updated_at"]).dt.date
                if table == "listings":
                    # Add dropshipper_out_of_stock flag
                    df["dropshipper_out_of_stock"] = df["direct_inventory_count"] == 0
                logger.info(f"Processed {table} with {len(df)} records")
            processed_dfs[table] = df

        # Merge DataFrames
        merged_df = card_df[["card_sku_id", "set_name", "product_name", "rarity", "condition"]].copy()
        for table, df in processed_dfs.items():
            suffix = f"_{table}"
            merge_key = ["card_sku_id", "date"] if table != "market_prices" else ["card_sku_id"]
            merged_df = merged_df.merge(df, on=merge_key, how="left", suffixes=("", suffix))
            logger.info(f"Merged {table} with {len(df)} records")

        # Filter out card_sku_id with no data in any SQL table
        sql_columns = [col for col in merged_df.columns if
                       col not in ["card_sku_id", "set_name", "product_name", "rarity", "condition"]]
        merged_df = merged_df.dropna(subset=sql_columns, how="all")
        logger.info(f"After filtering card_sku_id with no SQL table data, {len(merged_df)} rows remain")

        # Denote no seller inventory (set price and quantity to 0 for null listings)
        listing_columns = [col for col in merged_df.columns if
                           col.startswith("price_listings") or col.startswith("quantity_listings")]
        for col in listing_columns:
            merged_df[col] = merged_df[col].fillna(0)
        logger.info("Set price and quantity to 0 for no seller inventory in listings")

        logger.info("Data cleaning completed successfully")
        return merged_df

    except Exception as e:
        logger.error(f"Unexpected error in clean_data: {str(e)}")
        log_error(e, "Cleaning data")
        return None


if __name__ == "__main__":
    # Example usage with pre-fetched DataFrames
    from src.data_collection.fetch_card_data import fetch_card_data
    from src.data_collection.fetch_market_data import fetch_market_data

    card_df = fetch_card_data()
    market_dfs = fetch_market_data()  # Pre-fetch for testing
    if card_df is not None and market_dfs is not None:
        result_df = clean_data(card_df, market_dfs)
    else:
        logger.error("Failed to fetch input DataFrames")