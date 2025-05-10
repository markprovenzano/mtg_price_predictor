# src/data_collection/fetch_market_data.py
import psycopg2
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import logger
from src.utils.error_handler import log_error

# Load environment variables from .env
load_dotenv()

# Get project root directory (two levels up from src/data_collection/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def load_db_config():
    """Load TimescaleDB configuration from .env."""
    try:
        db_config = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD")
        }
        if not all(db_config.values()):
            raise ValueError("Incomplete TimescaleDB configuration in .env")
        return db_config
    except Exception as e:
        logger.error(f"Failed to load DB config: {e}")
        log_error(e, "Loading TimescaleDB configuration")
        raise

def load_filtered_card_sku_ids(conn, card_sku_ids: list):
    """Load card_sku_id values from sales_history with at least 12 sales over 60 days."""
    try:
        cursor = conn.cursor()
        sku_id_str = ",".join([f"'{sku_id}'" for sku_id in card_sku_ids])
        query = f"""
            SELECT card_sku_id
            FROM sales_history
            WHERE order_date >= CURRENT_DATE - INTERVAL '60 days'
            AND card_sku_id IN ({sku_id_str})
            GROUP BY card_sku_id
            HAVING COUNT(*) >= 12
        """
        cursor.execute(query)
        filtered_sku_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        logger.info(f"Filtered to {len(filtered_sku_ids)} card_sku_id values with >= 12 sales over 60 days")
        return filtered_sku_ids
    except Exception as e:
        logger.error(f"Failed to load filtered card_sku_ids: {e}")
        log_error(e, "Loading filtered card_sku_ids")
        raise

def load_card_sku_ids(csv_path: str = os.path.join(RAW_DATA_DIR, "card_list.csv")):
    """Load card_sku_id values from card_list.csv."""
    try:
        if not os.path.exists(csv_path):
            logger.error(f"Card list CSV not found: {csv_path}")
            log_error(FileNotFoundError(f"Card list CSV not found: {csv_path}"), "Loading card_sku_ids")
            raise FileNotFoundError(f"Card list CSV not found: {csv_path}")
        df = pd.read_csv(csv_path)
        card_sku_ids = df["card_sku_id"].astype(str).tolist()
        logger.info(f"Loaded {len(card_sku_ids)} card_sku_id values from {csv_path}")
        return card_sku_ids
    except Exception as e:
        logger.error(f"Failed to load card_sku_ids: {e}")
        log_error(e, "Loading card_sku_ids")
        raise

def fetch_market_data(tables: list = ["market_prices", "sales_history", "listings"]):
    """
    Fetch data from specified TimescaleDB tables, filtered by card_sku_id with >= 12 sales and 60-day limit, and return DataFrames with corrected prices.

    Args:
        tables (list): List of table names to query (market_prices, sales_history, listings).

    Returns:
        dict: Dictionary of pandas DataFrames, keyed by table name, or None if an error occurs.
    """
    try:
        # Load initial card_sku_id from card_list.csv
        card_sku_ids = load_card_sku_ids()

        db_config = load_db_config()
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = conn.cursor()
        logger.info(f"Connected to TimescaleDB database: {db_config['dbname']}")

        # Filter card_sku_id by sales frequency
        filtered_sku_ids = load_filtered_card_sku_ids(conn, card_sku_ids)
        sku_id_str = ",".join([f"'{sku_id}'" for sku_id in filtered_sku_ids])

        dataframes = {}
        for table in tables:
            logger.info(f"Querying table: {table}")
            start_time = datetime.now()
            if table == "listings":
                query = f"""
                    SELECT *
                    FROM listings
                    WHERE card_sku_id IN ({sku_id_str})
                    AND updated_at >= CURRENT_DATE - INTERVAL '60 days'
                """
            elif table == "sales_history":
                query = f"SELECT * FROM {table} WHERE order_date >= CURRENT_DATE - INTERVAL '60 days' AND card_sku_id IN ({sku_id_str})"
            else:  # market_prices
                query = f"SELECT * FROM {table} WHERE card_sku_id IN ({sku_id_str}) AND updated_at >= CURRENT_DATE - INTERVAL '60 days'"
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # Create DataFrame
            data_df = pd.DataFrame(rows, columns=columns)

            # Correct price columns (divide by 100 to convert from cents to dollars)
            if table == "listings":
                data_df["price"] = pd.to_numeric(data_df["price"], errors="coerce") / 100
                data_df["is_low_inventory"] = data_df["direct_inventory_count"] <= 5  # Flag potential out-of-stock
            elif table == "sales_history":
                data_df["price"] = data_df["price"] / 100
            elif table == "market_prices":
                price_columns = ["low", "lowest_list", "market", "direct_low"]
                for col in price_columns:
                    if col in data_df.columns:
                        data_df[col] = data_df[col] / 100

            dataframes[table] = data_df
            query_duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Queried {table} with {len(data_df)} records in {query_duration:.2f} seconds")

        cursor.close()
        conn.close()
        logger.info("Market data fetch from TimescaleDB completed successfully")
        return dataframes

    except psycopg2.Error as e:
        logger.error(f"Database error fetching market data: {e}")
        log_error(e, "Fetching market data from TimescaleDB")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in fetch_market_data: {e}")
        log_error(e, "Unexpected error in fetch_market_data")
        return None

if __name__ == "__main__":
    fetch_market_data()