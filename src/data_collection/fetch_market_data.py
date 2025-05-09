# src/data_collection/fetch_market_data.py
import psycopg2
import json
import os
import yaml
from datetime import datetime
from src.utils.logger import logger
from src.utils.error_handler import log_error

# Get project root directory (two levels up from src/data_collection/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
CONFIG_FILE = os.path.join(PROJECT_ROOT, "config", "config.yaml")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def load_db_config(config_file: str = CONFIG_FILE):
    """Load TimescaleDB configuration from config.yaml."""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        db_config = config.get("database", {})
        if not all(key in db_config for key in ["host", "port", "dbname", "user", "password"]):
            raise ValueError("Incomplete TimescaleDB configuration in config.yaml")
        return db_config
    except Exception as e:
        logger.error(f"Failed to load DB config: {e}")
        log_error(e, "Loading TimescaleDB configuration")
        raise

def fetch_market_data(tables: list = ["prices", "card_metadata", "market_trends"]):
    """
    Fetch data from specified TimescaleDB tables and save as JSON to data/raw/.

    Args:
        tables (list): List of table names to query.

    Returns:
        bool: True if successful, False if an error occurs.
    """
    try:
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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for table in tables:
            logger.info(f"Querying table: {table}")
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]

            # Save to JSON
            output_file = os.path.join(RAW_DATA_DIR, f"{table}_{timestamp}.json")
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(data)} records from {table} to {output_file}")

        cursor.close()
        conn.close()
        logger.info("Market data fetch from TimescaleDB completed successfully")
        return True

    except psycopg2.Error as e:
        logger.error(f"Database error fetching market data: {e}")
        log_error(e, "Fetching market data from TimescaleDB")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in fetch_market_data: {e}")
        log_error(e, "Unexpected error in fetch_market_data")
        return False

if __name__ == "__main__":
    fetch_market_data()