# File: src/data_collection/fetch_card_data.py
import os
import requests
import pandas as pd
from datetime import datetime
from ..utils.logger import setup_logger, log_error
from ..utils.error_handler import handle_api_error, handle_data_error, handle_general_error


def fetch_cards(config, use_api=True, logger=None):
    """
    Fetch card metadata from Scryfall API or card_attributes.csv, filtered by card_list.csv.
    Args:
        config (dict): Configuration dictionary with API endpoint and data paths.
        use_api (bool): If True, fetch from API; else, use CSVs.
        logger (logging.Logger): Logger instance.
    Returns:
        pd.DataFrame: DataFrame with card metadata (card_sku_id, name, rarity, set_name, mana_cost, type_line).
    """
    logger = logger or setup_logger(config["data"]["logs_path"] + "info_log.txt")
    logger.info("Starting card data fetch")

    try:
        # Load filtered card list
        list_path = config["data"]["card_list_csv"]
        if not os.path.exists(list_path):
            raise FileNotFoundError("card_list.csv not found")
        card_list = pd.read_csv(list_path)
        required_list_columns = ["card_sku_id", "name", "set_code", "collector_number"]
        missing_list = [col for col in required_list_columns if col not in card_list.columns]
        if missing_list:
            raise ValueError(f"Missing columns in card_list.csv: {missing_list}")

        if use_api:
            # Fetch from Scryfall API
            endpoint = config["api"]["card_endpoint"]
            response = requests.get(endpoint, params={"page": 1})
            response.raise_for_status()
            data = response.json().get("data", [])

            # Convert to DataFrame
            df = pd.DataFrame(data)
            required_columns = ["id", "name", "rarity", "set_name", "mana_cost", "type_line"]
            if not all(col in df.columns for col in required_columns):
                raise ValueError("Missing required columns in API data")
            df = df[required_columns].rename(columns={"id": "card_sku_id"})

            # Filter by card_list
            df = df[df["card_sku_id"].isin(card_list["card_sku_id"])]
        else:
            # Load from card_attributes.csv
            attributes_path = config["data"]["card_attributes_csv"]
            if not os.path.exists(attributes_path):
                raise FileNotFoundError("card_attributes.csv not found")

            df = pd.read_csv(attributes_path)
            required_columns = ["card_sku_id", "name", "rarity", "set_name", "mana_cost", "type_line"]
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                raise ValueError(f"Missing columns in card_attributes.csv: {missing}")

            # Filter by card_list
            df = df[df["card_sku_id"].isin(card_list["card_sku_id"])]

        if df.empty:
            raise ValueError("No matching cards found after filtering")

        # Save to raw data path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d")
        output_path = f"{config['data']['raw_path']}cards_{timestamp}.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved card data to {output_path}")

        return df

    except requests.RequestException as e:
        handle_api_error(e, "fetch_card_data")
    except (ValueError, FileNotFoundError) as e:
        handle_data_error(e, "fetch_card_data")
    except Exception as e:
        handle_general_error(e, "fetch_card_data")


if __name__ == "__main__":
    import yaml

    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    df = fetch_cards(config, use_api=False)  # Test with CSVs
    print(df.head())