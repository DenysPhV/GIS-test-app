import os
import sys
import pandas as pd

from dotenv import load_dotenv
from arcgis.gis import GIS
from arcgis.features import Feature, FeatureLayer

from process_google_sheet_data import process_google_sheet_data
from logger_config import logging

def upload_data_to_arcgis(df: pd.DataFrame, gis_url: str, gis_user: str, gis_pass: str, item_id: str):
    """
    Downloads data from DataFrame to the specified Feature Layer in ArcGIS Online.

    Args:
        df (pd.DataFrame): DataFrame with prepared data.
        gis_url (str): URL of your portal ArcGIS Online (url., "https://magneticonegis.maps.arcgis.com").
        gis_user (str): Name user ArcGIS Online.
        gis_pass (str): Pass user ArcGIS Online.
        item_id (str): ID (Feature Layer) в ArcGIS Online.
    """

    try:
        logging.info("connect up to ArcGIS Online...")
        gis = GIS(gis_url, gis_user, gis_pass)
        logging.info(f"Successfully connected as user: {gis.properties.user.username}")

        # Отримання доступу до шару за його ID
        feature_layer_item = gis.content.get(item_id)
        if not feature_layer_item:
            sys.exit(f"Failed to find item from ID: {item_id}. Check ID and access rights.")
        
        feature_layer = feature_layer_item.layers[0] 
        
        # Мапінг колонок DataFrame на поля атрибутивної таблиці шару
        field_mapping = {
            'Дата': 'd_date',
            'Область': 't_region',
            'Місто': 't_city',
            'Значення 1': 'i_value_1', 'Значення 2': 'i_value_2',
            'Значення 3': 'i_value_3', 'Значення 4': 'i_value_4',
            'Значення 5': 'i_value_5', 'Значення 6': 'i_value_6',
            'Значення 7': 'i_value_7', 'Значення 8': 'i_value_8',
            'Значення 9': 'i_value_9', 'Значення 10': 'i_value_10',
        }

        features_to_add = []
        logging.info("Preparing objects to download...")

        for _, row in df.iterrows():
            attributes = {arcgis_field: row.get(df_col) for df_col, arcgis_field in field_mapping.items()}
            
            try:
                lon = float(str(row['long']).replace(',', '.'))
                lat = float(str(row['lat']).replace(',', '.'))
                geometry = {'x': lon, 'y': lat, 'spatialReference': {'wkid': 4326}}
                feature = Feature(geometry=geometry, attributes=attributes)
                features_to_add.append(feature)
            except (ValueError, TypeError) as e:
                logging.warning(f"Passed string due to coordinate conversion error: {row.to_dict()}. Error: {e}")
                continue

        if not features_to_add:
            logging.info("There are no objects to add. Completion of work.")
            return

        # Для надійності, спочатку очистимо шар від старих даних
        logging.info("Cleaning the layer from old data...")
        feature_layer.delete_features(where="1=1")

        # Додавання об'єктів до шару частинами
        chunk_size = 1000
        for i in range(0, len(features_to_add), chunk_size):
            chunk = features_to_add[i:i + chunk_size]

            logging.info(f"Loading part {i//chunk_size + 1}/{ -(-len(features_to_add)//chunk_size)} (objects: {len(chunk)})...")

            result = feature_layer.edit_features(adds=chunk)
            success_count = sum(1 for item in result['addResults'] if item['success'])
            error_count = len(chunk) - success_count

            logging.info(f"Successfully added {success_count}, Errors: {error_count}")

            if error_count > 0:
                logging.info("Error details:", [res['error'] for res in result['addResults'] if not res['success']])

    except Exception as e:
        logging.error(f"A critical error occurred while working with ArcGIS: {e}")

def load_config():
    """Loads the configuration from the .env file and checks it."""
    load_dotenv()
    config = {
        "SHEET_URL": os.environ.get("SHEET_URL"),
        "CREDENTIALS_FILE_NAME": os.environ.get("CREDENTIALS_FILE_NAME"),
        "ARCGIS_URL": os.environ.get("ARCGIS_URL"),
        "ARCGIS_USERNAME": os.environ.get("ARCGIS_USERNAME"),
        "ARCGIS_PASSWORD": os.environ.get("ARCGIS_PASSWORD"),
        "ITEM_ID": os.environ.get("ITEM_ID")
    }
    
    # Перевірка, чи всі змінні завантажені
    missing_vars = [key for key, value in config.items() if value is None]
    if missing_vars:
        sys.exit(f"ERROR: The following variables were not found in the .env file {', '.join(missing_vars)}")
        
    # Створюємо повний шлях до credentials.json
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config["CREDENTIALS_FILE_PATH"] = os.path.join(script_dir, config["CREDENTIALS_FILE_NAME"])
    
    return config


if __name__ == "__main__":
    # --- ЕТАП 0: Завантаження та перевірка конфігурації ---
    logging.info("--- Stage 1: Loading the configuration ---")
    config = load_config()

    # --- ЕТАП 1: Отримання та обробка даних з Google Sheets ---
    logging.info("\n--- Stage 2: Processing data from Google Sheets ---")
    if not os.path.exists(config["CREDENTIALS_FILE_PATH"]):
        sys.exit(f"ERROR: Credentials file not found {config['CREDENTIALS_FILE_PATH']}")
        
    processed_df = process_google_sheet_data(config["SHEET_URL"], config["CREDENTIALS_FILE_PATH"])

    if processed_df is not None and not processed_df.empty:
        logging.info(f"Successfully processed, generated {len(processed_df)} strings.")
        
        # --- ЕТАП 2: Завантаження даних в ArcGIS Online ---
        logging.info("\n--- Stage 3: Downloading data to ArcGIS ---")
        upload_data_to_arcgis(
            processed_df, 
            config["ARCGIS_URL"], 
            config["ARCGIS_USERNAME"], 
            config["ARCGIS_PASSWORD"], 
            config["ITEM_ID"]
        )
        logging.info("\nTask completed: Check the data update on the map.")
    else:
        logging.info("Failed to retrieve or process data from Google Sheets. Completion of work.")