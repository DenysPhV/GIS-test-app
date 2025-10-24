import os
import sys
import logging
import pandas as pd

from dotenv import load_dotenv
from arcgis.gis import GIS
from arcgis.features import Feature

from logger_config import setup_logger
from process_google_sheet_data import process_google_sheet_data

setup_logger()
logger = logging.getLogger(__name__)

def upload_data_to_arcgis(df: pd.DataFrame, gis_url: str, gis_user: str, gis_pass: str, item_id: str):
    """
    Uploads data from a DataFrame to a specified Feature Layer in ArcGIS Online.
    
    This function:
    1. Connects to the GIS.
    2. Clears all existing features from the target layer.
    3. Processes coordinates (handling both integer and decimal formats).
    4. Maps DataFrame columns to layer fields.
    5. Uploads new features in chunks.
    """

    try:
        logger.info("connect up to ArcGIS Online...")
        gis = GIS(gis_url, gis_user, gis_pass)
        logger.info(f"Successfully connected as user: {gis.properties.user.username}")

        logger.info(f"Searching for item with ID: {item_id}...")
        feature_layer_item = gis.content.get(item_id)

        if not feature_layer_item:
            logger.critical(f"FATAL: Could not find item with ID: {item_id}. Please check the ID.")
            sys.exit(1)

        if not feature_layer_item.layers:
            logger.critical(f"FATAL: Item found, but it contains no layers accessible to user '{gis_user}'. Please check permissions.")
            sys.exit(1)
        
        feature_layer = feature_layer_item.layers[0] 
        logger.info(f"Successfully accessed layer: '{feature_layer.properties.name}'")

        logger.info("Clearing old data from the layer (executing delete_features where='1=1')...")
        feature_layer.delete_features(where="1=1")
        logger.info("Layer cleared successfully.")

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
            'long': 'long', 'lat': 'lat'
        }

        features_to_add = []
        logger.info("Preparing objects to download...")

        for _, row in df.iterrows():
            try:
                # --- START: SMART COORDINATE PROCESSING LOGIC ---
                # This logic handles both integer (e.g., 307306393) and decimal (e.g., 30.252525) formats.
                
                lon_raw = str(row['long']).strip()
                lat_raw = str(row['lat']).strip()

                # Skip if coordinates are missing
                if not lon_raw or not lat_raw:
                    logger.warning(f"Skipping row with missing coordinate data: {row.to_dict()}")
                    continue

                lon = float(lon_raw)
                lat = float(lat_raw)

                # Skip if coordinates are (0,0) (often means missing data)
                if lon == 0 and lat == 0:
                    logger.warning(f"Skipping row with (0,0) coordinates: {row.to_dict()}")
                    continue

                # If latitude seems to be in integer format (e.g., 464702111 > 90)
                if abs(lat) > 90:
                    lat = lat / 10000000.0
                
                # If longitude seems to be in integer format (e.g., 307306393 > 180)
                if abs(lon) > 180:
                    lon = lon / 10000000.0

                # Final validation to ensure coordinates are within valid GIS bounds
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    logger.warning(f"Skipping row with invalid final coordinates (lat={lat}, lon={lon})")
                    continue
                # --- END: SMART COORDINATE PROCESSING LOGIC ---

                # Create attributes based on mapping
                attributes = {arcgis_field: row.get(df_col) for df_col, arcgis_field in field_mapping.items()}
                
                # Overwrite attributes with the *processed* coordinates
                attributes['long'] = lon
                attributes['lat'] = lat

                # Create the geometry object
                geometry = {'x': lon, 'y': lat, 'spatialReference': {'wkid': 4326}}
                feature = Feature(geometry=geometry, attributes=attributes)
                features_to_add.append(feature)

            except (ValueError, TypeError, AttributeError):
                logger.warning(f"Skipping row due to invalid/non-numeric coordinate data: {row.to_dict()}")
                continue

        if not features_to_add:
            logger.warning("No valid features to add after processing. Finishing process.")
            return

        # Для надійності, спочатку очистимо шар від старих даних
        logger.info("Cleaning the layer from old data...")
        feature_layer.delete_features(where="1=1")

        # Upload data in chunks to avoid timeouts
        chunk_size = 1000
        total_chunks = -(-len(features_to_add) // chunk_size)

        for i in range(0, len(features_to_add), chunk_size):
            chunk = features_to_add[i:i + chunk_size]
            logger.info(f"Uploading chunk {i//chunk_size + 1}/{total_chunks} ({len(chunk)} features)...")
            result = feature_layer.edit_features(adds=chunk)
            
            success_count = sum(1 for item in result.get('addResults', []) if item.get('success'))
            error_count = len(chunk) - success_count
            logger.info(f"Chunk upload complete. Success: {success_count}, Errors: {error_count}")
            
            if error_count > 0:
                # Log details of failed uploads for debugging
                error_details = [res.get('error') for res in result.get('addResults', []) if not res.get('success')]
                logger.error(f"Error details for failed uploads: {error_details}")

    except Exception as e:
        # logger.exception automatically captures and logs the full traceback
        logger.exception("A critical error occurred during an ArcGIS operation.")

def load_config():
    """Loads and validates configuration from the .env file."""
    load_dotenv()
    config = {
        "SHEET_URL": os.environ.get("SHEET_URL"),
        "CREDENTIALS_FILE_NAME": os.environ.get("CREDENTIALS_FILE_NAME"),
        "ARCGIS_URL": os.environ.get("ARCGIS_URL"),
        "ARCGIS_USERNAME": os.environ.get("ARCGIS_USERNAME"),
        "ARCGIS_PASSWORD": os.environ.get("ARCGIS_PASSWORD"),
        "ITEM_ID": os.environ.get("ITEM_ID")
    }
    
    # Validate that all variables were loaded
    missing_vars = [key for key, value in config.items() if value is None]
    if missing_vars:
        logger.critical(f"FATAL: The following required environment variables are missing in .env: {', '.join(missing_vars)}")
        sys.exit(1)
        
    # Create the full, absolute path to the credentials file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config["CREDENTIALS_FILE_PATH"] = os.path.join(script_dir, config["CREDENTIALS_FILE_NAME"])
    
    return config


# Main execution block
if __name__ == "__main__":
    logger.info("--- Stage 0: Loading Configuration ---")
    config = load_config()

    logger.info("\n--- Stage 1: Processing Data from Google Sheets ---")
    if not os.path.exists(config["CREDENTIALS_FILE_PATH"]):
        logger.critical(f"FATAL: Credentials file not found: {config['CREDENTIALS_FILE_PATH']}")
        sys.exit(1)
        
    processed_df = process_google_sheet_data(config["SHEET_URL"], config["CREDENTIALS_FILE_PATH"])

    if processed_df is not None and not processed_df.empty:
        logger.info(f"Successfully processed and generated {len(processed_df)} rows.")
        
        logger.info("\n--- Stage 2: Uploading Data to ArcGIS ---")
        upload_data_to_arcgis(
            processed_df, 
            config["ARCGIS_URL"], 
            config["ARCGIS_USERNAME"], 
            config["ARCGIS_PASSWORD"], 
            config["ITEM_ID"]
        )
        logger.info("\nTask completed. Please check the map for data updates.")
    else:
        logger.warning("Could not retrieve or process data from Google Sheets. Terminating.")