import os
import sys
import logging
import pandas as pd

from dotenv import load_dotenv
from arcgis.gis import GIS

from logger_config import setup_logger
from process_google_sheet_data import process_google_sheet_data

setup_logger()
logger = logging.getLogger(__name__)

def upload_data_to_arcgis(df: pd.DataFrame, gis_url: str, gis_user: str, gis_pass: str, item_id: str):
    """
    З'єднуємося з ArcGIS Online і додаємо точки з DataFrame до hosted feature layer.
    Очікується, що df має колонки: 'Дата' (або d_date), 'Область' (t_region), 'Місто' (t_city),
    'i_value_1'..'i_value_10' (або відповідні), а також 'long' та 'lat' або 'long_num'/'lat_num'.
    """
    logger.info(f"Connecting to ArcGIS: {gis_url} as {gis_user}")
    gis = GIS(gis_url, gis_user, gis_pass)

    # Отримати item і перший layer
    item = gis.content.get(item_id)
    if item is None:
        logger.error(f"Feature service item not found for ITEM_ID={item_id}")
        return

    # Припускаємо, що перший шар — потрібний hosted feature layer
    try:
        flayer = item.layers[0]
    except Exception as e:
        logger.exception("Cannot access layers from item. Item.layers may be empty.")
        return

    logger.info(f"Target layer found: {flayer.properties.name}")

    # Перетворення DataFrame у список features
    features_payload = []
    for _, row in df.iterrows():
        try:
            lon = None
            lat = None
            for cand in ("long_num", "long", "longitude", "x"):
                if cand in row and pd.notna(row[cand]):
                    lon = float(row[cand])
                    break
            for cand in ("lat_num", "lat", "latitude", "y"):
                if cand in row and pd.notna(row[cand]):
                    lat = float(row[cand])
                    break

            if lon is None or lat is None:
                logger.warning(f"Skipping row without coords: {_}")
                continue

            attrs = {
                "d_date": row.get("Дата") or row.get("d_date"),
                "t_region": row.get("Область") or row.get("t_region"),
                "t_city": row.get("Місто") or row.get("t_city"),
            }
            for i in range(1, 11):
                colname = f"Значення {i}"
                field_out = f"i_value_{i}"
                if colname in row:
                    attrs[field_out] = row[colname]
                elif field_out in row:
                    attrs[field_out] = row[field_out]

            feat = {
                "geometry": {"x": lon, "y": lat, "spatialReference": {"wkid": 4326}},
                "attributes": attrs
            }
            features_payload.append(feat)
        except Exception:
            logger.exception("Error converting row to feature.")

    if not features_payload:
        logger.warning("No features prepared to upload.")
        return

    try:
        logger.info(f"Uploading {len(features_payload)} features to layer...")
        res = flayer.edit_features(adds=features_payload)
        logger.info(f"Upload result: {res}")
    except Exception:
        logger.exception("Failed to upload features to ArcGIS.")

def load_config():
    """Loads and validates configuration from the .env file."""
    load_dotenv()
    config = {
        "SHEET_URL": os.environ.get("SHEET_URL"),
        "CREDENTIALS_FILE_NAME": os.environ.get("CREDENTIALS_FILE"),
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