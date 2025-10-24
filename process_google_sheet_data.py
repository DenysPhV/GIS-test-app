import os
import sys
import gspread
import logging
import pandas as pd

from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from logger_config import setup_logger


setup_logger()
logger = logging.getLogger(__name__)

def process_google_sheet_data(sheet_url: str, credentials_path: str) -> pd.DataFrame:
    """
    Connects to Google Sheet, reads data and transforms it according to the logic of the test task.
    Args:
        sheet_url: URL Google tables.
        credentials_path: Path to JSON with credentials data.

    Returns:
        pandas.DataFrame: Transformed DataFrame ready to download in ArcGIS.
    """
    # Автентифікація та підключення до Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)

    # Відкриття таблиці та зчитування даних у DataFrame
    sheet = client.open_by_url(sheet_url).sheet1
    data = sheet.get_all_records()
    
    source_df = pd.DataFrame(data)
    processed_rows = []
    value_columns = [f'Значення {i}' for i in range(1, 11)]

    if not data:
        logger.warning("No data found in the Google Sheet.")
        return pd.DataFrame()
    
    # Ітерація по кожному рядку вихідного DataFrame
    for _, row in source_df.iterrows():
        values = {}
        
        for col in value_columns:
            raw_value = row.get(col)
            if raw_value is not None and str(raw_value).strip() != '':
                clean_value_str = str(raw_value).replace(',', '.')
                numeric_value = pd.to_numeric(clean_value_str, errors='coerce')
                values[col] = numeric_value if pd.notna(numeric_value) else 0
            else:
                values[col] = 0

        # Знаходимо максимальне значення серед усіх колонок "Значення"
        max_value = int(max(values.values()))
        # Якщо немає значень більше 0, пропускаємо рядок
        if max_value == 0:
            continue

        # Створення нових рядків на основі max_value
        for i in range(1, max_value + 1):
            new_row = {
                'Дата': row.get('Дата', ''),
                'Область': row.get('Область', ''),
                'Місто': row.get('Місто', ''),
                'long': str(row.get('long', '0')).replace(',', '.'),
                'lat': str(row.get('lat', '0')).replace(',', '.')
            }
            # Заповнення колонок "Значення" за визначеною логікою
            for col_name, original_value in values.items():
                new_row[col_name] = 1 if i <= original_value else 0
            processed_rows.append(new_row)

    return pd.DataFrame(processed_rows)


if __name__ == "__main__":
   load_dotenv()
   script_dir = os.path.dirname(os.path.abspath(__file__))
   SHEET_URL = os.environ.get("SHEET_URL")
   CREDENTIALS_FILE_NAME = os.environ.get("CREDENTIALS_FILE_NAME")

   if not SHEET_URL or not CREDENTIALS_FILE_NAME:
       logger.critical("Error: SHEET_URL or CREDENTIALS_FILE not found in the .env file.")
       sys.exit(1)

   CREDENTIALS_FILE = os.path.join(script_dir, CREDENTIALS_FILE_NAME)

   logger.info(f"Starting data processing for: {SHEET_URL}")

   try:
        if not os.path.exists(CREDENTIALS_FILE):
           logger.critical(f"FATAL: Credentials file not found: {CREDENTIALS_FILE}")
           sys.exit(1)
           
        processed_dataframe = process_google_sheet_data(SHEET_URL, CREDENTIALS_FILE)
        
        if not processed_dataframe.empty:
            logger.info(f"Data processed successfully. Generated {len(processed_dataframe)} new rows.")
            logger.info("First 5 rows of transformed data:\n" + processed_dataframe.head().to_string())
        else:
            logger.warning("Processing finished, but no rows were generated.")
            
   except Exception:
       logger.exception("An unexpected error occurred during execution.")
