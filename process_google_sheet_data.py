import os
import gspread
import pandas as pd

from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from logger_config import logging

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

    # Перевірка, чи дані не порожні, перед створенням DataFrame
    if not data:
        logging.warning("Попередження: Не знайдено даних у Google Sheet.")
        return pd.DataFrame()
    
    source_df = pd.DataFrame(data)

    # Список для зберігання нових, трансформованих рядків
    processed_rows = []

    # Визначення колонок зі значеннями
    value_columns = [f'Значення {i}' for i in range(1, 11)]
    
    # Ітерація по кожному рядку вихідного DataFrame
    for _, row in source_df.iterrows():
        # Зберігаємо значення з колонок "Значення" в окремий словник
        # і конвертуємо їх у числовий тип, ігноруючи помилки
        values = {col: pd.to_numeric(row.get(col), errors="coerce") for col in value_columns}
        values= {k: v if pd.notna(v) else 0 for k, v in values.items()}

        # Знаходимо максимальне значення серед усіх колонок "Значення"
        max_value = int(max(values.values()))
        # Якщо немає значень більше 0, пропускаємо рядок
        if max_value == 0:
            continue

        # Створення нових рядків на основі max_value
        for i in range(1, max_value + 1):
            new_row = {
                'Дата': row['Дата'],
                'Область': row['Область'],
                'Місто': row['Місто'],
                'long': row['long'],
                'lat': row['lat']
            }
            # Заповнення колонок "Значення" за визначеною логікою
            for col_name, original_value in values.items():
                new_row[col_name] = 1 if i <= original_value else 0
            processed_rows.append(new_row)

    return pd.DataFrame(processed_rows)


if __name__ == "__main__":
   load_dotenv()
   SHEET_URL = os.environ.get("SHEET_URL")
   CREDENTIALS_FILE = os.environ.get("CREDENTIALS_FILE_NAME")

   try:
        processed_dataframe = process_google_sheet_data(SHEET_URL, CREDENTIALS_FILE)
        
        if not processed_dataframe.empty:
            logging.info("Data processed successfully. Number of new lines:", len(processed_dataframe))
            logging.info("First 5 lines of transformed data:")
            logging.info(processed_dataframe.head())
        else:
            logging.info("Processing is complete, but no lines have been generated.")
            
   except FileNotFoundError:
       logging.error(f"Credentials file not found by path: {CREDENTIALS_FILE}")
       logging.info("Please check and correct the path in the CREDENTIALS_FILE variable.")
   except Exception as e:
       logging.error(f"An unexpected error occurred during execution: {e}")
