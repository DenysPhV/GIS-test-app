import os
import folium
import traceback
import pandas as pd

from flask import Flask, render_template
from markupsafe import Markup
from dotenv import load_dotenv


# Імпортуємо функцію для обробки даних
from process_google_sheet_data import process_google_sheet_data
from logger_config import logging

load_dotenv()

app = Flask(__name__)

def create_map_from_df(df: pd.DataFrame):
    df_for_map = df.copy()
    df_for_map['lat_num'] = pd.to_numeric(df_for_map['lat'].astype(str).str.replace(',', '.'), errors='coerce')
    df_for_map['long_num'] = pd.to_numeric(df_for_map['long'].astype(str).str.replace(',', '.'), errors='coerce')

    if not df_for_map.empty and df_for_map['lat_num'].max() > 90:
        df_for_map['lat_num'] = df_for_map['lat_num'] / 10000000.0
    if not df_for_map.empty and df_for_map['long_num'].max() > 180:
        df_for_map['long_num'] = df_for_map['long_num'] / 10000000.0
    
    df_for_map.dropna(subset=['lat_num', 'long_num'], inplace=True)
    
    if df_for_map.empty: return None

    map_center = [df_for_map['lat_num'].mean(), df_for_map['long_num'].mean()]
    m = folium.Map(location=map_center, zoom_start=6)

    for _, row in df_for_map.head(1000).iterrows():
        folium.CircleMarker(
            location=[row['lat_num'], row['long_num']],
            radius=3, color='blue', fill=True, fill_color='blue', fill_opacity=0.7,
            popup=f"Місто: {row['Місто']}<br>Дата: {row['Дата']}"
        ).add_to(m)
    
    return m._repr_html_()

@app.route('/')
def index():
    logging.info("Home page request received...")
    
    SHEET_URL = os.environ.get("SHEET_URL")
    CREDENTIALS_FILE_NAME = os.environ.get("CREDENTIALS_FILE")
    
    if not SHEET_URL or not CREDENTIALS_FILE_NAME:
        error_msg = "CONFIGURATION ERROR: NO SHEET_URL or CREDENTIALS_FILE_NAME variables found. Check if the.env file exists and is downloaded."
        logging.error(error_msg)
        return error_msg, 500

    script_dir = os.path.dirname(os.path.abspath(__file__))
    CREDENTIALS_FILE = os.path.join(script_dir, CREDENTIALS_FILE_NAME)

    table_html, map_html, error_message = None, None, None

    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"Файл облікових даних '{CREDENTIALS_FILE_NAME}' не знайдено у папці проєкту.")

        logging.info("Launching data processing from Google Sheets...")
        processed_df = process_google_sheet_data(SHEET_URL, CREDENTIALS_FILE)

        if processed_df is not None and not processed_df.empty:
            logging.info(f"Data successfully received. {len(processed_df)} lines.")

            table_html = processed_df.to_html(
                classes='display compact hover',
                table_id='data-table',
                border=2,
                index=False
            )
            map_html = create_map_from_df(processed_df)
            logging.info("Map created.")
        else:
            error_message = "Data was not obtained from Google Sheets. The table may be empty or an access error has occurred."
            logging.error(error_message)

    except Exception as e:
        error_message = f"An error occurred while executing: {e}"
        logging.error(error_message)
        traceback.print_exc()

    return render_template(
        'index.html', 
        table=Markup(table_html) if table_html else None, 
        error=error_message, 
        map_html=Markup(map_html) if map_html else None
    )

if __name__ == '__main__':
    app.run(debug=True)
