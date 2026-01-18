import datetime
import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, Optional
from urllib.parse import quote_plus

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def initialize_directory():
    load_dotenv()
    os.makedirs("data/logs", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("data/logs/weather_pipeline.log"),
            logging.StreamHandler()
        ]
    )

@lru_cache(maxsize=None)
def get_db_engine():
    user = os.getenv("DB_USER", "")
    pw = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    
    # URL encode credentials to handle special characters safely
    encoded_user = quote_plus(user)
    encoded_pw = quote_plus(pw)
    
    return create_engine(f'postgresql://{encoded_user}:{encoded_pw}@{host}:{port}/{db}')

def init_db():
    """Initializes the database schema if it does not exist."""
    try:
        engine = get_db_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(255),
                    temp_celsius FLOAT,
                    humidity_pct FLOAT,
                    recorded_at TIMESTAMP,
                    processed_at TIMESTAMP,
                    CONSTRAINT city_recorded_at_unique UNIQUE (city, recorded_at)
                );
            """))
        logging.info("Database schema initialized.")
    except Exception as e:
        logging.error(f"Error initializing database schema: {e}")

def fetch_data(city: str) -> Optional[Dict[str, Any]]:
    API_KEY=os.getenv('OPENWEATHERMAP_API_KEY')
    if not API_KEY:
        logging.error("API Key not found in environment variables.")
        return None

    URL = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }

    try:
        response = requests.get(URL, params=params, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/weather_{city}_{timestamp}.json"

        # store raw data into json file
        with open(filename, 'w') as f:
            json.dump(raw_data, f, indent=4)
            logging.info(f"Raw data saved to {filename}")

        return raw_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for {city}: {e}")
        return None

def filter_data(raw_data: Optional[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    if not raw_data:
        logging.warning("No raw data provided for filtering.")
        return None
    try:
        df = pd.json_normalize(raw_data)
        cols = {
            "name": "city",
            "main.temp": "temp_celsius",
            "main.humidity": "humidity_pct",
            "dt": "timestamp_unix"
        }
        df = df[cols.keys()].rename(columns=cols)
        df["recorded_at"] = pd.to_datetime(df["timestamp_unix"], unit="s")
        df["processed_at"] = pd.Timestamp.now(datetime.timezone.utc).replace(tzinfo=None)
        df = df.drop(columns=["timestamp_unix"])
        return df
    except Exception as e:
        logging.error(f"Error filtering data: {e}")
        return None

def store_to_db(df: Optional[pd.DataFrame]):
    if df is None or df.empty:
        logging.warning("No DataFrame to store in database.")
        return

    engine = get_db_engine()

    df.to_sql('temp_weather', engine, if_exists='replace', index=False)

    query = text(
        """
            INSERT INTO weather_data (city, temp_celsius, humidity_pct, recorded_at, processed_at)
            SELECT city, temp_celsius, humidity_pct, recorded_at, processed_at
            FROM temp_weather
            ON CONFLICT (city, recorded_at) DO NOTHING;
        """
    )

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Data stored to database")

if __name__ == "__main__":
    initialize_directory()
    init_db()
    raw_data = fetch_data('Kuala Lumpur')
    df = filter_data(raw_data)
    store_to_db(df)
    
