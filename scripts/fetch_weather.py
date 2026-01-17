import logging
import os
import datetime

import pandas as pd
import requests
import json
from dotenv import load_dotenv

os.makedirs("data/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data/logs/weather_pipeline.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
OPENWEATHERMAP_API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')

def fetch_weather_data(city: str):
    """
    Fetches weather data for a given city from OpenWeatherMap API.
    """
    if not OPENWEATHERMAP_API_KEY:
        logging.error("API Key not found. Please set OPENWEATHERMAP_API_KEY.")
        return None

    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": OPENWEATHERMAP_API_KEY,
        "units": "metric"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        raw_data = response.json()

        os.makedirs("data/raw", exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/weather_{city}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(raw_data, f, indent=4)
            logging.info(f"Raw data saved to {filename}")

        df = pd.json_normalize(raw_data)

        cols = {
            'name': 'city',
            'main.temp': 'temp_celsius',
            'main.humidity': 'humidity_pct',
            'dt': 'timestamp_unix'
        }
        df = df[cols.keys()].rename(columns=cols)
        df['recorded_at'] = pd.to_datetime(df['timestamp_unix'], unit='s')
        df = df.drop(columns=['timestamp_unix'])

        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for {city}: {e}")
        return None

if __name__ == "__main__":
    weather = fetch_weather_data('Kuala Lumpur')
    if weather is not None:
        print(weather)
