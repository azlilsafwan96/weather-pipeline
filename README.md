# Weather Data Pipeline

A Python-based pipeline to fetch, process, and store weather data using the OpenWeatherMap API.

## Features

- **Data Ingestion**: Fetches current weather data for a specified city (default: Kuala Lumpur).
- **Raw Storage**: Saves raw JSON responses to `data/raw/` with timestamps for historical tracking.
- **Processing**: Normalizes JSON data into a structured Pandas DataFrame.
- **Logging**: comprehensive logging to both console and `data/logs/weather_pipeline.log`.

## Prerequisites

- Python 3.8+
- An OpenWeatherMap API Key

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   Create a `.env` file in the project root and add your API key:
   ```ini
   OPENWEATHERMAP_API_KEY=your_actual_api_key_here
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_NAME=your_db_name
   DB_HOST=your_db_host
   DB_PORT=your_db_port
   ```

## Usage

Run the main fetch script:

```bash
python scripts/weather_pipeline.py
```

## Output

- **Console**: Prints the processed DataFrame containing City, Temperature, Humidity, and Recorded Time.
- **Files**: Raw JSON data is saved to `data/raw/weather_<City>_<Timestamp>.json`.
