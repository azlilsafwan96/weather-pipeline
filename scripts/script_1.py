import os, requests, pandas as pd
from dotenv import load_dotenv

load_dotenv()
OPENWEATHERMAP_API_KEY=os.getenv('OPENWEATHERMAP_API_KEY')

def fetch_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        print(f"Error: {response.status_code}")
        return None

weather = fetch_weather_data('Kuala Lumpur')


