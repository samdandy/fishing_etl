import requests
from datetime import datetime
from database import database

NWS_API_URL = "https://api.weather.gov/gridpoints/HGX/53,51/forecast/hourly"

def fetch_weather_data(api_url, params=None):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return {"error": str(e)}

def transform_weather_data(data):
    run_time = datetime.now()
    periods = data.get("properties", {}).get("periods", [])
    weather_data = []
    for period in periods:
        start_time_central = datetime.fromisoformat(period["startTime"])
        end_time_central = datetime.fromisoformat(period["endTime"])
        weather_data.append({
            "start_time_central": start_time_central,
            "end_time_central": end_time_central,
            "temperature_f": period["temperature"],
            "wind_speed_mph": period["windSpeed"].split(" ")[0],
            "wind_direction": period["windDirection"],
            "created_at_central": run_time,
        })
    return weather_data

def get_weather_forecast():
    db = database.FishDatabase()
    weather_data = fetch_weather_data(NWS_API_URL)
    db.test_connection()
    if "error" not in weather_data:
        return transform_weather_data(weather_data)
    else:
        print("Failed to retrieve weather data.")