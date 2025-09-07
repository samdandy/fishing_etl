import requests
import polars as pl
from datetime import datetime
from database import database
from etl_logic.nws.vars import wind_direction_weights
from etl_logic.nws.utils import get_wind_speed_weight

NWS_API_URL = "https://api.weather.gov/gridpoints/HGX/53,51/forecast/hourly"
format_string = "%Y-%m-%dT%H:%M:%S"


def fetch_weather_data(api_url, params=None):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return


def transform_weather_data(data):
    run_time = datetime.now()
    periods = data.get("properties", {}).get("periods", [])
    weather_data = []
    for period in periods:
        start_time_central = datetime.fromisoformat(
            period["startTime"].replace("-05:00", "")
        )
        end_time_central = datetime.fromisoformat(
            period["endTime"].replace("-05:00", "")
        )
        wind_speed_weight = get_wind_speed_weight(period["windSpeed"].split(" ")[0])
        wind_direction_weight = wind_direction_weights.get(period["windDirection"], 0)
        weather_data.append(
            {
                "start_time_central": start_time_central,
                "end_time_central": end_time_central,
                "temperature_f": period["temperature"],
                "wind_speed_mph": period["windSpeed"].split(" ")[0],
                "wind_speed_weight": wind_speed_weight,
                "wind_direction": period["windDirection"],
                "wind_direction_weight": wind_direction_weight,
                "created_at_central": run_time,
            }
        )
    return pl.DataFrame(weather_data)


def get_weather_forecast():
    db = database.FishDatabase()
    weather_data = fetch_weather_data(NWS_API_URL)
    db.merge_dataframe(
        "weather.nws_wind",
        transform_weather_data(weather_data),
        delete_columns=["start_time_central", "end_time_central"],
        primary_key_columns=["start_time_central", "end_time_central"],
    )
    db.close_connection()
    return
