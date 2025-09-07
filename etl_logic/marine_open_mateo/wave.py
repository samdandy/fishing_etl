import requests
import polars as pl
from datetime import datetime
from database import database

MARINE_OPEN_MATEO_API_URL = "https://marine-api.open-meteo.com/v1/marine?latitude=28.8353&longitude=-95.6647&hourly=wave_height,wind_wave_direction,wave_direction&timezone=America%2FChicago&length_unit=imperial"
format_string = "%Y-%m-%dT%H:%M:%S"
def fetch_wave_data(api_url, params=None):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  
        print("Wave data fetched successfully")
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching wave data: {e}")
        return 

def transform_wave_data(data):
    run_time = datetime.now()
    df = pl.DataFrame(data['hourly'])
    df = df.rename(
        {"time": "time_central", "wave_height": "wave_height_ft", "wave_direction": "wave_direction_deg"}
    ).with_columns(pl.lit(run_time).alias("created_at_central"),
                   pl.lit("Sargent").alias("location"))
    df = df.select(["time_central","location", "wave_height_ft", "wave_direction_deg", "created_at_central"])
    return df

def get_wave_forecast():
    db = database.FishDatabase()
    wave_data = fetch_wave_data(MARINE_OPEN_MATEO_API_URL)
    transformed_data = transform_wave_data(wave_data)
    db.merge_dataframe("weather.open_mateo_wave", transformed_data, delete_columns=["time_central","location"], primary_key_columns=["time_central","location"])
    db.close_connection()
    return

