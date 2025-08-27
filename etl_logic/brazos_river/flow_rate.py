import requests
from datetime import datetime
import polars as pl
from database import database
from etl_logic.brazos_river.util import grade_flow_rate

OUTPUT_FILE = "/tmp/flow_rate.txt"

def transform_flow_rate_data(df: pl.DataFrame, location:str) -> pl.DataFrame:
    return (
        df.rename({"Reading": "reading_time_central","Value": "flow_rate","Unit": "unit"})  
          .with_columns([
              pl.col("flow_rate")
                .str.replace_all(",", "")     
                .cast(pl.Float64),
             pl.lit(datetime.now()).alias("created_at_central"),
             pl.lit(location).alias("location_name"),
             pl.col("flow_rate")
                .str.replace_all(",", "")
                .cast(pl.Float64)
                .map_elements(grade_flow_rate, return_dtype=pl.Float64)
                .alias("flow_rate_weight"),
          ])
          .drop(["Receive", "Data Quality"])  
    )

def fetch_flow_rate_data() -> pl.DataFrame:
    now = datetime.now().date()
    current_str = now.strftime("%Y-%m-%d %H:%M:%S")
    date_start = str(current_str).replace(" ", "%20")
    date_end = str(current_str).replace(" ", "%20").replace("00:00", "23:59")
    # URL = f"https://www.brazosbasinnow.org/export/file/?site_id=582&site=b1743dfa-519d-4f92-9fe1-aaadbb0dd740&device_id=2&device=0c0df2a1-6f34-438d-8076-b8ce236bf4d0&mode=&hours=&data_start=2020-01-01%2000:00:00&data_end=2025-08-30%2013:33:05&tz=US%2FCentral&format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S&mime=txt&delimeter=tab"
    URL = f"https://www.brazosbasinnow.org/export/file/?site_id=582&site=b1743dfa-519d-4f92-9fe1-aaadbb0dd740&device_id=2&device=0c0df2a1-6f34-438d-8076-b8ce236bf4d0&mode=&hours=&data_start={date_start}&data_end={date_end}&tz=US%2FCentral&format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S&mime=txt&delimeter=tab"
    try:
        response = requests.get(URL)
        response.raise_for_status()
        with open(OUTPUT_FILE, 'wb') as f:
            f.write(response.content)
        df = pl.read_csv(OUTPUT_FILE, separator="\t")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return 
    except ValueError as ve:
        print(f"Error reading Excel file: {ve}")
    except KeyError as ke:
        print(f"Sheet or column not found: {ke}")


def get_flow_rate() -> pl.DataFrame:
    db = database.FishDatabase()
    df = fetch_flow_rate_data()
    db.merge_dataframe("river.bra_flow_rate", transform_flow_rate_data(df, "West Columbia, TX"), delete_columns=["reading_time_central"], primary_key_columns=["reading_time_central"])
    db.close_connection()
    return
 