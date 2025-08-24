import requests
from datetime import datetime, timedelta
import polars as pl
from io import BytesIO
import pandas as pd


OUTPUT_FILE = "flow_rate.txt"


def transform_flow_rate_data(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename({"Reading": "reading_time_central","Value": "value","Unit": "unit"})  
          .with_columns([
              pl.col("value")
                .str.replace_all(",", "")     
                .cast(pl.Float64),
             pl.lit(datetime.now()).alias("created_at_central"),
          ])
          .drop(["Receive", "Data Quality"])  
    )

def fetch_flow_rate_data() -> pl.DataFrame:
    now = datetime.now().date()
    current_str = now.strftime("%Y-%m-%d %H:%M:%S")
    date_start = str(current_str).replace(" ", "%20")
    date_end = str(current_str).replace(" ", "%20").replace("00:00", "23:59")
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
    except ValueError as ve:
        print(f"Error reading Excel file: {ve}")
    except KeyError as ke:
        print(f"Sheet or column not found: {ke}")

df = fetch_flow_rate_data()
data = transform_flow_rate_data(df)
print(data)