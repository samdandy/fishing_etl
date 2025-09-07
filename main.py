import argparse
import logging
import time
from datetime import datetime
from utils.log import initialize_logging
from etl_logic.nws.weather import get_weather_forecast
from etl_logic.brazos_river.flow_rate import get_flow_rate
from etl_logic.marine_open_mateo.wave import get_wave_forecast
parser = argparse.ArgumentParser(prog="FISH", description="Loads FISH data")
parser.add_argument("--load-weather", action="store_true", help="Load weather data from NWS")
parser.add_argument("--load-flow-rate", action="store_true", help="Load flow rate data from Brazos River Authority")
parser.add_argument("--load-wave-data", action="store_true", help="Load wave data from Marine Open Meteo")
parser.add_argument("--start-date", type=str, help="Start date for data loading in YYYY-MM-DD format")
parser.add_argument("--end-date", type=str, help="End date for data loading in YYYY-MM-DD format")
ARGS = parser.parse_args()

def run_etl_process():
    start_date = ARGS.start_date
    end_date = ARGS.end_date
    if ARGS.load_weather:
        get_weather_forecast()
    if ARGS.load_flow_rate:
        get_flow_rate(start_date, end_date)
    if ARGS.load_wave_data:
        get_wave_forecast()

def lambda_handler(event=None, context=None):
    """
    AWS Lambda entrypoint
    """
    initialize_logging()
    start_time = time.time()
    logger = logging.getLogger("FISH")

    # Always run both in Lambda (or inspect event to decide)
    get_weather_forecast()
    get_flow_rate()

    end_time = time.time()
    run_time = datetime.fromtimestamp(end_time) - datetime.fromtimestamp(start_time)
    logger.info(f"Run time: {run_time}")
    logger.info("FISH ETL process completed (Lambda)")

    return {"status": "success", "run_time": str(run_time)}

if __name__ == "__main__":
    initialize_logging()
    start_time = time.time()
    logger = logging.getLogger("FISH")
    run_etl_process()
    end_time = time.time()
    run_time = datetime.fromtimestamp(end_time) - datetime.fromtimestamp(start_time)
    logger.info(f"Run time: {run_time}")
    logger.info("FISH ETL process completed")
    logger.info("Exiting FISH")
