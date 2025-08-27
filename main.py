import argparse
import logging
import time
from datetime import datetime
from utils.log import initialize_logging
from etl_logic.nws.weather import get_weather_forecast
from etl_logic.brazos_river.flow_rate import get_flow_rate
parser = argparse.ArgumentParser(prog="FISH", description="Loads FISH data")

parser.add_argument("--load-weather", action="store_true", help="Load weather data from NWS")


parser.add_argument("--load-flow-rate", action="store_true", help="Load flow rate data from Brazos River Authority")
ARGS = parser.parse_args()

def run_etl_process():
    get_weather_forecast()
    get_flow_rate()

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
