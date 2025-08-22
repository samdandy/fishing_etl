import argparse
import logging
import time
from datetime import datetime
from utils.log import initialize_logging


parser = argparse.ArgumentParser(prog="BATS", description="Loads BATS data")

parser.add_argument("--load-weather", action="store_true", help="Load weather data from NWS")
ARGS = parser.parse_args()
if __name__ == "__main__":
    initialize_logging()
    start_time = time.time()
    logger = logging.getLogger("FISH")
    end_time = time.time()
    run_time = datetime.fromtimestamp(end_time) - datetime.fromtimestamp(start_time)
    logger.info(f"Run time: {run_time}")
    logger.info("FISH ETL process completed")
    logger.info("Exiting FISH")
