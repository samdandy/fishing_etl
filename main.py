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
    # if ARGS.load_game:
    #     logger.info("Loading BATS game data")
    #     get_game(secret_client=secret_client, on_prem_backfill=on_prem_backfill)
    # if ARGS.load_video:
    #     logger.info("Loading BATS video data")
    #     get_video(secret_client=secret_client, on_prem_backfill=on_prem_backfill)

    # end_time = time.time()
    # run_time = datetime.fromtimestamp(end_time) - datetime.fromtimestamp(start_time)
    # logger.info(f"Run time: {run_time}")
    # logger.info("BATS ETL process completed")
    # logger.info("Exiting BATS")
