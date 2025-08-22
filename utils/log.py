import logging
import os
from sys import stdout


def initialize_logging():
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    )


    # Console handler (info+ only)
    console_handler = logging.StreamHandler(stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logging.basicConfig(
        level=os.environ.get("PYTHON_LOGGING_LEVEL", logging.DEBUG),
        handlers=[console_handler],
    )
