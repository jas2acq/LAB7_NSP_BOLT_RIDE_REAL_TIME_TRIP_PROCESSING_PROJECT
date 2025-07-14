import os
import sys
import json
import random
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME')

LOG_DIR = "logs"
LOG_FILE = "streaming.log"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIP_START_CSV = os.path.join(BASE_DIR, "data", "trip_start.csv")
TRIP_END_CSV = os.path.join(BASE_DIR, "data", "trip_end.csv")

def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    logger = logging.getLogger("TripStreamLogger")
    logger.setLevel(logging.INFO)
    log_path = os.path.join(LOG_DIR, LOG_FILE)
    handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=3)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()