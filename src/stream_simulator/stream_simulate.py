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

def load_data(csv_path):
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} records from {csv_path}")
        return df
    except FileNotFoundError as e:
        logger.error(f"CSV file not found: {csv_path}")
        raise e
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file: {e}")
        raise e

def init_kinesis_client():
    try:
        client = boto3.client(
            'kinesis',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Kinesis client: {e}")
        raise e


def data_stream_generator(df, name, base_rate=1, spike_chance=0.05, max_spike=100, outage_chance=0.1, outage_duration_range=(5, 15)):
    """
    Generator that sends each unique record exactly once in random order,
    with random spikes and outages.
    """
    indices = list(df.index)
    random.shuffle(indices)
    total_records = len(indices)
    logger.info(f"{name} stream generator shuffled {total_records} unique records.")

    outage = False
    outage_end_time = None
    sent_records = 0

    while sent_records < total_records:
        if outage:
            if datetime.utcnow() >= outage_end_time:
                outage = False
                logger.info(f"{name} stream outage ended, resuming.")
            else:
                # During outage, yield empty batch and sleep 1 sec
                yield []
                time.sleep(1)
                continue
        else:
            # Randomly start an outage
            if random.random() < outage_chance:
                outage = True
                outage_length = random.uniform(*outage_duration_range)
                outage_end_time = datetime.utcnow() + timedelta(seconds=outage_length)
                logger.warning(f"{name} stream outage started for {outage_length:.1f} seconds")
                yield []
                time.sleep(1)
                continue

        # Determine number of records to send this iteration
        if random.random() < spike_chance:
            num_to_send = random.randint(base_rate, max_spike)
            logger.info(f"{name} stream spike: sending {num_to_send} records")
        else:
            # Normally send base_rate records (usually 1)
            num_to_send = base_rate

        # Adjust if near end of data
        remaining = total_records - sent_records
        num_to_send = min(num_to_send, remaining)

        batch = []
        for _ in range(num_to_send):
            idx = indices[sent_records]
            record = df.loc[idx].to_dict()
            record['event_timestamp'] = datetime.utcnow().isoformat() + 'Z'
            batch.append(json.dumps(record))
            sent_records += 1

        yield batch

        # Sleep roughly 1 second with jitter
        time.sleep(max(1 + random.uniform(-0.3, 0.3), 0.1))

    logger.info(f"{name} stream generator exhausted all unique records.")


def send_batch_to_kinesis(batch, kinesis_client, stream_name, source_name):
    if not batch:
        logger.info(f"No records to send for {source_name} (possible outage).")
        return
    records = []
    for record_str in batch:
        partition_key = str(random.randint(1, 1000000))
        records.append({'Data': record_str.encode('utf-8'), 'PartitionKey': partition_key})
    max_records_per_call = 500
    for i in range(0, len(records), max_records_per_call):
        chunk = records[i:i+max_records_per_call]
        try:
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=chunk
            )
            failed_count = response.get('FailedRecordCount', 0)
            if failed_count > 0:
                logger.warning(f"{failed_count} records failed to put to Kinesis for {source_name} batch")
            logger.info(f"Sent {len(chunk)} records to Kinesis for {source_name}")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error sending {source_name} records to Kinesis: {e}")
            time.sleep(5)