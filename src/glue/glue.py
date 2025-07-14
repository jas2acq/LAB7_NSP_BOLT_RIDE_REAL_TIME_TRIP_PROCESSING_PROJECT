import boto3
import json
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from collections import defaultdict
import io

# Set up logging with string buffer
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_buffer = io.StringIO()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
buffer_handler = logging.StreamHandler(log_buffer)
buffer_handler.setLevel(logging.INFO)
buffer_handler.setFormatter(formatter)
logger.addHandler(buffer_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')
TABLE_NAME = 'TripData'
S3_BUCKET = 'lab7-stream-project'
S3_KPI_PREFIX = 'kpi/'
S3_LOG_PREFIX = 'logs/glue/'
S3_STATE_KEY = 'state/kpi_state.json'



def read_state_file():
    """Read processed dates from state file or initialize it."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_STATE_KEY)
        state = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Read state file: {state}")
        return state.get('processed_dates', [])
    except s3.exceptions.NoSuchKey:
        logger.info("State file not found, initializing empty state.")
        return []
    except Exception as e:
        logger.error(f"Failed to read state file: {e}")
        raise

def write_state_file(processed_dates):
    """Write updated processed dates to state file."""
    try:
        state = {'processed_dates': sorted(list(set(processed_dates)))}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_STATE_KEY,
            Body=json.dumps(state, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Updated state file at s3://{S3_BUCKET}/{S3_STATE_KEY}")
    except Exception as e:
        logger.error(f"Failed to write state file: {e}")
        raise


def upload_logs_to_s3():
    """Upload logs to S3."""
    try:
        log_content = log_buffer.getvalue()
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S-%f')
        s3_key = f"{S3_LOG_PREFIX}{timestamp}.log"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=log_content.encode('utf-8'),
            ContentType='text/plain'
        )
        logger.info(f"Uploaded logs to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload logs to S3: {e}")
