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


def scan_dynamodb():
    """Scan DynamoDB table to retrieve all trip records."""
    try:
        records = []
        paginator = dynamodb.get_paginator('scan')
        for page in paginator.paginate(TableName=TABLE_NAME):
            records.extend(page.get('Items', []))
        logger.info(f"Retrieved {len(records)} records from {TABLE_NAME}")
        return records
    except Exception as e:
        logger.error(f"Failed to scan DynamoDB table: {e}")
        raise


def parse_dynamodb_item(item):
    """Parse DynamoDB item into a dictionary with native Python types."""
    def convert_dynamodb_types(value):
        if 'S' in value:
            return value['S']
        elif 'N' in value:
            return Decimal(value['N'])
        elif 'M' in value:
            return {k: convert_dynamodb_types(v) for k, v in value['M'].items()}
        elif 'L' in value:
            return [convert_dynamodb_types(v) for v in value['L']]
        return value
    return {k: convert_dynamodb_types(v) for k, v in item.items()}


def filter_completed_trips(records):
    """
    Filter trips that have all critical columns present and non-null/non-empty.
    Critical columns from trip_start and trip_end combined:
      - trip_id
      - pickup_location_id
      - dropoff_location_id
      - vendor_id
      - pickup_datetime
      - dropoff_datetime
      - fare_amount
      - payment_type
      - trip_distance
    """
    completed_trips_by_date = defaultdict(list)

    required_fields = [
        'trip_id',
        'pickup_location_id',
        'dropoff_location_id',
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'fare_amount',
        'payment_type',
        'trip_distance'
    ]

    for record in records:
        trip = parse_dynamodb_item(record)

        # Check all required fields exist and are not null/empty
        if all(field in trip and trip[field] not in (None, '', 'null') for field in required_fields):

            # Validate fare_amount is positive number
            try:
                fare_amount = Decimal(str(trip['fare_amount']))
                if fare_amount <= 0:
                    logger.warning(f"Skipping trip_id {trip.get('trip_id')} due to non-positive fare_amount")
                    continue
            except Exception as e:
                logger.warning(f"Skipping trip_id {trip.get('trip_id')} due to invalid fare_amount: {e}")
                continue

            # Parse dropoff date
            try:
                dropoff_datetime_str = trip['dropoff_datetime']
                if dropoff_datetime_str.endswith('Z'):
                    dropoff_datetime_str = dropoff_datetime_str[:-1]
                dropoff_date = datetime.fromisoformat(dropoff_datetime_str).date().isoformat()
            except Exception as e:
                logger.warning(f"Invalid dropoff_datetime for trip_id {trip.get('trip_id')}: {e}")
                continue

            completed_trips_by_date[dropoff_date].append(trip)
        else:
            logger.warning(f"Incomplete or invalid trip data for trip_id {trip.get('trip_id')}")

    logger.info(f"Found completed trips for {len(completed_trips_by_date)} days")
    return completed_trips_by_date


def calculate_kpis(trips_by_date):
    """Calculate KPIs for each day."""
    kpis = {}
    for date, trips in trips_by_date.items():
        fares = []
        for trip in trips:
            try:
                fares.append(Decimal(str(trip['fare_amount'])))
            except Exception as e:
                logger.warning(f"Skipping trip with invalid fare_amount on {date}: {e}")

        if not fares:
            logger.info(f"No valid fares for {date}, skipping KPIs")
            continue

        total_fare = sum(fares)
        count_trips = len(fares)
        average_fare = total_fare / count_trips if count_trips > 0 else Decimal('0')
        max_fare = max(fares)
        min_fare = min(fares)

        kpis[date] = {
            'date': date,
            'total_fare': float(total_fare),
            'count_trips': count_trips,
            'average_fare': float(average_fare),
            'max_fare': float(max_fare),
            'min_fare': float(min_fare)
        }
        logger.info(f"Calculated KPIs for {date}: {kpis[date]}")
    return kpis


def write_kpis_to_s3(kpis):
    """Write KPIs to S3 as JSON files."""
    for date, kpi in kpis.items():
        try:
            year, month, day = date.split('-')
            s3_key = f"{S3_KPI_PREFIX}{year}/{month}/{day}/{date}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(kpi, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Wrote KPIs to s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to write KPIs for {date} to S3: {e}")
            raise