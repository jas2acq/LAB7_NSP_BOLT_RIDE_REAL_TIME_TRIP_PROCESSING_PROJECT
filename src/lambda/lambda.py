import json
import boto3
import logging
import uuid
import base64
import os
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Explicitly set region for boto3 clients/resources
REGION = os.environ.get('AWS_REGION', 'eu-north-1')

dynamodb = boto3.client('dynamodb', region_name=REGION)
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)

TABLE_NAME = 'TripData'
ERROR_TABLE_NAME = 'TripDataErrors'


def create_table_if_not_exists(table_name, key_name):
    try:
        response = dynamodb.describe_table(TableName=table_name)
        status = response['Table']['TableStatus']
        logger.info(f"Table {table_name} already exists with status {status}.")
        if status != 'ACTIVE':
            logger.info(f"Waiting for table {table_name} to become ACTIVE...")
            dynamodb.get_waiter('table_exists').wait(TableName=table_name)
            logger.info(f"Table {table_name} is now ACTIVE.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.info(f"Table {table_name} not found. Creating...")
            try:
                dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=[{'AttributeName': key_name, 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': key_name, 'AttributeType': 'S'}],
                    BillingMode='PAY_PER_REQUEST'
                )
                dynamodb.get_waiter('table_exists').wait(TableName=table_name)
                logger.info(f"Created table {table_name} with on-demand billing and it is ACTIVE.")
            except ClientError as create_error:
                logger.error(f"Failed to create table {table_name}: {create_error}", exc_info=True)
                raise
        else:
            logger.error(f"Error checking table existence: {e}", exc_info=True)
            raise

def convert_floats_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def is_blank(val):
    # Returns True if val is None, empty string, only whitespace, or 'null'
    return val is None or (isinstance(val, str) and val.strip() == '') or val == 'null'


def upsert_error_record(error_table, trip_id, reason, original_data):
    timestamp = datetime.utcnow().isoformat() + 'Z'
    try:
        # Convert floats in original_data to Decimal before writing to DynamoDB
        original_data = convert_floats_to_decimal(original_data)

        response = error_table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression="""
                SET error_reasons = list_append(if_not_exists(error_reasons, :empty_list), :new_reason),
                    error_timestamps = list_append(if_not_exists(error_timestamps, :empty_list), :new_timestamp),
                    original_data = :original_data
            """,
            ExpressionAttributeValues={
                ':new_reason': [reason],
                ':new_timestamp': [timestamp],
                ':empty_list': [],
                ':original_data': original_data
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"Upserted error record for trip_id {trip_id} with reason: {reason}")
    except Exception as e:
        logger.error(f"Failed to upsert error record for trip_id {trip_id}: {e}", exc_info=True)
        raise

def validate_record(data):
    trip_id = data.get('trip_id')
    if is_blank(trip_id) or not isinstance(trip_id, str):
        return False, "Missing or invalid trip_id"

    # Strict validation for trip_start
    if 'pickup_location_id' in data:
        required_fields = [
            'trip_id',
            'pickup_location_id',
            'dropoff_location_id',
            'vendor_id',
            'pickup_datetime'
        ]
        for field in required_fields:
            if field not in data or is_blank(data[field]):
                return False, f"Missing, null, or blank required field '{field}' in trip_start"
        return True, "trip_start"

    # Strict validation for trip_end
    elif 'dropoff_datetime' in data:
        required_fields = [
            'trip_id',
            'dropoff_datetime',
            'fare_amount',
            'payment_type',
            'trip_distance'
        ]
        for field in required_fields:
            if field not in data or is_blank(data[field]):
                return False, f"Missing, null, or blank required field '{field}' in trip_end"
        return True, "trip_end"

    else:
        return False, "Unknown record type - missing key fields"
    


def lambda_handler(event, context):
    try:
        if not event.get('Records', []):
            logger.info("Received empty batch from Kinesis, possible outage.")
            return {'statusCode': 200, 'body': json.dumps('No records to process')}

        create_table_if_not_exists(TABLE_NAME, 'trip_id')
        create_table_if_not_exists(ERROR_TABLE_NAME, 'trip_id')

        table = dynamodb_resource.Table(TABLE_NAME)
        error_table = dynamodb_resource.Table(ERROR_TABLE_NAME)

        processed_records = 0
        error_records = 0
        failed_records = 0

        for record in event['Records']:
            try:
                kinesis_data = record['kinesis']['data']
                decoded_data = base64.b64decode(kinesis_data).decode('utf-8')
                data = json.loads(decoded_data)

                is_valid, reason = validate_record(data)
                logger.info(f"Validation result for trip_id {data.get('trip_id')}: {is_valid}, reason: {reason}")

                if not is_valid:
                    trip_id = data.get('trip_id')
                    if is_blank(trip_id) or not isinstance(trip_id, str):
                        trip_id = str(uuid.uuid4())
                    upsert_error_record(error_table, trip_id, reason, data)
                    error_records += 1
                    logger.warning(f"Invalid record upserted in error table: reason={reason}, trip_id={trip_id}")
                    continue

                data = convert_floats_to_decimal(data)
                trip_id = data['trip_id']

                update_expression_parts = []
                expression_attribute_values = {}

                for k, v in data.items():
                    if k == 'trip_id':
                        continue  # skip primary key attribute
                    placeholder = f":{k}"
                    update_expression_parts.append(f"{k} = {placeholder}")
                    expression_attribute_values[placeholder] = v

                update_expression = "SET " + ", ".join(update_expression_parts)

                table.update_item(
                    Key={'trip_id': trip_id},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values
                )

                processed_records += 1
                logger.info(f"Upserted record for trip_id: {trip_id}, event_type: {reason}")

            except (json.JSONDecodeError, base64.binascii.Error) as e:
                logger.error(f"Failed to decode record data: {e}", exc_info=True)
                failed_records += 1
            except Exception as e:
                logger.error(f"Error processing record: {e}", exc_info=True)
                failed_records += 1

        logger.info(f"Processing complete. Processed: {processed_records}, Errors: {error_records}, Failed: {failed_records}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {processed_records} records, '
                               f'logged {error_records} errors, {failed_records} failures')
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }