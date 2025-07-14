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