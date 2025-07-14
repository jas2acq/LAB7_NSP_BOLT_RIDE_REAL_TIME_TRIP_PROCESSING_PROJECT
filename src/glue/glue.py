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