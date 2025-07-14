# NSP Bolt Ride - Real-Time Trip Processing Pipeline

## Overview

This project implements a scalable, event-driven pipeline for ingesting, processing, and aggregating ride-hailing trip data in near real-time using AWS-native services. It handles trip start and end events, stores data in a NoSQL database, triggers transformations for completed trips, and generates daily KPI metrics stored as structured JSON in Amazon S3. 

The pipeline supports operational analytics for NSP Bolt Ride, a ride-hailing company, by processing trip events and producing key metrics like total fare, trip count, average fare, max fare, min fare, and potentially trip duration. The system includes validation, error handling, and state management to ensure robustness and scalability.

## Architecture

![Architecture Diagram](imgs/lab7-architecture.drawio.svg)### Architecture Description

The pipeline is designed within the AWS Cloud (Region: eu-north-1) and follows these steps:

1. **Stream Source**: Data from the NSP Bolt Ride app is ingested as events.
2. **Amazon Kinesis Data Streams**: Ingests data from the app, providing a scalable stream to handle real-time trip start and end events.
3. **AWS Lambda**: Validates and writes data to `TripData` in DynamoDB, logs errors to `TripDataErrors`, and sends logs to CloudWatch. Lambda handles upsert operations using `trip_id` as the primary key.
4. **Amazon DynamoDB**: Stores streaming data in the `TripData` table and error records in the `TripDataErrors` table.
5. **Amazon EventBridge**: Triggers AWS Glue on a daily schedule to initiate KPI computation.
6. **AWS Glue**: Scans `TripData`, filters completed trips (requiring all fields: trip_id, pickup_location_id, dropoff_location_id, vendor_id, pickup_datetime, dropoff_datetime, fare_amount, payment_type, trip_distance), calculates KPIs, updates a state file (`state/kpi_state.json`), and uploads logs.
7. **Amazon S3**: Houses the computed KPIs as timestamped JSON files (e.g., `kpi/YYYY/MM/DD/YYYY-MM-DD.json`), state files, and Glue logs (e.g., `logs/glue/YYYY-MM-DD-HH-MM-SS-ms.log`).
8. **GitHub Actions**: Deploys Glue scripts, Lambda functions, and runs stream simulations, integrating with S3 and Lambda.

**Key Features**:

- Handles out-of-order events from Kinesis with validation for `trip_start` (pickup_location_id, dropoff_location_id, vendor_id, pickup_datetime) and `trip_end` (dropoff_datetime, fare_amount, payment_type, trip_distance).
- Ensures modularity with separate processing (Lambda) and aggregation (Glue) stages.
- Scales with Kinesis shards, DynamoDB capacity, and Lambda concurrency.
- Validates trip data with strict rules and logs errors with timestamps in `TripDataErrors`.
- Tracks processed dates in a state file to avoid reprocessing, with optional last_updated timestamp.
- Simulates streams with random spikes (up to 100 for trip_start, 50 for trip_end) and outages (10-15% chance).
- Supports rollback and notification via GitHub Actions workflows.

## Features

- ✅ Real-time ingestion of trip start/end events via Kinesis
- 🔍 Validation and enrichment of events in Lambda with datetime and fare checks
- 🔄 Upsert trip data into DynamoDB with `trip_id` as the primary key
- 📊 Daily KPI aggregation (total_fare, count_trips, average_fare, max_fare, min_fare, potential trip_duration) by Glue
- 📦 Structured JSON output in S3, timestamped for efficient access
- ⚖️ Modular and scalable to handle high throughput and out-of-order events
- 🧪 Stream simulation with configurable spikes, outages, and throughput limits
- 🚀 Automated deployment via GitHub Actions with validation and rollback

## Technologies

- **AWS Services**: Kinesis, Lambda, DynamoDB, EventBridge, Glue, S3, CloudWatch
- **Languages/Libraries**: Python, boto3, pandas, dotenv
- **CI/CD**: GitHub Actions for deployment (Python 3.9 for Lambda, 3.12 for Simulator)
- **Testing**: Local stream simulation scripts, potential unit tests

## Prerequisites

- AWS account with credentials configured
- Python 3.9+ and 3.12+
- AWS CLI installed
- IAM roles with permissions for Kinesis, Lambda, DynamoDB, EventBridge, Glue, S3, and CloudWatch
- GitHub repository with secrets configured (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `PROJECT_BUCKET`, `LAMBDA_FUNCTION_NAME`, `KINESIS_STREAM_NAME`)
- Data files: `src/data/trip_start.csv` and `src/data/trip_end.csv` with required fields
- DynamoDB tables: `TripData` and `TripDataErrors` (created automatically if not present)
- S3 bucket: `lab7-stream-project` with `kpi/`, `logs/glue/`, and `state/` prefixes

## Usage

1. **Deploy Infrastructure**:
   - Follow `SETUP.md` to configure resources using GitHub workflows.
   - Trigger `deploy_glue.yml`, `deploy_lambda.yml`, or `deploy_simulator.yml` manually via GitHub Actions UI.
2. **Run Locally**:
   - Use `src/stream_simulator/stream_simulate.py` to simulate trip events from `trip_start.csv` and `trip_end.csv`.
   - Verify data in DynamoDB (`TripData`) and errors in `TripDataErrors`, with logs in `logs/streaming.log`.
3. **Deploy to AWS**:
   - Use `.github/workflows/`:
     - `deploy_glue.yml` to upload Glue scripts to S3 with validation.
     - `deploy_lambda.yml` to package and deploy Lambda functions with rollback.
     - `deploy_simulator.yml` to run stream simulation with configurable duration.
4. **Monitor**:
   - Check `logs/streaming.log` for local logs (rotating file with 5MB max size).
   - Query S3 for KPI JSON files (`kpi/`) and Glue logs (`logs/glue/`), and use CloudWatch for AWS logs.
   - Review GitHub Actions runs for deployment logs or artifacts.

## Project Structure

```text
.
├── .github
│   └── workflows
│       ├── deploy_glue.yml
│       ├── deploy_lambda.yml
│       └── deploy_simulator.yml
├── imgs
│   ├── lab7-architecture.drawio.png
│   └── lab7-architecture.drawio.svg
├── logs
│   └── streaming.log
├── src
│   ├── data
│   │   ├── trip_end.csv
│   │   └── trip_start.csv
│   ├── glue
│   │   └── glue.py
│   ├── lambda
│   │   ├── lambda.py
│   │   └── requirements.txt
│   ├── stream_simulator
│   │   ├── stream_simulate.py
│   │   └── stream_env
│   │       └── env
│   ├── .gitignore
│   └── add.py
├── README.md
└── SETUP.md
```

## Code Overview

- **glue.py**: Computes KPIs for completed trips, manages state file, and uploads logs to S3.
- **lambda.py**: Validates and upserts trip data to DynamoDB, logs errors to `TripDataErrors`.
- **stream_simulate.py**: Simulates trip events to Kinesis with spikes and outages.

## Sample KPI Output

Daily metrics are stored in S3 as `s3://<bucket>/kpi/YYYY/MM/DD/YYYY-MM-DD.json`:

```json
{
  "date": "2025-07-13",
  "total_fare": 1893.50,
  "count_trips": 55,
  "average_fare": 34.42,
  "max_fare": 97.30,
  "min_fare": 9.90,
  "average_trip_duration": 25.5
}
```