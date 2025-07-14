# NSP Bolt Ride - Setup & Deployment Guide

This guide covers setting up the environment, deploying the pipeline, and testing the NSP Bolt Ride real-time trip processing system. 

## 1. Environment Setup

### Prerequisites

- Python 3.9+ and 3.12+ installed
- AWS CLI configured with valid credentials
- Git installed
- AWS account with permissions to create:
  - Kinesis streams
  - Lambda functions
  - DynamoDB tables
  - EventBridge rules
  - Glue jobs
  - S3 buckets
  - CloudWatch logs

### Steps

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd LAB7_NSP_BOLT_RIDE_REAL_TIME_TRIP_PRO...
   ```

2. **Install Dependencies**:

   - For Lambda: `pip install -r src/lambda/requirements.txt` (Python 3.9)
   - For Simulator: `pip install boto3 pandas dotenv` (Python 3.12)
   - Configure `src/stream_simulator/stream_env/env` with environment variables.

3. **Configure Environment Variables**:

   - Create a `.env` file in `src/stream_simulator/stream_env/`:

     ```env
     AWS_ACCESS_KEY_ID=<your-key>
     AWS_SECRET_ACCESS_KEY=<your-secret>
     AWS_REGION=eu-north-1
     KINESIS_STREAM_NAME=<stream-name>
     DYNAMODB_TABLE_NAME=<table-name>
     S3_BUCKET_NAME=lab7-stream-project
     ```
   - Configure GitHub Secrets (`Settings > Secrets and variables > Actions`):
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`
     - `AWS_REGION`
     - `PROJECT_BUCKET` (set to `lab7-stream-project`)
     - `LAMBDA_FUNCTION_NAME`
     - `KINESIS_STREAM_NAME`

4. **Create AWS Resources** (manual or via IaC):

   - **Kinesis Stream**: Create `TripDataStream` with 1 shard.
   - **DynamoDB Table**: Create `TripData` with `trip_id` (string) as the partition key.
   - **S3 Bucket**: Create `lab7-stream-project` with `kpi/`, `logs/glue/`, and `state/` prefixes.
   - **EventBridge Rule**: Create a daily schedule rule to trigger Glue.
   - **IAM Roles**:
     - Lambda role with Kinesis, DynamoDB, CloudWatch access.
     - Glue role with DynamoDB, S3, Glue catalog access.
   - (Optional) Use Terraform or CloudFormation.

## 2. Secrets Management

- Add to GitHub Secrets:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`
  - `PROJECT_BUCKET` (set to `lab7-stream-project`)
  - `LAMBDA_FUNCTION_NAME`
  - `KINESIS_STREAM_NAME`
- Reference in `.github/workflows/` for CI/CD.

## 3. Lambda Deployment

1. **Package Lambda Function**:
   - Navigate to `src/lambda/`.
   - Run: `pip install -r requirements.txt -t . && zip -r ../../lambda-deployment.zip .`
2. **Deploy via GitHub Actions**:
   - Push changes to `main` branch or trigger `workflow_dispatch` in `.github/workflows/deploy_lambda.yml`.
   - The workflow packages the Lambda code, validates dependencies, uploads to `s3://lab7-stream-project/scripts/lambda/lambda-deployment.zip`, and updates the Lambda function with rollback if needed.
3. **Test Lambda**:
   - Trigger via Kinesis or AWS Console.

## 4. Glue Job Setup

1. **Upload Glue Scripts**:
   - Push changes to `main` branch or trigger `workflow_dispatch` in `.github/workflows/deploy_glue.yml`.
   - The workflow validates script syntax, uploads `src/glue/` to `s3://lab7-stream-project/scripts/glue_scripts/`, and optionally cleans up old scripts.
2. **Create Glue Job**:
   - In AWS Glue Console:
     - Type: Spark
     - Script: `s3://lab7-stream-project/scripts/glue_scripts/glue.py`
     - IAM Role: Glue role
     - Schedule: Link to EventBridge daily rule
3. **Parameters**:
   - Set `DYNAMODB_TABLE_NAME` as `TripData` and `S3_BUCKET` as `lab7-stream-project`.

## 5. Testing Locally

1. **Simulate Trip Events**:
   - Run: `python src/stream_simulator/stream_simulate.py` with optional `--duration` parameter.
   - Use `src/data/trip_start.csv` and `trip_end.csv` as input.
2. **Verify DynamoDB**:
   - Check `TripData` table in AWS Console.
3. **Check S3 Output**:
   - Confirm KPI JSON in `s3://lab7-stream-project/kpi/YYYY/MM/DD/YYYY-MM-DD.json`.
   - Check Glue logs in `s3://lab7-stream-project/logs/glue/`.
4. **Monitor Logs**:
   - Check `logs/streaming.log` for local logs (rotating file with 5MB max size).
   - Use CloudWatch for AWS logs.
   - Download simulation logs from GitHub Actions artifacts.

## 

## 6. Troubleshooting

- **Kinesis Lag**: Increase shard count or adjust Lambda batch size.
- **DynamoDB Write Capacity**: Scale table capacity if throttled.
- **Glue Job Failures**: Check logs in `s3://lab7-stream-project/logs/glue/` for schema mismatches or S3 issues.
- **Deployment Issues**: Review GitHub Actions logs for validation or rollback failures.

---