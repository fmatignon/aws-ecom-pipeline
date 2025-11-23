# Environment Variables Configuration

Copy this file to `.env` and fill in your actual values.

```bash
# ============================================================================
# AWS Credentials (Required for CDK deployment and AWS SDK operations)
# ============================================================================
# These are required before running 'cdk deploy'
# Get these from AWS IAM Console -> Users -> Your User -> Security Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# AWS Region (defaults to us-east-1 if not set)
AWS_DEFAULT_REGION=us-east-1

# AWS Account ID (optional - CDK can auto-detect, but good to set explicitly)
# Find this in AWS Console top-right corner or via: aws sts get-caller-identity
AWS_ACCOUNT_ID=your_aws_account_id

# ============================================================================
# RDS Database Configuration (Required AFTER infrastructure deployment)
# ============================================================================
# These values are needed for both data generation (ECS) and local development.
#
# After running 'cdk deploy', you'll get the RDS secret ARN from the stack outputs.
# The data generation service (ECS) and ingestion pipeline both retrieve ALL database
# credentials (host, port, database, username, password) from AWS Secrets Manager.
#
# Required: Set the RDS secret ARN (get this from CDK stack outputs after deployment)
RDS_SECRET_ARN=arn:aws:secretsmanager:us-east-1:123456789012:secret:ecom-rds-credentials-xxxxx

# ============================================================================
# S3 Configuration (Optional - has defaults)
# ============================================================================
# S3 bucket name (defaults to 'aws-ecom-pipeline' if not set)
# Used by: source-systems/s3/transform_to_json.py
S3_BUCKET_NAME=aws-ecom-pipeline

# ============================================================================
# Lambda Environment Variables (Set Automatically by CDK)
# ============================================================================
# These are NOT set in .env - they are automatically configured by CDK
# when deploying the infrastructure. See infrastructure/api/api_stack.py
# for how these are set:
#
# - S3_BUCKET_NAME: Set from the S3 stack bucket name
# - PAYMENTS_API_KEY: Hardcoded in CDK as 'demo-payments-api-key-12345'
# - SHIPMENTS_API_KEY: Hardcoded in CDK as 'demo-shipments-api-key-67890'
#
# To use these APIs, you'll need the API Key ID from CDK outputs.
# The API keys themselves are hardcoded in the Lambda functions for demo purposes.

# ================================================================================
# Optional Overrides
# ================================================================================
# Uncomment to reuse an existing payments/shipments API key secret instead
# of letting the API stack create a new one automatically.
# PAYMENTS_API_SECRET_NAME=ecom-payments-api-key
# When supplying your own key via the reusable secret, ensure the value is alphanumeric
# and at least 20 characters, e.g.:
# PAYMENTS_API_SECRET_NAME=ecom-payments-api-key

# Override default ingestion load type (FULL or INCREMENTAL)
# LOAD_TYPE=INCREMENTAL
```

