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
# These values are needed to run the data loading script (source-systems/rds/load_to_rds.py)
# 
# After running 'cdk deploy', you'll get the RDS endpoint from the stack outputs.
# The database credentials are stored in AWS Secrets Manager, but you can also
# set them directly here for convenience.
#
# Option 1: Use Secrets Manager (recommended for production)
# RDS_SECRET_ARN=arn:aws:secretsmanager:us-east-1:123456789012:secret:ecom-rds-credentials-xxxxx
#
# Option 2: Set credentials directly (easier for development)
RDS_ENDPOINT=your-rds-endpoint.region.rds.amazonaws.com
RDS_DATABASE_NAME=ecommerce
RDS_USERNAME=admin
RDS_PASSWORD=your_password_here

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
```

