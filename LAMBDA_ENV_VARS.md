# How Lambda Environment Variables Are Set Automatically

## Overview

Lambda functions get their environment variables automatically configured by AWS CDK during deployment. You don't need to set them in your `.env` file because CDK handles this for you.

## How It Works

### 1. CDK Stack Configuration

In `infrastructure/api/api_stack.py`, when we create the Lambda functions, we pass an `environment` dictionary:

```python
payments_lambda = lambda_.Function(
    self,
    "PaymentsApiFunction",
    runtime=lambda_.Runtime.PYTHON_3_11,
    handler="payments_api.lambda_handler",
    code=lambda_.Code.from_asset("source-systems/lambda"),
    role=lambda_role,
    timeout=Duration.seconds(30),
    environment={
        "S3_BUCKET_NAME": s3_bucket.bucket_name,  # ← Dynamically set from S3 stack
        "PAYMENTS_API_KEY": "demo-payments-api-key-12345",  # ← Hardcoded value
    },
)
```

### 2. What Happens During Deployment

When you run `cdk deploy`:

1. **CDK synthesizes the CloudFormation template** - It converts your Python code into AWS CloudFormation JSON/YAML
2. **The `environment` dictionary becomes Lambda environment variables** - Each key-value pair in the dictionary becomes an environment variable in the Lambda function
3. **AWS creates/updates the Lambda function** - The environment variables are set at the Lambda function level in AWS

### 3. Runtime Access

When your Lambda function runs, it can access these environment variables using:

```python
import os

# In payments_api.py
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
VALID_API_KEY = os.environ.get('PAYMENTS_API_KEY')
```

## Environment Variables Set Automatically

### Payments Lambda Function

- **`S3_BUCKET_NAME`**: Automatically set to the bucket name from the S3 stack (`aws-ecom-pipeline`)
- **`PAYMENTS_API_KEY`**: Hardcoded as `'demo-payments-api-key-12345'` in the CDK code

### Shipments Lambda Function

- **`S3_BUCKET_NAME`**: Automatically set to the bucket name from the S3 stack (`aws-ecom-pipeline`)
- **`SHIPMENTS_API_KEY`**: Hardcoded as `'demo-shipments-api-key-67890'` in the CDK code

## Why This Approach?

### Advantages

1. **No manual configuration** - Environment variables are set automatically during deployment
2. **Consistency** - All Lambda functions get the same bucket name automatically
3. **Infrastructure as Code** - Everything is version-controlled and reproducible
4. **Dynamic values** - The S3 bucket name is resolved at deployment time, so if you change the bucket name in CDK, Lambda automatically gets the new value

### How Values Are Resolved

- **`s3_bucket.bucket_name`**: This is a CDK reference that gets resolved to the actual bucket name when the stack is deployed. If you change the bucket name in `s3_stack.py`, the Lambda functions automatically get the new name.

- **Hardcoded strings**: These are literal values that are set directly in the Lambda function's environment.

## Viewing Environment Variables

After deployment, you can view the environment variables in the AWS Console:

1. Go to AWS Lambda Console
2. Find your function (e.g., `EcomAPIStack-PaymentsApiFunction-xxxxx`)
3. Go to the "Configuration" tab
4. Click on "Environment variables" in the left sidebar

You'll see:
```
S3_BUCKET_NAME = aws-ecom-pipeline
PAYMENTS_API_KEY = demo-payments-api-key-12345
```

## Changing Environment Variables

To change Lambda environment variables:

1. **Edit `infrastructure/api/api_stack.py`** - Modify the `environment` dictionary
2. **Redeploy** - Run `cdk deploy` again
3. **CDK updates the Lambda function** - The new environment variables are applied automatically

## Important Notes

- **Lambda environment variables are NOT read from `.env` files** - They're set by CDK/CloudFormation
- **Your local scripts** (like `transform_to_json.py` and `load_to_rds.py`) use `.env` because they run on your local machine
- **Lambda functions run in AWS** - They get their environment variables from the Lambda function configuration, not from `.env` files

## Summary

```
┌─────────────────────────────────────────────────────────────┐
│  Your Local Machine (.env file)                             │
│  ├─ AWS_ACCESS_KEY_ID                                       │
│  ├─ AWS_SECRET_ACCESS_KEY                                   │
│  ├─ RDS_ENDPOINT                                            │
│  └─ ...                                                     │
│       ↓ Used by                                             │
│       ├─ CDK deploy (reads AWS credentials)                │
│       ├─ transform_to_json.py (reads S3_BUCKET_NAME)        │
│       └─ load_to_rds.py (reads RDS credentials)            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  AWS Lambda Functions (Environment Variables)                │
│  ├─ S3_BUCKET_NAME (set by CDK from S3 stack)              │
│  ├─ PAYMENTS_API_KEY (hardcoded in CDK)                     │
│  └─ SHIPMENTS_API_KEY (hardcoded in CDK)                   │
│       ↓ Set automatically during                             │
│       └─ cdk deploy                                          │
└─────────────────────────────────────────────────────────────┘
```

