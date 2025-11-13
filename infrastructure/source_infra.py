#!/usr/bin/env python3
"""
AWS CDK App for E-commerce Source Systems Infrastructure
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import aws_cdk as cdk
from rds.rds_stack import RDSStack
from s3.s3_stack import S3Stack
from api.api_stack import APIStack

# Load environment variables from .env file (in project root, one level up)
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    
    # Set AWS environment variables in os.environ so they're available to AWS CLI/CDK CLI
    # This ensures child processes can access the credentials
    # Note: load_dotenv already sets os.environ, but we ensure AWS vars are explicitly set
    aws_vars = ['AWS_PROFILE', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION', 'AWS_ACCOUNT_ID']
    for var in aws_vars:
        value = os.getenv(var)
        if value:
            os.environ[var] = value.strip() if isinstance(value, str) else value
else:
    print(f"Warning: .env file not found at {env_path}", file=sys.stderr)

app = cdk.App()

# Get environment variables
env = cdk.Environment(
    account=os.getenv('AWS_ACCOUNT_ID', os.getenv('CDK_DEFAULT_ACCOUNT')),
    region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
)

# Create stacks
s3_stack = S3Stack(app, "EcomS3Stack", env=env)
rds_stack = RDSStack(app, "EcomRDSStack", env=env)
api_stack = APIStack(
    app, 
    "EcomAPIStack", 
    s3_bucket=s3_stack.bucket,
    env=env
)

app.synth()

