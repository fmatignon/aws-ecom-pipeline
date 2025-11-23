#!/usr/bin/env python3
"""
AWS CDK App for E-commerce Source Systems Infrastructure
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import aws_cdk as cdk
from vpc.vpc_stack import VPCStack
from rds.rds_stack import RDSStack
from s3.s3_stack import S3Stack
from api.api_stack import APIStack
from operations.operations_stack import OperationsStack
from ingestion.ingestion_stack import IngestionStack
from orchestration.orchestration_stack import EcomOrchestrationStack

# Load environment variables from .env file (in project root, one level up)
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

    # Set AWS environment variables in os.environ so they're available to AWS CLI/CDK CLI
    # This ensures child processes can access the credentials
    # Note: load_dotenv already sets os.environ, but we ensure AWS vars are explicitly set
    aws_vars = [
        "AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
        "AWS_ACCOUNT_ID",
    ]
    for var in aws_vars:
        value = os.getenv(var)
        if value:
            os.environ[var] = value.strip() if isinstance(value, str) else value
else:
    print(f"Warning: .env file not found at {env_path}", file=sys.stderr)

app = cdk.App()

# Get environment variables
env = cdk.Environment(
    account=os.getenv("AWS_ACCOUNT_ID", os.getenv("CDK_DEFAULT_ACCOUNT")),
    region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)

# Create stacks in dependency order
vpc_stack = VPCStack(app, "EcomVPCStack", env=env)
s3_stack = S3Stack(app, "EcomS3Stack", env=env)

# For existing deployments, RDS stack can work without VPC parameter (backward compatibility)
# Uncomment the line below to use new VPC architecture:
# rds_stack = RDSStack(app, "EcomRDSStack", vpc=vpc_stack.vpc, env=env)
# For existing deployments, use this (RDS creates its own VPC):
rds_stack = RDSStack(app, "EcomRDSStack", env=env)
api_stack = APIStack(app, "EcomSourceAPIStack", s3_bucket=s3_stack.bucket, env=env)

operations_stack = OperationsStack(
    app,
    "EcomOperationsStack",
    s3_bucket=s3_stack.bucket,
    rds_database=rds_stack.database,
    rds_secret=rds_stack.db_secret,
    # vpc parameter optional - gets from RDS instance if not provided
    env=env,
)

ingestion_stack = IngestionStack(
    app,
    "EcomIngestionStack",
    s3_bucket=s3_stack.bucket,
    rds_secret=rds_stack.db_secret,
    api_endpoints={
        "payments": api_stack.payments_endpoint,
        "shipments": api_stack.shipments_endpoint,
    },
    api_key_secret=api_stack.api_key_secret,
    env=env,
)

# Create orchestration stack that coordinates operations and ingestion
orchestration_stack = EcomOrchestrationStack(
    app,
    "EcomOrchestrationStack",
    operations_stack=operations_stack,
    ingestion_stack=ingestion_stack,
    env=env,
)

app.synth()
