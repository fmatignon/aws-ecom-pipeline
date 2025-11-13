"""
S3 Bucket Stack for E-commerce Pipeline
"""
from aws_cdk import (
    Stack,
    Duration,
    Tags,
    aws_s3 as s3,
    CfnOutput,
    RemovalPolicy,
)
from constructs import Construct


class S3Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Create S3 bucket for e-commerce pipeline
        bucket = s3.Bucket(
            self,
            "EcomPipelineBucket",
            bucket_name="aws-ecom-pipeline",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
            auto_delete_objects=True,  # For development - remove for production
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    enabled=True,
                    expiration=Duration.days(365),  # Keep objects for 1 year
                )
            ],
        )

        # Outputs
        CfnOutput(
            self,
            "BucketName",
            value=bucket.bucket_name,
            description="S3 bucket name for e-commerce pipeline",
        )

        CfnOutput(
            self,
            "BucketArn",
            value=bucket.bucket_arn,
            description="S3 bucket ARN",
        )

        # Store reference for other stacks
        self.bucket = bucket

