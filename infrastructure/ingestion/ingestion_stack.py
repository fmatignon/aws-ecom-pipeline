"""
Ingestion Stack for data lake bronze tier updates.

Creates Lambda function with IAM permissions and CloudWatch monitoring.
Scheduling is handled by the orchestration stack.
"""

from aws_cdk import (
    Stack,
    Duration,
    Size,
    Tags,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_ecr_assets as ecr_assets,
    CfnOutput,
)
from constructs import Construct
from pathlib import Path


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket,
        rds_secret,
        api_endpoints: dict = None,
        api_key_secret=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Lambda code directory
        ingestion_dir = Path(__file__).parent.parent.parent / "ingestion"

        # Create IAM role for Lambda function
        lambda_role = iam.Role(
            self,
            "IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Grant permissions for data operations
        s3_bucket.grant_read_write(
            lambda_role
        )  # Read source data, write bronze data and logs

        # Grant access to RDS secret
        rds_secret.grant_read(lambda_role)

        # Grant access to API key secret
        if api_key_secret:
            api_key_secret.grant_read(lambda_role)

        # Use container image to avoid 250MB zip limit (container images support up to 10GB)
        # This is necessary for large data science libraries (pandas, pyarrow, awswrangler)
        ingestion_lambda = lambda_.DockerImageFunction(
            self,
            "IngestionFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                directory=str(ingestion_dir),
                platform=ecr_assets.Platform.LINUX_AMD64,  # Lambda runs on x86_64
            ),
            architecture=lambda_.Architecture.X86_64,  # Explicitly set architecture
            role=lambda_role,
            timeout=Duration.minutes(15),  # Allow time for data processing
            memory_size=3008,  # Maximum Lambda memory to handle concurrent processing and large datasets
            ephemeral_storage_size=Size.mebibytes(1024),  # 1GB ephemeral storage
            environment={
                "S3_BUCKET_NAME": s3_bucket.bucket_name,
                "RDS_SECRET_ARN": rds_secret.secret_arn,
                "PAYMENTS_API_URL": api_endpoints.get("payments", "")
                if api_endpoints
                else "",
                "SHIPMENTS_API_URL": api_endpoints.get("shipments", "")
                if api_endpoints
                else "",
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn
                if api_key_secret
                else "",
                "LOAD_TYPE": "INCREMENTAL",  # Default to incremental, can be overridden
            },
        )

        # Create CloudWatch Log Group with retention
        log_group = logs.LogGroup(
            self,
            "IngestionLogGroup",
            log_group_name=f"/aws/lambda/{ingestion_lambda.function_name}",
            retention=logs.RetentionDays.ONE_MONTH,
        )


        # Create CloudWatch alarms for monitoring
        self.error_alarm = cloudwatch.Alarm(
            self,
            "IngestionErrorAlarm",
            alarm_name="IngestionPipelineErrors",
            alarm_description="Alert when ingestion Lambda has errors",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Errors",
                dimensions_map={"FunctionName": ingestion_lambda.function_name},
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        self.duration_alarm = cloudwatch.Alarm(
            self,
            "IngestionDurationAlarm",
            alarm_name="IngestionPipelineDuration",
            alarm_description="Alert when ingestion takes too long",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Duration",
                dimensions_map={"FunctionName": ingestion_lambda.function_name},
                statistic="Maximum",
            ),
            threshold=800000,  # 800 seconds = 13.3 minutes (80% of 15 min timeout)
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        )

        # Outputs
        CfnOutput(
            self,
            "IngestionFunctionArn",
            value=ingestion_lambda.function_arn,
            description="ARN of the ingestion Lambda function",
        )

        CfnOutput(
            self,
            "IngestionFunctionName",
            value=ingestion_lambda.function_name,
            description="Name of the ingestion Lambda function",
        )


        CfnOutput(
            self,
            "LogGroupName",
            value=log_group.log_group_name,
            description="CloudWatch Log Group for ingestion logs",
        )

        # Store reference for other stacks if needed
        self.ingestion_lambda = ingestion_lambda
