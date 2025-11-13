"""
API Gateway and Lambda Stack for Payments and Shipments APIs
"""
from aws_cdk import (
    Stack,
    Duration,
    Tags,
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_iam as iam,
    CfnOutput,
    BundlingOptions,
    DockerImage,
)
from constructs import Construct
from pathlib import Path


class APIStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Create IAM role for Lambda functions
        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Grant S3 read permissions to Lambda
        s3_bucket.grant_read(lambda_role)

        # Lambda code directory
        lambda_dir = Path(__file__).parent.parent.parent / "source-systems" / "lambda"
        
        # Use standard Python image for bundling (Lambda image has special entrypoint)
        bundling_image = DockerImage.from_registry("python:3.11-slim")
        
        # Bundling command to install dependencies for x86_64 and copy lambda function files
        bundling_command = [
            "bash", "-c",
            "pip install --no-cache-dir --platform manylinux2014_x86_64 --only-binary=:all: -r requirements.txt -t /asset-output && "
            "cp /asset-input/payments_api.py /asset-output/ && "
            "cp /asset-input/shipments_api.py /asset-output/"
        ]
        
        # Create Lambda function for Payments API
        payments_lambda = lambda_.Function(
            self,
            "PaymentsApiFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="payments_api.lambda_handler",
            code=lambda_.Code.from_asset(
                str(lambda_dir),
                bundling=BundlingOptions(
                    image=bundling_image,
                    command=bundling_command,
                ),
            ),
            role=lambda_role,
            timeout=Duration.seconds(60),  # Increased for Parquet file processing
            memory_size=512,  # Sufficient memory for pyarrow operations
            environment={
                "S3_BUCKET_NAME": s3_bucket.bucket_name,
                "PAYMENTS_API_KEY": "demo-payments-api-key-12345",  # Hardcoded demo key
            },
        )

        # Create Lambda function for Shipments API
        shipments_lambda = lambda_.Function(
            self,
            "ShipmentsApiFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="shipments_api.lambda_handler",
            code=lambda_.Code.from_asset(
                str(lambda_dir),
                bundling=BundlingOptions(
                    image=bundling_image,
                    command=bundling_command,
                ),
            ),
            role=lambda_role,
            timeout=Duration.seconds(60),  # Increased for Parquet file processing
            memory_size=512,  # Sufficient memory for pyarrow operations
            environment={
                "S3_BUCKET_NAME": s3_bucket.bucket_name,
                "SHIPMENTS_API_KEY": "demo-shipments-api-key-67890",  # Hardcoded demo key
            },
        )

        # Create API Gateway REST API
        api = apigateway.RestApi(
            self,
            "EcomApi",
            rest_api_name="E-commerce Source Systems API",
            description="API for payments and shipments data",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "X-Amz-Date", "Authorization", "X-Api-Key"],
            ),
        )

        # Create API Key
        api_key = apigateway.ApiKey(
            self,
            "ApiKey",
            api_key_name="ecom-api-key",
        )

        # Create Usage Plan and associate with API stage
        usage_plan = api.add_usage_plan(
            "UsagePlan",
            name="ecom-usage-plan",
            throttle=apigateway.ThrottleSettings(
                rate_limit=100,
                burst_limit=200,
            ),
            api_stages=[
                apigateway.UsagePlanPerApiStage(
                    api=api,
                    stage=api.deployment_stage,
                )
            ],
        )

        # Associate API Key with Usage Plan
        usage_plan.add_api_key(api_key)

        # Create API Gateway integrations for Lambda functions
        payments_integration = apigateway.LambdaIntegration(
            payments_lambda,
            request_templates={"application/json": '{"statusCode": "200"}'},
        )

        shipments_integration = apigateway.LambdaIntegration(
            shipments_lambda,
            request_templates={"application/json": '{"statusCode": "200"}'},
        )

        # Add resources and methods
        payments_resource = api.root.add_resource("payments")
        payments_resource.add_method(
            "GET",
            payments_integration,
            api_key_required=True,
        )

        shipments_resource = api.root.add_resource("shipments")
        shipments_resource.add_method(
            "GET",
            shipments_integration,
            api_key_required=True,
        )

        # Usage plan is automatically associated with the API stage
        # The throttle settings apply to all methods in the usage plan

        # Outputs
        CfnOutput(
            self,
            "ApiEndpoint",
            value=api.url,
            description="API Gateway endpoint URL",
        )

        CfnOutput(
            self,
            "ApiKeyId",
            value=api_key.key_id,
            description="API Key ID",
        )

        CfnOutput(
            self,
            "PaymentsEndpoint",
            value=f"{api.url}payments",
            description="Payments API endpoint",
        )

        CfnOutput(
            self,
            "ShipmentsEndpoint",
            value=f"{api.url}shipments",
            description="Shipments API endpoint",
        )

