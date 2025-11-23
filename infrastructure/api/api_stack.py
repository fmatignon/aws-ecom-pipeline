"""
API Stack for Payments and Shipments Lambda APIs.

Creates Lambda functions for payments and shipments APIs,
exposes them via API Gateway with API key authentication,
and manages the API key secret for integration with other stacks.
"""

from aws_cdk import (
    Stack,
    Duration,
    Tags,
    CfnOutput,
    CustomResource,
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_logs as logs,
    BundlingOptions,
    DockerImage,
)
from constructs import Construct
from pathlib import Path


class APIStack(Stack):
    """
    Stack that creates Lambda-backed APIs for payments and shipments data.

    Creates:
    - Lambda functions for payments and shipments APIs
    - API Gateway REST API with API key authentication
    - Secrets Manager secret for API key
    - IAM roles and permissions
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Lambda code directory
        lambda_dir = Path(__file__).parent.parent.parent / "source-systems" / "lambda"

        # Create API key secret (empty initially, will be populated by Custom Resource)
        # We generate the password in the Lambda to ensure it's exactly 48 characters
        api_key_secret = secretsmanager.Secret(
            self,
            "ApiKeySecret",
            secret_name="ecom-payments-api-key",
            description="API key for payments and shipments APIs",
        )

        # Create IAM role for Lambda functions
        lambda_role = iam.Role(
            self,
            "ApiLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Grant S3 read permissions for source data
        s3_bucket.grant_read(lambda_role)

        # Grant Secrets Manager read permissions
        api_key_secret.grant_read(lambda_role)

        # Use standard Python image for bundling
        bundling_image = DockerImage.from_registry("python:3.11-slim")

        # Bundling command to install dependencies for x86_64 and copy Lambda files
        bundling_command = [
            "bash",
            "-c",
            "pip install --no-cache-dir --platform manylinux2014_x86_64 --only-binary=:all: -r requirements.txt -t /asset-output && "
            "cp -r /asset-input/* /asset-output/",
        ]

        # Common Lambda configuration
        lambda_environment = {
            "S3_BUCKET_NAME": s3_bucket.bucket_name,
            "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
        }

        # Create Payments Lambda function
        # Increased timeout to handle large date ranges with pagination
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
            timeout=Duration.minutes(15),  # Max Lambda timeout to handle large queries
            memory_size=1024,  # Increased memory for processing large datasets
            environment=lambda_environment,
        )

        # Create Shipments Lambda function
        # Increased timeout to handle large date ranges with pagination
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
            timeout=Duration.minutes(15),  # Max Lambda timeout to handle large queries
            memory_size=1024,  # Increased memory for processing large datasets
            environment=lambda_environment,
        )

        # Create CloudWatch Log Groups for Lambda functions
        logs.LogGroup(
            self,
            "PaymentsApiLogGroup",
            log_group_name=f"/aws/lambda/{payments_lambda.function_name}",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        logs.LogGroup(
            self,
            "ShipmentsApiLogGroup",
            log_group_name=f"/aws/lambda/{shipments_lambda.function_name}",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # Create Custom Resource Lambda to generate and sync API key
        api_key_generator_role = iam.Role(
            self,
            "ApiKeyGeneratorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Create API Gateway REST API first (needed for usage plan)
        api = apigateway.RestApi(
            self,
            "SourceApi",
            rest_api_name="Ecom Source API",
            description="API for accessing payments and shipments data",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=[
                    "Content-Type",
                    "X-Amz-Date",
                    "Authorization",
                    "X-Api-Key",
                ],
            ),
        )

        # Create usage plan (API key will be added by Custom Resource)
        usage_plan = api.add_usage_plan(
            "SourceApiUsagePlan",
            name="ecom-source-api-usage-plan",
            description="Usage plan for source APIs",
            throttle=apigateway.ThrottleSettings(
                rate_limit=1000,  # 1000 requests per second
                burst_limit=2000,  # 2000 burst capacity
            ),
            quota=apigateway.QuotaSettings(
                limit=100000,  # 100k requests per day
                period=apigateway.Period.DAY,
            ),
        )
        usage_plan.add_api_stage(stage=api.deployment_stage)

        # Grant permissions to read and write secret, and manage API Gateway
        api_key_secret.grant_read(api_key_generator_role)
        api_key_secret.grant_write(api_key_generator_role)
        api_key_generator_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "apigateway:POST",
                    "apigateway:GET",
                    "apigateway:PUT",
                    "apigateway:PATCH",
                ],
                resources=["*"],
            )
        )

        # Lambda function to generate API key and sync it
        # Code is in external file for better maintainability
        api_key_generator_dir = Path(__file__).parent

        api_key_generator = lambda_.Function(
            self,
            "ApiKeyGenerator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="api_key_generator.handler",
            code=lambda_.Code.from_asset(
                str(api_key_generator_dir),
                bundling=BundlingOptions(
                    image=bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "cp /asset-input/api_key_generator.py /asset-output/ && "
                        "cp /asset-input/cfnresponse.py /asset-output/",
                    ],
                ),
            ),
            role=api_key_generator_role,
            timeout=Duration.seconds(30),
        )

        # Create Custom Resource
        api_key_resource = CustomResource(
            self,
            "ApiKeyResource",
            service_token=api_key_generator.function_arn,
            properties={
                "SecretArn": api_key_secret.secret_arn,
                "ApiKeyName": "ecom-source-api-key",
                "UsagePlanId": usage_plan.usage_plan_id,
            },
        )

        # Import the API key created by Custom Resource for outputs
        api_key = apigateway.ApiKey.from_api_key_id(
            self,
            "SourceApiKey",
            api_key_id=api_key_resource.get_att_string("ApiKeyId"),
        )

        # Create Lambda integrations (proxy integration for API Gateway)
        # Note: LambdaIntegration automatically grants API Gateway permission to invoke Lambda
        payments_integration = apigateway.LambdaIntegration(
            payments_lambda,
            proxy=True,  # Use proxy integration to pass through request/response
        )

        shipments_integration = apigateway.LambdaIntegration(
            shipments_lambda,
            proxy=True,  # Use proxy integration to pass through request/response
        )

        # Create API resources and methods
        payments_resource = api.root.add_resource("payments")
        payments_resource.add_method(
            "GET",
            payments_integration,
            api_key_required=True,
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
                apigateway.MethodResponse(
                    status_code="400",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
                apigateway.MethodResponse(
                    status_code="500",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
            ],
        )

        shipments_resource = api.root.add_resource("shipments")
        shipments_resource.add_method(
            "GET",
            shipments_integration,
            api_key_required=True,
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
                apigateway.MethodResponse(
                    status_code="400",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
                apigateway.MethodResponse(
                    status_code="500",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                    },
                ),
            ],
        )

        # Note: CORS is already configured via default_cors_preflight_options on the REST API
        # which automatically creates OPTIONS methods for all resources

        # Store references for other stacks
        self.api_key_secret = api_key_secret
        self.payments_endpoint = f"{api.url}payments"
        self.shipments_endpoint = f"{api.url}shipments"

        # Outputs
        CfnOutput(
            self,
            "PaymentsApiEndpoint",
            value=self.payments_endpoint,
            description="Payments API endpoint URL",
        )

        CfnOutput(
            self,
            "ShipmentsApiEndpoint",
            value=self.shipments_endpoint,
            description="Shipments API endpoint URL",
        )

        CfnOutput(
            self,
            "ApiKeySecretArn",
            value=api_key_secret.secret_arn,
            description="ARN of the API key secret",
        )

        CfnOutput(
            self,
            "ApiKeyId",
            value=api_key.key_id,
            description="API Key ID for authentication",
        )

        CfnOutput(
            self,
            "RestApiId",
            value=api.rest_api_id,
            description="API Gateway REST API ID",
        )
