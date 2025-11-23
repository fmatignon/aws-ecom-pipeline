"""
Operations Stack for ECS Fargate task infrastructure.

Creates the necessary AWS resources to run operations simulation tasks,
including ECR repository, ECS cluster, task definition, and IAM roles.
"""

from aws_cdk import (
    Stack,
    Duration,
    Tags,
    CfnOutput,
    RemovalPolicy,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct
from pathlib import Path
import os


class OperationsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket,
        rds_database,
        rds_secret,
        vpc=None,  # Optional - can get from RDS instance
        start_date: str = None,  # Optional START_DATE for initial run (YYYY-MM-DD)
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources
        Tags.of(self).add("project", "ecom-pipeline")

        # Get environment variables
        bucket_name = (
            s3_bucket.bucket_name if hasattr(s3_bucket, "bucket_name") else s3_bucket
        )
        db_endpoint = (
            rds_database.instance_endpoint.hostname
            if hasattr(rds_database, "instance_endpoint")
            else None
        )
        secret_arn = (
            rds_secret.secret_arn if hasattr(rds_secret, "secret_arn") else None
        )

        # Get VPC from parameter or RDS instance (for backward compatibility)
        if vpc is None and hasattr(rds_database, "vpc"):
            vpc = rds_database.vpc

        # Create ECR Repository
        ecr_repo = ecr.Repository(
            self,
            "OperationsRepository",
            repository_name="ecom-operations",
            image_scan_on_push=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self,
            "OperationsCluster",
            cluster_name="ecom-operations-cluster",
            vpc=vpc,
            container_insights_v2=ecs.ContainerInsights.ENHANCED,  # Enable Container Insights with enhanced observability for detailed task-level metrics
        )

        # Create security group for ECS tasks
        task_security_group = ec2.SecurityGroup(
            self,
            "TaskSecurityGroup",
            vpc=vpc,
            description="Security group for operations ECS tasks",
            allow_all_outbound=True,
        )

        # Note: RDS access is handled via Secrets Manager and IAM
        # Security group ingress rules are configured in RDS stack for development access

        # Create task execution role
        task_execution_role = iam.Role(
            self,
            "TaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        # Grant Secrets Manager access
        if secret_arn:
            task_execution_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    resources=[secret_arn],
                )
            )

        # Create task role
        task_role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # Grant Secrets Manager access to task role
        if secret_arn:
            task_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    resources=[secret_arn],
                )
            )

        # Grant S3 access
        if hasattr(s3_bucket, "grant_read_write"):
            s3_bucket.grant_read_write(task_role)
        else:
            # Fallback: grant permissions via policy
            task_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                    ],
                    resources=[
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*",
                    ],
                )
            )

        # Grant RDS access (via VPC, but add policy for completeness)
        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["rds-db:connect"],
                resources=[
                    f"arn:aws:rds-db:{self.region}:{self.account}:dbuser:*/ecomadmin"
                ],
            )
        )

        # Create CloudWatch Log Group
        log_group = logs.LogGroup(
            self,
            "OperationsLogGroup",
            log_group_name="/ecs/ecom-operations",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Create ECS Task Definition
        task_definition = ecs.FargateTaskDefinition(
            self,
            "OperationsTaskDefinition",
            memory_limit_mib=16384,  # Increased from 8192 MiB (8 GB) to 16384 MiB (16 GB) to handle large data processing
            cpu=2048,
            execution_role=task_execution_role,
            task_role=task_role,
        )

        # Add container
        container = task_definition.add_container(
            "OperationsContainer",
            image=ecs.ContainerImage.from_ecr_repository(ecr_repo, "latest"),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="operations", log_group=log_group
            ),
            environment={
                "S3_BUCKET_NAME": bucket_name,
                "RDS_SECRET_ARN": secret_arn or "",
                "AWS_DEFAULT_REGION": self.region,
            },
        )


        # Outputs
        CfnOutput(
            self,
            "EcrRepositoryUri",
            value=ecr_repo.repository_uri,
            description="ECR repository URI for operations Docker image",
        )


        # Store references for other stacks
        self.ecr_repo = ecr_repo
        self.cluster = cluster
        self.task_definition = task_definition
        self.task_security_group = task_security_group
        self.vpc = vpc
