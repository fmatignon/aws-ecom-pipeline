"""
Operations Stack for ECS Fargate task, Step Functions, and EventBridge
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
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
)
from constructs import Construct
from pathlib import Path


class OperationsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket,
        rds_database,
        rds_secret,
        vpc,
        start_date: str = None,  # Optional START_DATE for initial run (YYYY-MM-DD)
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources
        Tags.of(self).add("project", "ecom-pipeline")

        # Get environment variables
        bucket_name = s3_bucket.bucket_name if hasattr(s3_bucket, 'bucket_name') else s3_bucket
        db_endpoint = rds_database.instance_endpoint.hostname if hasattr(rds_database, 'instance_endpoint') else None
        secret_arn = rds_secret.secret_arn if hasattr(rds_secret, 'secret_arn') else None

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
        )

        # Create security group for ECS tasks
        task_security_group = ec2.SecurityGroup(
            self,
            "TaskSecurityGroup",
            vpc=vpc,
            description="Security group for operations ECS tasks",
            allow_all_outbound=True,
        )

        # Allow RDS access
        if hasattr(rds_database, 'connections'):
            rds_database.connections.allow_from(
                task_security_group,
                ec2.Port.tcp(5432),
                "Allow ECS tasks to connect to RDS"
            )

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
                        "secretsmanager:DescribeSecret"
                    ],
                    resources=[secret_arn]
                )
            )

        # Create task role
        task_role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # Grant S3 access
        if hasattr(s3_bucket, 'grant_read_write'):
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
                        "s3:ListBucket"
                    ],
                    resources=[
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                )
            )

        # Grant RDS access (via VPC, but add policy for completeness)
        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "rds-db:connect"
                ],
                resources=[f"arn:aws:rds-db:{self.region}:{self.account}:dbuser:*/ecomadmin"]
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
            memory_limit_mib=4096,
            cpu=2048,
            execution_role=task_execution_role,
            task_role=task_role,
        )

        # Add container
        container = task_definition.add_container(
            "OperationsContainer",
            image=ecs.ContainerImage.from_ecr_repository(ecr_repo, "latest"),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="operations",
                log_group=log_group
            ),
            environment={
                "S3_BUCKET_NAME": bucket_name,
                "RDS_ENDPOINT": db_endpoint or "",
                "RDS_DATABASE_NAME": "ecommerce",
                "RDS_SECRET_ARN": secret_arn or "",
                "AWS_DEFAULT_REGION": self.region,
                **({"START_DATE": start_date} if start_date else {}),
            },
        )

        # Create Step Functions state machine
        # Define the workflow
        run_task = tasks.EcsRunTask(
            self,
            "RunOperationsTask",
            cluster=cluster,
            task_definition=task_definition,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            assign_public_ip=True,
            security_groups=[task_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.taskResult",
        )

        # Add retry configuration
        run_task.add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(60),
            max_attempts=2,
            backoff_rate=2.0,
        )

        # Create state machine
        state_machine = sfn.StateMachine(
            self,
            "OperationsStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(run_task),
            timeout=Duration.minutes(30),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "StateMachineLogGroup",
                    log_group_name="/aws/stepfunctions/ecom-operations",
                    retention=logs.RetentionDays.ONE_MONTH,
                    removal_policy=RemovalPolicy.DESTROY,
                ),
                level=sfn.LogLevel.ALL,
            ),
        )

        # Create EventBridge Rule (daily at 2 AM UTC)
        rule = events.Rule(
            self,
            "OperationsScheduleRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="2",
                day="*",
                month="*",
                year="*"
            ),
            description="Daily trigger for operations simulation",
        )

        # Add Step Functions as target
        rule.add_target(
            targets.SfnStateMachine(state_machine)
        )

        # Grant EventBridge permission to invoke Step Functions
        state_machine.grant_start_execution(
            iam.ServicePrincipal("events.amazonaws.com")
        )

        # Outputs
        CfnOutput(
            self,
            "EcrRepositoryUri",
            value=ecr_repo.repository_uri,
            description="ECR repository URI for operations Docker image",
        )

        CfnOutput(
            self,
            "StateMachineArn",
            value=state_machine.state_machine_arn,
            description="Step Functions state machine ARN",
        )

        CfnOutput(
            self,
            "StateMachineName",
            value=state_machine.state_machine_name,
            description="Step Functions state machine name",
        )

        CfnOutput(
            self,
            "EventBridgeRuleArn",
            value=rule.rule_arn,
            description="EventBridge rule ARN",
        )

        # Store references
        self.ecr_repo = ecr_repo
        self.cluster = cluster
        self.task_definition = task_definition
        self.state_machine = state_machine
        self.rule = rule

