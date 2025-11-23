"""
E-commerce Pipeline Orchestration Stack.

Creates a unified Step Functions state machine that coordinates
the operations simulation (ECS task) and data ingestion (Lambda)
pipelines in sequential order.
"""

from aws_cdk import (
    Stack,
    Tags,
    CfnOutput,
    RemovalPolicy,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
)
import json
from pathlib import Path
from constructs import Construct


class EcomOrchestrationStack(Stack):
    """
    Orchestrates the e-commerce pipeline by coordinating operations and ingestion.

    This stack creates a Step Functions state machine that runs operations first
    to generate data, then ingestion to process that data. Both tasks include
    retry logic and error handling.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        operations_stack,
        ingestion_stack,
        **kwargs,
    ) -> None:
        """
        Initialize the orchestration stack.

        Args:
            scope (Construct): CDK construct scope.
            construct_id (str): Unique identifier for the stack.
            operations_stack: Deployed operations stack that exposes ECS resources.
            ingestion_stack: Deployed ingestion stack exposing the Lambda function.
            **kwargs: Additional CDK Stack kwargs such as env.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources
        Tags.of(self).add("project", "ecom-pipeline")

        # Extract resources from dependency stacks
        ecs_cluster = operations_stack.cluster
        ecs_task_definition = operations_stack.task_definition
        task_security_group = operations_stack.task_security_group
        vpc = operations_stack.vpc
        ingestion_lambda = ingestion_stack.ingestion_lambda

        # Create CloudWatch Log Group for Step Functions
        log_group = logs.LogGroup(
            self,
            "OrchestrationLogGroup",
            log_group_name="/aws/stepfunctions/ecom-orchestration",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )



        # Create IAM role for Step Functions
        state_machine_role = iam.Role(
            self,
            "OrchestrationStateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        # Grant permissions for Step Functions to run ECS tasks
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ecs:RunTask",
                    "ecs:DescribeTasks",
                    "ecs:StopTask",
                ],
                resources=[
                    ecs_task_definition.task_definition_arn,
                    f"{ecs_task_definition.task_definition_arn}:*",
                ],
            )
        )

        # Grant permissions for Step Functions to pass IAM roles to ECS
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["iam:PassRole"],
                resources=[
                    ecs_task_definition.execution_role.role_arn,
                    ecs_task_definition.task_role.role_arn,
                ],
            )
        )

        # Grant permissions for Step Functions to invoke Lambda
        ingestion_lambda.grant_invoke(state_machine_role)

        # Allow Step Functions logging to CloudWatch Logs
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                    "logs:DescribeResourcePolicies",
                    "logs:GetLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutLogEvents",
                    "logs:PutResourcePolicy",
                    "logs:UpdateLogDelivery",
                ],
                resources=["*"],
            )
        )

        # Grant permissions for Step Functions to manage the EventBridge-managed rule
        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "events:PutRule",
                    "events:DeleteRule",
                    "events:DescribeRule",
                    "events:PutTargets",
                    "events:RemoveTargets",
                ],
                resources=["*"],
            )
        )

        # Get subnet IDs and security group IDs
        if hasattr(vpc, 'select_subnets'):
            subnet_ids = vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC).subnet_ids
        else:
            # Fallback - assume VPC has public subnets
            subnet_ids = ["subnet-placeholder"]

        security_group_ids = [task_security_group.security_group_id]

        # Load ASL definition from file as raw text
        asl_file_path = Path(__file__).parent.parent.parent / "step-functions" / "orchestration_workflow.asl.json"
        with open(asl_file_path, "r", encoding="utf-8") as asl_file:
            asl_payload = json.load(asl_file)

        # Strip helper metadata and prepare deterministic placeholder replacements
        asl_payload.pop("Note", None)
        definition_string = json.dumps(asl_payload, separators=(",", ":"))
        definition_string = definition_string.replace(
            '"__SUBNET_IDS_PLACEHOLDER__"', json.dumps(subnet_ids)
        ).replace(
            '"__SECURITY_GROUP_IDS_PLACEHOLDER__"', json.dumps(security_group_ids)
        )

        # Create the Step Functions state machine using the ASL definition
        state_machine = sfn.StateMachine(
            self,
            "EcomOrchestrationStateMachine",
            definition_body=sfn.DefinitionBody.from_string(definition_string),
            definition_substitutions={
                "EcsClusterArn": ecs_cluster.cluster_arn,
                "TaskDefinitionArn": ecs_task_definition.task_definition_arn,
                "IngestionFunctionArn": ingestion_lambda.function_arn,
            },
            role=state_machine_role,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
        )

        # Create EventBridge Rule (daily at 2 AM UTC)
        rule = events.Rule(
            self,
            "PipelineScheduleRule",
            schedule=events.Schedule.cron(
                minute="0", hour="2", day="*", month="*", year="*"
            ),
            description="Daily trigger for e-commerce pipeline orchestration",
        )

        # Add Step Functions as target
        rule.add_target(targets.SfnStateMachine(state_machine))

        # Grant EventBridge permission to invoke Step Functions
        state_machine.grant_start_execution(
            iam.ServicePrincipal("events.amazonaws.com")
        )

        # Outputs
        CfnOutput(
            self,
            "OrchestrationStateMachineArn",
            value=state_machine.state_machine_arn,
            description="Step Functions orchestration state machine ARN",
        )

        CfnOutput(
            self,
            "OrchestrationStateMachineName",
            value=state_machine.state_machine_name,
            description="Step Functions orchestration state machine name",
        )

        CfnOutput(
            self,
            "PipelineScheduleRuleArn",
            value=rule.rule_arn,
            description="EventBridge rule ARN for pipeline scheduling",
        )

        CfnOutput(
            self,
            "OrchestrationLogsGroupName",
            value=log_group.log_group_name,
            description="CloudWatch Log Group for orchestration logs",
        )

        # Store references
        self.state_machine = state_machine
        self.rule = rule
