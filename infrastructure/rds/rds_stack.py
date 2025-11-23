"""
RDS PostgreSQL Stack for E-commerce Database (Free Tier Optimized)
Uses RDS t3.micro in public subnet (no NAT Gateway needed)
Free Tier: 750 hours/month for first 12 months
"""
from aws_cdk import (
    Stack,
    Duration,
    Tags,
    aws_ec2 as ec2,
    aws_rds as rds,
    CfnOutput,
    RemovalPolicy,
)
from constructs import Construct


class RDSStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Use provided VPC or create one (for backward compatibility)
        if vpc is None:
            vpc = ec2.Vpc(
                self,
                "EcomVpc",
                max_azs=2,
                nat_gateways=0,  # No NAT Gateway needed - RDS in public subnet
                subnet_configuration=[
                    ec2.SubnetConfiguration(
                        subnet_type=ec2.SubnetType.PUBLIC,
                    name="Public",
                    cidr_mask=24,
                ),
            ],
        )

        # Create security group for RDS
        db_security_group = ec2.SecurityGroup(
            self,
            "RdsSecurityGroup",
            vpc=vpc,
            description="Security group for RDS PostgreSQL instance",
            allow_all_outbound=True,
        )

        # Allow inbound PostgreSQL connections from anywhere (for local access)
        # In production, restrict this to specific IPs
        db_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL connections",
        )

        # Create DB subnet group
        subnet_group = rds.SubnetGroup(
            self,
            "RdsSubnetGroup",
            description="Subnet group for RDS PostgreSQL instance",
            vpc=vpc,
            subnet_group_name="ecom-rds-subnet-group",
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
        )

        # Create database credentials secret
        db_secret = rds.DatabaseSecret(
            self,
            "RdsSecret",
            username="ecomadmin",
            secret_name="ecom-rds-credentials",
        )

        # Create RDS PostgreSQL instance (t3.micro is Free Tier eligible)
        database = rds.DatabaseInstance(
            self,
            "EcomDatabase",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_17_6
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_groups=[db_security_group],
            subnet_group=subnet_group,
            credentials=rds.Credentials.from_secret(db_secret),
            database_name="ecommerce",
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            publicly_accessible=True,  # Public access - no NAT Gateway needed
            multi_az=False,
            backup_retention=Duration.days(1),  # Minimum 1 day for stability
        )

        # Outputs
        CfnOutput(
            self,
            "DatabaseEndpoint",
            value=database.instance_endpoint.hostname,
            description="RDS PostgreSQL endpoint",
        )

        CfnOutput(
            self,
            "DatabaseName",
            value="ecommerce",
            description="Database name",
        )

        CfnOutput(
            self,
            "SecretArn",
            value=db_secret.secret_arn,
            description="ARN of the database credentials secret",
        )

        # Store references for other stacks
        self.vpc = vpc
        self.database = database
        self.db_security_group = db_security_group
        self.db_secret = db_secret

