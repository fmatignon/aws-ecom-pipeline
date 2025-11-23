"""
VPC Stack for shared networking resources
Creates VPC that can be used by RDS, Operations, and other stacks
"""
from aws_cdk import (
    Stack,
    Tags,
    aws_ec2 as ec2,
)
from constructs import Construct


class VPCStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("project", "ecom-pipeline")

        # Create minimal VPC (no NAT Gateway - saves $32/month)
        # RDS and ECS require subnets in at least 2 AZs
        self.vpc = ec2.Vpc(
            self,
            "EcomVpc",
            max_azs=2,
            nat_gateways=0,  # No NAT Gateway needed - public subnets
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC,
                    name="Public",
                    cidr_mask=24,
                ),
            ],
        )
