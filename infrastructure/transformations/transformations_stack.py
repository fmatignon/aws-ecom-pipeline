"""
Transformations stack for Glue catalog infrastructure.

Provides the Glue database and crawler that will catalog bronze data
so dbt-athena can read it.
"""

from aws_cdk import (
    Aws,
    CfnOutput,
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class EcomTransformationsStack(Stack):
    """
    Stack that provisions Glue catalog resources for transformations.

    This stack creates a Glue database that points to the bronze prefix and a
    crawler that keeps the catalog up to date, following the bronze-focused
    best practices described in the AWS Glue documentation.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        s3_bucket: s3.IBucket,
        bronze_prefix: str = "data/bronze",
        schedule_cron: str = "cron(0 3 * * ? *)",
        **kwargs,
    ) -> None:
        """
        Initialize the transformations stack.

        Args:
            scope (Construct): CDK construct scope.
            construct_id (str): Stack identifier.
            s3_bucket (s3.IBucket): Bucket that stores bronze data.
            bronze_prefix (str): S3 prefix used for bronze assets.
            schedule_cron (str): Glue crawler cron expression.
            **kwargs: Additional Stack keyword arguments.
        """
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("project", "ecom-pipeline")

        normalized_prefix = bronze_prefix.strip("/")
        if normalized_prefix:
            location_uri = f"s3://{s3_bucket.bucket_name}/{normalized_prefix}"
            s3_objects_arn = f"{s3_bucket.bucket_arn}/{normalized_prefix}/*"
        else:
            location_uri = f"s3://{s3_bucket.bucket_name}"
            s3_objects_arn = f"{s3_bucket.bucket_arn}/*"

        database_input = glue.CfnDatabase.DatabaseInputProperty(
            name="ecom_bronze",
            description="Glue database for the bronze zone so dbt-athena can read raw data.",
            location_uri=location_uri,
        )

        database = glue.CfnDatabase(
            self,
            "BronzeGlueDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=database_input,
        )

        self.database = database
        self.database_name = "ecom_bronze"
        self.database_location = location_uri

        glue_catalog_arn = (
            f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog"
        )
        glue_database_arn = (
            f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{self.database_name}"
        )
        glue_table_pattern = (
            f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/"
            f"{self.database_name}/*"
        )

        crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[s3_bucket.bucket_arn],
            )
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[s3_objects_arn],
            )
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTables",
                    "glue:GetTable",
                    "glue:GetPartition",
                    "glue:GetPartitionIndexes",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                    "glue:BatchUpdatePartition",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                ],
                resources=[glue_catalog_arn, glue_database_arn, glue_table_pattern],
            )
        )

        crawler_name = "ecom-bronze-crawler"

        crawler = glue.CfnCrawler(
            self,
            "BronzeCrawler",
            role=crawler_role.role_arn,
            database_name=self.database_name,
            description="Crawler that catalogs bronze files for dbt-athena.",
            name=crawler_name,
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression=schedule_cron
            ),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=location_uri,
                        exclusions=["**/_SUCCESS", "**/*.manifest", "**/tmp/**"],
                    )
                ]
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="LOG",
            ),
        )

        self.crawler = crawler
        self.crawler_name = crawler_name
        self.crawler_arn = self.format_arn(
            service="glue", resource="crawler", resource_name=crawler_name
        )

        CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.database_name,
            description="Glue database for bronze data used by dbt-athena",
        )
        CfnOutput(
            self,
            "GlueDatabaseLocation",
            value=self.database_location,
            description="S3 path for the glue database",
        )
        CfnOutput(
            self,
            "BronzeCrawlerName",
            value=self.crawler_name,
            description="Name of the Glue crawler that catalogs bronze data",
        )
        CfnOutput(
            self,
            "BronzeCrawlerArn",
            value=self.crawler_arn,
            description="ARN of the Glue crawler for bronze data",
        )

