# Transformations Readiness

This document explains how the new `EcomTransformationsStack` prepares the project for dbt-athena development by cataloging the bronze tier.

## Stack behavior

1. **Glue Database** – `EcomTransformationsStack` creates the `ecom_bronze` Glue database that points at the `s3://<pipeline-bucket>/data/bronze` prefix so Athena can discover transient parquet/csv files. The database name and location are published via stack outputs (`GlueDatabaseName`, `GlueDatabaseLocation`).
2. **Glue Crawler** – A Glue crawler (`ecom-bronze-crawler`) watches the bronze prefix using `CRAWL_NEW_FOLDERS_ONLY`, the recommended incremental strategy from the AWS Glue best-practice guide (https://docs.aws.amazon.com/glue/latest/dg/best-practice-catalog.html). It is scheduled daily at 03:00 UTC and skips temporary files by excluding `**/_SUCCESS`, `**/*.manifest`, and `**/tmp/**`.
3. **Orchestration Integration** – The Step Functions workflow now runs the crawler after ingestion completes using `glue:startCrawler.sync`, ensuring the catalog is fresh before the state machine reports success. The orchestration role has explicit `glue:GetCrawler`/`glue:StartCrawler` permissions so the new state can execute reliably.

## dbt-athena readiness

1. **Profile configuration** – Point dbt’s Athena profile at the `ecom_bronze` database published by the stack output. Example `profiles.yml` snippet:

```yaml
ecom_athena:
  target: dev
  outputs:
    dev:
      type: athena
      schema: ecom_bronze
      database: ecom_bronze
      s3_staging_dir: s3://aws-ecom-pipeline/dbt-athena/staging
      region_name: us-east-1
```

2. **Source tables** – After the crawler finishes, bronze paths such as `data/bronze/customers` map to Glue tables. Point dbt `sources` at the relevant tables (e.g., `bronze_customers`) and build your silver/gold models on top.
3. **Automation** – Continue the pipeline by scheduling dbt via the same Step Functions workflow or a downstream stack once the crawler completes. For now, the crawler state ensures the catalog is synced before dbt models run later.

## Monitoring & tuning

- Watch the Glue crawler log in CloudWatch and use the crawler output (table names, schema changes) to verify the catalog as ingestion changes.
- Use the scheduler output (`BronzeCrawlerName`) if you need to trigger runs manually or extend the workflow.


