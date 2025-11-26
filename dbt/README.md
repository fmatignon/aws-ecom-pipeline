"""
Documentation for the dbt transformations project including setup, run instructions,
and known data quality considerations.
"""

# dbt Transformations

This directory hosts the dbt project that transforms bronze tables into silver (clean, typed, deduped) and gold (business-ready aggregates) layers using Athena/Glue.

## Project Structure

```
dbt/
├── macros/
│   ├── dedupe_using_record_hash.sql    # Deduplication helper macro
│   └── parse_timestamp_safe.sql        # Safe timestamp parsing macro
├── models/
│   ├── staging/                        # Silver layer (ecom_silver schema)
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_payments.sql
│   │   ├── stg_products.sql
│   │   └── stg_shipments.sql
│   ├── gold/                           # Gold layer (ecom_gold schema)
│   │   ├── order_facts.sql
│   │   ├── daily_metrics.sql
│   │   ├── customer_360.sql
│   │   ├── rfm_scores.sql
│   │   └── product_catalog.sql
│   └── schema.yml                      # Source and model definitions with tests
├── dbt_project.yml
├── profiles.yml
└── README.md
```

## Local Development

1. Create or activate a Python virtual environment and install dependencies:
   ```sh
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Configure your dbt profile by copying `profiles.yml` to `~/.dbt/profiles.yml` and updating:
   - `region_name`: Your AWS region
   - `s3_staging_dir`: S3 path for Athena query staging
   - `s3_data_dir`: S3 path for dbt-athena data storage
   - `database`: Glue database name (default: `ecom_bronze`)
   - `aws_profile_name`: Your AWS CLI profile (or remove if using env credentials)

3. Verify connectivity:
   ```sh
   dbt debug
   ```

## Running Models

### Full Refresh

Use full refresh when you need to rebuild all models from scratch:

```sh
# Rebuild all models
dbt run --full-refresh

# Rebuild specific model
dbt run --full-refresh --select stg_customers

# Rebuild silver layer only
dbt run --full-refresh --select staging.*

# Rebuild gold layer only
dbt run --full-refresh --select gold.*
```

### Incremental Runs

For production incremental updates:

```sh
# Run all incremental models
dbt run

# Run specific model incrementally
dbt run --select order_facts

# Run with dependencies
dbt run --select +order_facts  # Runs all upstream models too
```

### Running Tests

```sh
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_customers

# Run schema tests only
dbt test --select test_type:schema
```

## Macros

### dedupe_using_record_hash

Deduplicates records using `row_number()` partitioned by primary key and ordered by `_ingestion_ts` descending.

```sql
{{ dedupe_using_record_hash('source_table', ['customer_id']) }}
```

### parse_timestamp_safe

Safely parses varchar columns to timestamps with error flag for failed parses.

```sql
{{ parse_timestamp_safe('"payment-date"', 'payment_date') }}
```

## Data Quality Considerations

### Known Parse Issues

- **Payments**: `payment-date`, `created-at`, `updated-at` columns are stored as varchar and parsed to timestamps. Check `*_parse_error` columns for parsing failures.
- **Shipments**: Date columns (`shipment-date`, `estimated-delivery-date`, `actual-delivery-date`, `created-at`, `updated-at`) are varchar and may have parse errors.

### QA Flags

Silver models include QA flags for data quality monitoring:

- **stg_customers**:
  - `is_valid_email`: Email passes regex validation
  - `is_adult`: Customer age >= 18 based on date_of_birth
  - `has_contact`: Customer has email or phone

- **stg_orders**:
  - `is_paid`: Payment status is completed/success
  - `is_delivered`: Order delivered or has delivery date
  - `is_refunded`: Order or payment was refunded
  - `is_cancelled`: Order was cancelled

### Accepted Values

| Field | Valid Values |
|-------|--------------|
| Gender | `M`, `F`, `Other`, `NULL` |
| Customer Segment | `New`, `Regular`, `VIP` |
| Order Status | `processing`, `shipped`, `delivered`, `cancelled`, `pending`, `refunded` |
| Payment Status | `completed`, `failed`, `pending`, `refunded` |
| Shipment Status | `pending`, `in_transit`, `delivered`, `returned`, `cancelled`, `exception` |
| Payment Methods | `credit_card`, `paypal`, `apple_pay`, `google_pay` |
| Card Brands | `visa`, `mastercard`, `amex`, `discover` |
| Carriers | `UPS`, `FedEx`, `USPS`, `DHL` |

## Orchestration Integration

To integrate with Step Functions workflow:

1. Use the same ECS cluster (or a dedicated task) to run dbt
2. Trigger after the `RunCrawlerTask` state succeeds
3. Command: `dbt run --profiles-dir ~/.dbt`
4. Capture success/failure in Step Functions for retry handling

## Troubleshooting

### Common Issues

1. **Schema not found**: Ensure Glue crawler has run and cataloged bronze tables
2. **Permission denied**: Verify IAM role has Athena, S3, and Glue permissions
3. **Incremental failures**: Try `--full-refresh` to rebuild from scratch
4. **Parse errors**: Check `*_parse_error` columns in silver models for data issues
