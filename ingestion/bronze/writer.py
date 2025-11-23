"""
Bronze tier data writer.

Writes raw data to S3 bronze tier in Parquet format with proper partitioning
and metadata for change detection.
"""

import pandas as pd
import awswrangler as wr
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import uuid
import boto3
from botocore.exceptions import ClientError

from config import Config
from utils.logging_utils import log_progress, log_error


def delete_entity_directory(entity: str) -> None:
    """
    Delete all objects in the entity's bronze directory for a clean slate.

    Args:
        entity: Entity name (e.g., 'customers', 'orders')
    """
    try:
        s3_client = boto3.client("s3")
        entity_prefix = f"{Config.BRONZE_PREFIX}/{entity}/"

        # List all objects in the entity directory
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=Config.S3_BUCKET_NAME, Prefix=entity_prefix)

        objects_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects_to_delete.append({"Key": obj["Key"]})

        # Delete objects in batches of 1000 (S3 limit)
        if objects_to_delete:
            log_progress(f"Bronze Writer - {entity}", f"Deleting {len(objects_to_delete)} existing objects for clean FULL load")

            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                s3_client.delete_objects(
                    Bucket=Config.S3_BUCKET_NAME,
                    Delete={"Objects": batch}
                )

            log_progress(f"Bronze Writer - {entity}", "Entity directory cleared successfully")
        else:
            log_progress(f"Bronze Writer - {entity}", "No existing objects to delete")

    except ClientError as e:
        log_error(f"Bronze Writer - {entity}", f"Failed to delete entity directory: {e}")
        raise
    except Exception as e:
        log_error(f"Bronze Writer - {entity}", f"Unexpected error deleting entity directory: {e}")
        raise


def add_bronze_metadata(
    records: List[Dict[str, Any]], ingestion_timestamp: datetime, source_system: str
) -> List[Dict[str, Any]]:
    """
    Add bronze tier metadata to records.

    Args:
        records: Raw data records
        ingestion_timestamp: When the data was ingested
        source_system: Source system identifier ('rds' or 'api')

    Returns:
        Records with added metadata fields
    """
    enhanced_records = []

    for record in records:
        # Create a copy to avoid modifying original
        enhanced_record = record.copy()

        # Add metadata fields
        enhanced_record["_ingestion_ts"] = ingestion_timestamp.isoformat()
        enhanced_record["_source_system"] = source_system

        # Generate record hash for change detection
        # Sort keys for consistent hashing
        record_str = str(sorted(record.items()))
        enhanced_record["_record_hash"] = hashlib.md5(record_str.encode()).hexdigest()

        # Add unique ingestion ID
        enhanced_record["_ingestion_id"] = str(uuid.uuid4())

        enhanced_records.append(enhanced_record)

    return enhanced_records


def write_to_bronze(
    entity: str,
    records: List[Dict[str, Any]],
    ingestion_timestamp: datetime,
    load_type: str,
    source_system: str = "unknown",
) -> Dict[str, Any]:
    """
    Write data to the bronze tier in S3 with Parquet format and partitioning.

    Args:
        entity: Entity name (e.g., 'customers', 'orders')
        records: List of record dictionaries
        ingestion_timestamp: Timestamp when ingestion started
        load_type: 'FULL' or 'INCREMENTAL'
        source_system: Source system ('rds' or 'api')

    Returns:
        Dict with write operation results and metadata
    """
    if not records:
        log_progress(f"Bronze Writer - {entity}", "No records to write")
        return {"files_written": 0, "records_written": 0}

    try:
        # Add bronze metadata
        enhanced_records = add_bronze_metadata(
            records, ingestion_timestamp, source_system
        )

        # Convert to DataFrame
        df = pd.DataFrame(enhanced_records)

        # Ensure datetime columns are properly typed
        datetime_columns = []
        for col in df.columns:
            if col.endswith("_at") or col.endswith("_date") or col == "_ingestion_ts":
                try:
                    df[col] = pd.to_datetime(df[col])
                    datetime_columns.append(col)
                except (ValueError, TypeError):
                    pass  # Keep as string if conversion fails

        # Generate partition values
        ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")

        # Determine event date for partitioning (use first available timestamp column)
        event_date = None
        for col in [
            "updated_at",
            "created_at",
            "order_date",
            "signup_date",
            "payment_date",
            "shipment_date",
        ]:
            if col in df.columns and not df[col].empty:
                try:
                    # Get the most recent date for the batch
                    max_date = df[col].max()
                    if pd.notna(max_date):
                        if isinstance(max_date, pd.Timestamp):
                            event_date = max_date.strftime("%Y-%m-%d")
                        else:
                            event_date = str(max_date)[:10]  # Extract date part
                        break
                except Exception:
                    continue

        # Build S3 path
        s3_path = f"s3://{Config.S3_BUCKET_NAME}/{Config.get_bronze_path(entity, ingestion_date, load_type.lower(), event_date)}"

        # For FULL loads, delete the entire entity directory first for a clean slate
        if load_type == "FULL":
            delete_entity_directory(entity)

        log_progress(
            f"Bronze Writer - {entity}", f"Writing {len(records)} records to {s3_path}"
        )

        # Write to S3 with partitioning
        result = wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            partition_cols=[],  # Partitioning handled in path structure
            mode="overwrite_partitions" if load_type == "INCREMENTAL" else "overwrite",
            compression="snappy",
            index=False,
            boto3_session=None,  # Use default session
        )

        log_progress(
            f"Bronze Writer - {entity}", f"Successfully wrote data to bronze tier"
        )

        return {
            "files_written": len(result.get("paths", [])),
            "records_written": len(records),
            "s3_path": s3_path,
            "partition_info": {
                "ingestion_date": ingestion_date,
                "load_type": load_type.lower(),
                "event_date": event_date,
            },
        }

    except Exception as e:
        log_error(f"Bronze Writer - {entity}", str(e))
        raise
