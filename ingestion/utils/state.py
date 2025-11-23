"""
State store module for managing incremental ingestion checkpoints.

Uses S3 to store and retrieve last sync timestamps for each entity.
"""

import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

from config import Config
from utils.logging_utils import log_progress, log_error


def get_last_sync(entity: str) -> str:
    """
    Retrieve the last sync timestamp for an entity from S3 checkpoint file.

    Args:
        entity: The entity name (e.g., 'customers', 'orders')

    Returns:
        ISO format timestamp string, or INITIAL_LOAD_DATE if no checkpoint exists
    """
    client = boto3.client("s3")
    checkpoint_key = f"{Config.LOGS_PREFIX}/checkpoints.json"

    try:
        response = client.get_object(Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key)

        checkpoints = json.loads(response["Body"].read().decode("utf-8"))

        if entity in checkpoints and "last_sync_timestamp" in checkpoints[entity]:
            timestamp = checkpoints[entity]["last_sync_timestamp"]
            log_progress(
                f"State Store - {entity}", f"Retrieved checkpoint: {timestamp}"
            )
            return timestamp
        else:
            log_progress(
                f"State Store - {entity}",
                "No checkpoint found, using initial load date",
            )
            return Config.INITIAL_LOAD_DATE

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            # Checkpoint file doesn't exist yet - this is normal for first run
            log_progress(
                f"State Store - {entity}",
                "No checkpoint file found, using initial load date",
            )
            return Config.INITIAL_LOAD_DATE
        else:
            log_error(f"State Store - {entity}", f"Failed to retrieve checkpoint: {e}")
            # Return initial date on error to avoid breaking the pipeline
            return Config.INITIAL_LOAD_DATE
    except Exception as e:
        log_error(f"State Store - {entity}", str(e))
        return Config.INITIAL_LOAD_DATE


def update_sync(entity: str, timestamp: str, record_count: int = 0) -> None:
    """
    Update the last sync timestamp for an entity in S3 checkpoint file.

    Args:
        entity: The entity name (e.g., 'customers', 'orders')
        timestamp: ISO format timestamp string
        record_count: Number of records processed (for metrics)
    """
    client = boto3.client("s3")
    checkpoint_key = f"{Config.LOGS_PREFIX}/checkpoints.json"

    try:
        # Read existing checkpoints
        checkpoints = {}
        try:
            response = client.get_object(
                Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key
            )
            checkpoints = json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

        # Update or create checkpoint for this entity
        checkpoints[entity] = {
            "last_sync_timestamp": timestamp,
            "record_count": record_count,
            "updated_at": datetime.now().isoformat(),
        }

        # Write back to S3
        client.put_object(
            Bucket=Config.S3_BUCKET_NAME,
            Key=checkpoint_key,
            Body=json.dumps(checkpoints, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        log_progress(
            f"State Store - {entity}",
            f"Updated checkpoint to: {timestamp} ({record_count} records)",
        )

    except ClientError as e:
        log_error(f"State Store - {entity}", f"Failed to update checkpoint: {e}")
        raise
    except Exception as e:
        log_error(f"State Store - {entity}", str(e))
        raise


def ensure_checkpoint_file_exists() -> None:
    """
    Ensure the checkpoint file exists in S3.
    Creates it with an empty JSON object if it doesn't exist.
    """
    client = boto3.client("s3")
    checkpoint_key = f"{Config.LOGS_PREFIX}/checkpoints.json"

    try:
        # Check if checkpoint file exists
        client.head_object(Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key)
        log_progress("State Store", f"Checkpoint file {checkpoint_key} already exists")

    except ClientError as e:
        if (
            e.response["Error"]["Code"] == "404"
            or e.response["Error"]["Code"] == "NoSuchKey"
        ):
            # Create the checkpoint file with empty JSON
            log_progress("State Store", f"Creating checkpoint file {checkpoint_key}")

            client.put_object(
                Bucket=Config.S3_BUCKET_NAME,
                Key=checkpoint_key,
                Body=json.dumps({}, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            log_progress(
                "State Store",
                f"Checkpoint file {checkpoint_key} created successfully",
            )
        else:
            log_error("State Store", f"Failed to check/create checkpoint file: {e}")
            raise
