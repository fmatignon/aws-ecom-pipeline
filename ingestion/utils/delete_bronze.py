"""
Utility script to delete all bronze tier data and checkpoints.

This script provides a manual way to completely reset the bronze tier,
which will trigger a fresh FULL load on the next ingestion run.
"""

import sys
from pathlib import Path

# Add the ingestion directory to Python path so imports work from any location
# This MUST be done before importing config or utils modules
ingestion_dir = Path(__file__).parent.parent
if str(ingestion_dir) not in sys.path:
    sys.path.insert(0, str(ingestion_dir))

import boto3
from botocore.exceptions import ClientError
from config import Config
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    log_error,
)


def delete_bronze_data() -> int:
    """
    Delete all objects in the bronze tier directory.

    Returns:
        int: Number of objects deleted
    """
    log_section_start("Bronze Data Deletion")

    try:
        s3_client = boto3.client("s3")
        bronze_prefix = f"{Config.BRONZE_PREFIX}/"

        # List all objects in the bronze directory
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=Config.S3_BUCKET_NAME, Prefix=bronze_prefix)

        objects_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects_to_delete.append({"Key": obj["Key"]})

        # Delete objects in batches of 1000 (S3 limit)
        total_deleted = 0
        if objects_to_delete:
            log_progress(
                "Bronze Data Deletion",
                f"Found {len(objects_to_delete)} objects to delete",
            )

            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                s3_client.delete_objects(
                    Bucket=Config.S3_BUCKET_NAME, Delete={"Objects": batch}
                )
                total_deleted += len(batch)
                log_progress(
                    "Bronze Data Deletion",
                    f"Deleted {total_deleted}/{len(objects_to_delete)} objects",
                )

            log_section_complete(
                "Bronze Data Deletion", f"Deleted {total_deleted} objects"
            )
        else:
            log_progress("Bronze Data Deletion", "No bronze data found to delete")
            log_section_complete("Bronze Data Deletion", "No objects to delete")

        return total_deleted

    except ClientError as e:
        log_error("Bronze Data Deletion", f"Failed to delete bronze data: {e}")
        raise
    except Exception as e:
        log_error("Bronze Data Deletion", f"Unexpected error deleting bronze data: {e}")
        raise


def delete_checkpoints() -> bool:
    """
    Delete the checkpoints.json file.

    Returns:
        bool: True if file was deleted, False if it didn't exist
    """
    log_section_start("Checkpoints Deletion")

    try:
        s3_client = boto3.client("s3")
        checkpoint_key = f"{Config.LOGS_PREFIX}/checkpoints.json"

        # Check if checkpoint file exists
        try:
            s3_client.head_object(Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key)
            # File exists, delete it
            s3_client.delete_object(Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key)
            log_section_complete("Checkpoints Deletion", "Checkpoints file deleted")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ["404", "NoSuchKey"]:
                log_progress("Checkpoints Deletion", "Checkpoints file does not exist")
                log_section_complete("Checkpoints Deletion", "No file to delete")
                return False
            else:
                raise

    except ClientError as e:
        log_error("Checkpoints Deletion", f"Failed to delete checkpoints: {e}")
        raise
    except Exception as e:
        log_error("Checkpoints Deletion", f"Unexpected error deleting checkpoints: {e}")
        raise


def main() -> None:
    """
    Main function to delete all bronze data and checkpoints.
    """
    log_section_start("Bronze Tier Reset")

    try:
        # Validate only required configuration for this utility
        log_section_start("Configuration Validation")
        if not Config.S3_BUCKET_NAME:
            raise ValueError("Missing required environment variable: S3_BUCKET_NAME")
        log_section_complete("Configuration Validation")

        # Delete bronze data
        bronze_count = delete_bronze_data()

        # Delete checkpoints
        checkpoints_deleted = delete_checkpoints()

        # Summary
        log_section_complete(
            "Bronze Tier Reset",
            f"Reset complete - Deleted {bronze_count} bronze objects, "
            f"checkpoints {'deleted' if checkpoints_deleted else 'not found'}",
        )

    except Exception as e:
        log_error("Bronze Tier Reset", str(e))
        raise


if __name__ == "__main__":
    main()
