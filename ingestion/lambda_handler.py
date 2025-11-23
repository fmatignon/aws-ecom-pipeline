"""
AWS Lambda handler for daily ingestion of e-commerce data into bronze tier.

This function orchestrates the extraction of data from RDS and APIs,
maintains incremental state, and writes raw data to the bronze tier in S3.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from typing import Dict, Any, Tuple

# Import configuration and utilities
from config import Config
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    log_error,
)

# Import submodules
from extract.rds_reader import extract_rds_data
from extract.api_client import extract_api_data
from utils.state import get_last_sync, update_sync, ensure_checkpoint_file_exists
from bronze.writer import write_to_bronze


def process_entity(
    entity: str, load_type: str, run_timestamp: datetime
) -> Tuple[str, Dict[str, Any]]:
    """
    Process a single entity: extract data, write to bronze, and update state.

    Args:
        entity: Entity name to process
        load_type: Load type ('FULL' or 'INCREMENTAL')
        run_timestamp: Timestamp of the current run

    Returns:
        Tuple of (entity_name, result_dict)
    """
    try:
        log_section_start(f"Processing {entity}")

        # Get last sync timestamp for incremental loads
        if load_type == "INCREMENTAL":
            last_sync = get_last_sync(entity)
        else:
            last_sync = Config.INITIAL_LOAD_DATE

        # Extract data based on entity type
        if entity in ["customers", "products", "orders"]:
            data = extract_rds_data(entity, last_sync)
            source_system = "rds"
        else:
            data = extract_api_data(entity, last_sync)
            source_system = "api"

        # Write to bronze tier
        if data:
            write_result = write_to_bronze(
                entity, data, run_timestamp, load_type, source_system
            )
            log_progress(
                f"Processing {entity}",
                f"Extracted and wrote {len(data)} records",
            )
            result = {
                "records": len(data),
                "status": "success",
                "write_info": write_result,
            }

            # Update sync state after successful data extraction
            # This enables future incremental loads regardless of current load type
            update_sync(entity, run_timestamp.isoformat(), len(data))

        else:
            log_progress(f"Processing {entity}", "No new records to process")
            result = {"records": 0, "status": "no_data"}

        log_section_complete(f"Processing {entity}")
        return (entity, result)

    except Exception as e:
        log_error(f"Processing {entity}", str(e))
        return (entity, {"status": "error", "error": str(e)})


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for data ingestion.

    Args:
        event: Lambda event (unused for scheduled runs)
        context: Lambda context object

    Returns:
        Dict containing execution results and metrics
    """
    run_timestamp = datetime.now(UTC)

    try:
        # Validate configuration
        log_section_start("Configuration Validation")
        Config.validate()
        log_section_complete("Configuration Validation")

        # Ensure checkpoint file exists
        log_section_start("Checkpoint File Setup")
        ensure_checkpoint_file_exists()
        log_section_complete("Checkpoint File Setup")

        # Determine load type and entities to process
        entities = ["customers", "products", "orders", "payments", "shipments"]

        # Check if this is a first run (no entities have checkpoints)
        is_first_run = True
        try:
            import boto3
            client = boto3.client("s3")
            checkpoint_key = f"{Config.LOGS_PREFIX}/checkpoints.json"
            response = client.get_object(Bucket=Config.S3_BUCKET_NAME, Key=checkpoint_key)
            checkpoints = json.loads(response["Body"].read().decode("utf-8"))

            # Check if any entity has a valid checkpoint
            for entity in entities:
                if entity in checkpoints and checkpoints[entity].get("last_sync_timestamp"):
                    is_first_run = False
                    break
        except Exception:
            # If we can't read checkpoints, assume first run for safety
            log_progress("Ingestion Pipeline", "Unable to read checkpoints, assuming first run")

        # Determine load type - override to FULL for first run
        if is_first_run and Config.LOAD_TYPE == "INCREMENTAL":
            load_type = "FULL"
            log_progress("Ingestion Pipeline", "Detected first run - switching to FULL load type")
        else:
            load_type = Config.LOAD_TYPE

        log_section_start(f"Ingestion Pipeline - {load_type} Load")
        log_progress(
            "Ingestion Pipeline", f"Processing entities: {', '.join(entities)}"
        )

        # Separate entities by type for different processing strategies
        rds_entities = ["customers", "products", "orders"]
        api_entities = ["payments", "shipments"]

        results = {}

        # Process RDS entities sequentially (they share database connection)
        log_progress(
            "Ingestion Pipeline",
            f"Processing RDS entities sequentially: {', '.join(rds_entities)}",
        )
        for entity in rds_entities:
            entity_name, result = process_entity(entity, load_type, run_timestamp)
            results[entity_name] = result

        # Process API entities concurrently (they are separate APIs)
        log_progress(
            "Ingestion Pipeline",
            f"Processing API entities concurrently: {', '.join(api_entities)}",
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit all API entity processing tasks
            future_to_entity = {
                executor.submit(process_entity, entity, load_type, run_timestamp): entity
                for entity in api_entities
            }

            # Collect results as they complete
            for future in as_completed(future_to_entity):
                entity_name, result = future.result()
                results[entity_name] = result

        # Calculate summary metrics
        total_records = sum(
            result.get("records", 0)
            for result in results.values()
            if isinstance(result, dict)
        )
        success_count = sum(
            1 for result in results.values() if result.get("status") == "success"
        )
        error_count = sum(
            1 for result in results.values() if result.get("status") == "error"
        )

        log_section_complete(
            f"Ingestion Pipeline - {load_type} Load",
            f"Processed {total_records} total records, {success_count} entities successful, {error_count} entities failed",
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "run_timestamp": run_timestamp.isoformat(),
                    "load_type": load_type,
                    "results": results,
                    "summary": {
                        "total_records": total_records,
                        "successful_entities": success_count,
                        "failed_entities": error_count,
                    },
                }
            ),
        }
    except Exception as e:
        log_error("Ingestion Pipeline", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e), "run_timestamp": run_timestamp.isoformat()}
            ),
        }
