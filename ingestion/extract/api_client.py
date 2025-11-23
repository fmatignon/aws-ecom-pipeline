"""
API data extraction module.

Calls the existing Lambda-backed APIs for payments and shipments data,
supporting incremental extraction based on timestamps.
"""

import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

from config import Config
from utils.logging_utils import log_progress, log_error


def process_chunk(
    entity: str,
    config: Dict[str, Any],
    chunk_start: datetime,
    chunk_end: datetime,
    chunk_idx: int,
    total_chunks: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Process a single date chunk: fetch all pages for the chunk date range.

    Args:
        entity: Entity name for logging
        config: API configuration dict
        chunk_start: Start date for this chunk
        chunk_end: End date for this chunk
        chunk_idx: Index of this chunk (for logging)
        total_chunks: Total number of chunks (for logging)

    Returns:
        Tuple of (chunk_index, list_of_records)
    """
    # Create a session per chunk for thread safety and connection reuse within chunk
    session = requests.Session()
    session.headers.update(
        {"Content-Type": "application/json", "x-api-key": config["key"]}
    )

    chunk_records = []
    start_date_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end_date_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    log_progress(
        f"API Extraction - {entity}",
        f"Processing chunk {chunk_idx}/{total_chunks}: "
        f"{chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}",
    )

    # API pagination parameters for this chunk
    limit = 1000
    offset = 0
    has_more = True
    page_count = 0

    try:
        while has_more:
            params = {
                "start_date": start_date_str,
                "end_date": end_date_str,
                "limit": limit,
                "offset": offset,
            }

            # Retry logic for transient errors (500, 502, 503, 504)
            max_retries = 5  # Increased retries to handle persistent API Lambda errors
            retry_delay = 1  # Start with 1 second
            last_exception = None

            for attempt in range(max_retries):
                try:
                    # Make API request using session for connection reuse within chunk
                    response = session.get(config["url"], params=params, timeout=60)

                    # Check for retryable HTTP errors
                    if response.status_code in [500, 502, 503, 504]:
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (
                                2**attempt
                            )  # Exponential backoff
                            log_progress(
                                f"API Extraction - {entity}",
                                f"Chunk {chunk_idx}, page {page_count + 1}: "
                                f"Received {response.status_code}, retrying in {wait_time}s "
                                f"(attempt {attempt + 1}/{max_retries})",
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            # Last attempt failed, raise the error
                            response.raise_for_status()

                    # Success - process the response
                    response.raise_for_status()
                    data = response.json()
                    break  # Exit retry loop on success

                except requests.exceptions.RequestException as e:
                    last_exception = e
                    # Check if it's a retryable error
                    if hasattr(e, "response") and e.response is not None:
                        status_code = e.response.status_code
                        if (
                            status_code in [500, 502, 503, 504]
                            and attempt < max_retries - 1
                        ):
                            wait_time = retry_delay * (2**attempt)
                            log_progress(
                                f"API Extraction - {entity}",
                                f"Chunk {chunk_idx}, page {page_count + 1}: "
                                f"Request exception {type(e).__name__}, retrying in {wait_time}s "
                                f"(attempt {attempt + 1}/{max_retries})",
                            )
                            time.sleep(wait_time)
                            continue
                    # Non-retryable error or last attempt
                    if attempt == max_retries - 1:
                        raise
                    # For other exceptions, wait and retry
                    wait_time = retry_delay * (2**attempt)
                    log_progress(
                        f"API Extraction - {entity}",
                        f"Chunk {chunk_idx}, page {page_count + 1}: "
                        f"Request exception, retrying in {wait_time}s "
                        f"(attempt {attempt + 1}/{max_retries})",
                    )
                    time.sleep(wait_time)
            else:
                # All retries exhausted
                if last_exception:
                    raise last_exception
                raise requests.exceptions.RequestException(
                    f"Failed after {max_retries} attempts"
                )

            # Handle API Gateway Lambda proxy response format
            if isinstance(data, dict) and "body" in data:
                data = json.loads(data["body"])

            # Extract records from response
            if isinstance(data, dict):
                page_records = []
                if config["response_key"] in data:
                    page_records = data[config["response_key"]]
                    if isinstance(page_records, list):
                        chunk_records.extend(page_records)
                    elif page_records:
                        chunk_records.append(page_records)

                # Check pagination metadata
                has_more = data.get("has_more", False)
                next_offset = data.get("next_offset")
                count = data.get("count", len(page_records))

                if not has_more and next_offset is None:
                    has_more = count >= limit

                if has_more and next_offset is not None:
                    offset = next_offset
                elif has_more:
                    offset += limit
                else:
                    break

                page_count += 1

            elif isinstance(data, list):
                chunk_records.extend(data)
                has_more = False
            else:
                raise ValueError(
                    f"Unexpected API response format for {entity}: {type(data)}"
                )
    finally:
        # Always close session
        session.close()

    log_progress(
        f"API Extraction - {entity}",
        f"Chunk {chunk_idx}/{total_chunks} completed: {len(chunk_records)} records",
    )

    return (chunk_idx, chunk_records)


def extract_api_data(entity: str, last_sync_timestamp: str) -> List[Dict[str, Any]]:
    """
    Extract data from API endpoints for a specific entity since the last sync timestamp.

    Splits large date ranges into 7-day chunks and processes them concurrently
    to maximize throughput and avoid API Gateway timeouts.

    Args:
        entity: The entity name ('payments', 'shipments')
        last_sync_timestamp: ISO format timestamp to extract records after

    Returns:
        List of dictionaries containing the extracted records
    """
    # Configure API endpoints and keys
    api_configs = {
        "payments": {
            "url": Config.PAYMENTS_API_URL,
            "key": Config.get_payments_api_key(),
            "response_key": "payments",
            "date_column": "payment-date",
        },
        "shipments": {
            "url": Config.SHIPMENTS_API_URL,
            "key": Config.get_shipments_api_key(),
            "response_key": "shipments",
            "date_column": "shipment-date",
        },
    }

    if entity not in api_configs:
        raise ValueError(f"Unsupported entity: {entity}")

    config = api_configs[entity]

    try:
        # Determine overall date range for API query
        # For incremental loads, use last_sync_timestamp as start_date
        # For full loads, use 2024-01-01 to fetch all data since business start date
        if last_sync_timestamp and last_sync_timestamp != Config.INITIAL_LOAD_DATE:
            try:
                # Parse last sync timestamp
                start_dt = datetime.fromisoformat(
                    last_sync_timestamp.replace("Z", "+00:00")
                )
                # Use last sync as start_date (duplicates are handled during deduplication)
            except ValueError:
                log_progress(
                    f"API Extraction - {entity}",
                    f"Invalid timestamp format: {last_sync_timestamp}, using 2024-01-01",
                )
                # Fallback to business start date for full load
                start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        else:
            # For initial/full loads, fetch all historical data from business start date
            start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
            log_progress(
                f"API Extraction - {entity}",
                "Full load detected, fetching all historical data from 2024-01-01",
            )

        # End date is current time
        end_dt = datetime.now(timezone.utc)

        # Split large date ranges into 7-day chunks for faster individual requests
        # Smaller chunks = faster API responses, and we process them concurrently
        date_chunks = []
        current_start = start_dt

        while current_start < end_dt:
            # Create 7-day chunks (smaller = faster per request)
            current_end = min(current_start + timedelta(days=7), end_dt)
            date_chunks.append((current_start, current_end))
            current_start = current_end

        log_progress(
            f"API Extraction - {entity}",
            f"Split date range into {len(date_chunks)} chunk(s) (7 days each) for concurrent processing",
        )

        # Process chunks concurrently for maximum throughput
        # Use thread pool to process multiple chunks in parallel
        # Reduced to 4 workers to avoid overwhelming the API Lambdas
        all_records = []
        chunk_results = {}  # Store results by chunk index for ordering

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all chunk processing tasks
            future_to_chunk = {
                executor.submit(
                    process_chunk,
                    entity,
                    config,
                    chunk_start,
                    chunk_end,
                    chunk_idx + 1,
                    len(date_chunks),
                ): chunk_idx
                for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks)
            }

            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    result_chunk_idx, chunk_records = future.result()
                    chunk_results[result_chunk_idx] = chunk_records
                except Exception as e:
                    log_error(
                        f"API Extraction - {entity}",
                        f"Chunk {chunk_idx + 1} failed: {e}",
                    )
                    raise

        # Combine all chunk results in order
        for chunk_idx in sorted(chunk_results.keys()):
            all_records.extend(chunk_results[chunk_idx])

        log_progress(
            f"API Extraction - {entity}",
            f"Extracted {len(all_records)} records across {len(date_chunks)} chunk(s)",
        )

    except requests.exceptions.RequestException as e:
        log_error(f"API Extraction - {entity}", f"HTTP error: {e}")
        raise
    except Exception as e:
        log_error(f"API Extraction - {entity}", str(e))
        raise

    return all_records
