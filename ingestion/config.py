"""
Configuration module for the ingestion pipeline.

Reads environment variables and provides configuration values for
RDS connections, API endpoints, S3 buckets, and incremental settings.
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

"""
Injects environment variables from a .env file when running outside AWS or in local development.

This ensures developers can set configuration locally in a .env instead of setting shell env vars.
"""


def _load_dotenv_if_present(dotenv_path: str = ".env") -> None:
    """
    Loads environment variables from a .env file if it exists and not running in AWS.

    Args:
        dotenv_path (str): Relative path to the .env file.
    """
    # Detect if running outside of AWS (i.e., local)
    is_local = True
    for aws_indicator in (
        "AWS_EXECUTION_ENV",
        "AWS_LAMBDA_FUNCTION_NAME",
        "ECS_CONTAINER_METADATA_URI",
    ):
        if os.getenv(aws_indicator):
            is_local = False
            break
    if is_local and os.path.exists(dotenv_path):
        try:
            with open(dotenv_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        # Do not overwrite existing environment variables
                        if key and key not in os.environ:
                            os.environ[key] = value
        except Exception as e:
            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Error in dotenv loading: {e}"
            )


env_path = Path(__file__).parent.parent / ".env"
_load_dotenv_if_present(str(env_path))


class Config:
    """
    Configuration class that reads environment variables for the ingestion pipeline.
    """

    # S3 Configuration
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "")
    BRONZE_PREFIX: str = "data/bronze"
    LOGS_PREFIX: str = "logs/pipeline/ingestion"

    # RDS Configuration
    RDS_SECRET_ARN: str = os.getenv("RDS_SECRET_ARN", "")

    # API Configuration
    PAYMENTS_API_URL: str = os.getenv("PAYMENTS_API_URL", "")
    SHIPMENTS_API_URL: str = os.getenv("SHIPMENTS_API_URL", "")
    API_KEY_SECRET_ARN: str = os.getenv("API_KEY_SECRET_ARN", "")

    # Lazy-loaded secrets caches
    _api_key_cache: Dict[str, str] = {}
    _rds_secret_cache: Dict[str, Any] = {}

    @classmethod
    def get_api_key(cls, service: str) -> str:
        """
        Get API key for a service from Secrets Manager (with caching).

        Args:
            service: Either 'payments' or 'shipments'

        Returns:
            The API key string
        """
        if service not in cls._api_key_cache:
            if not cls.API_KEY_SECRET_ARN:
                raise ValueError("API_KEY_SECRET_ARN environment variable is required")

            import boto3
            import json

            secrets_client = boto3.client("secretsmanager")
            try:
                response = secrets_client.get_secret_value(
                    SecretId=cls.API_KEY_SECRET_ARN
                )
                secret_data = json.loads(response["SecretString"])
                cls._api_key_cache[service] = secret_data["api_key"]
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve API key from Secrets Manager: {e}"
                )

        return cls._api_key_cache[service]

    @classmethod
    def get_payments_api_key(cls) -> str:
        """Get payments API key from Secrets Manager."""
        return cls.get_api_key("payments")

    @classmethod
    def get_shipments_api_key(cls) -> str:
        """Get shipments API key from Secrets Manager."""
        return cls.get_api_key("shipments")

    @classmethod
    def _load_rds_secret(cls) -> Dict[str, Any]:
        """
        Retrieve and cache the RDS secret from AWS Secrets Manager.

        Returns:
            Dict containing the secret payload.
        """
        if not cls._rds_secret_cache:
            if not cls.RDS_SECRET_ARN:
                raise ValueError("RDS_SECRET_ARN environment variable is required")

            import boto3

            secrets_client = boto3.client("secretsmanager")
            try:
                response = secrets_client.get_secret_value(SecretId=cls.RDS_SECRET_ARN)
                cls._rds_secret_cache = json.loads(response["SecretString"])
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve RDS secret from Secrets Manager: {e}"
                )
        return cls._rds_secret_cache

    @classmethod
    def get_rds_connection_details(cls) -> Dict[str, Any]:
        """
        Provide database connection details sourced from the RDS secret.

        Returns:
            Dict containing host, port, database, user, and password.
        """
        secret = cls._load_rds_secret()

        required_keys = ["host", "port", "username", "password"]
        missing_keys = [key for key in required_keys if key not in secret]
        if missing_keys:
            raise ValueError(
                f"RDS secret missing required keys: {', '.join(missing_keys)}"
            )

        database_name = secret.get("dbname") or secret.get("database")
        if not database_name:
            raise ValueError("RDS secret must include either 'dbname' or 'database'")

        return {
            "host": secret["host"],
            "port": int(secret["port"]),
            "database": database_name,
            "user": secret["username"],
            "password": secret["password"],
        }

    # Incremental State Configuration
    INITIAL_LOAD_DATE: str = "1970-01-01 00:00:00"

    # Lambda Configuration
    LOAD_TYPE: str = os.getenv("LOAD_TYPE", "INCREMENTAL").upper()

    @classmethod
    def validate(cls) -> None:
        """
        Validate that required configuration values are present.

        Raises:
            ValueError: If any required configuration is missing.
        """
        required_vars = [
            ("S3_BUCKET_NAME", cls.S3_BUCKET_NAME),
            ("RDS_SECRET_ARN", cls.RDS_SECRET_ARN),
            ("PAYMENTS_API_URL", cls.PAYMENTS_API_URL),
            ("SHIPMENTS_API_URL", cls.SHIPMENTS_API_URL),
            ("API_KEY_SECRET_ARN", cls.API_KEY_SECRET_ARN),
        ]

        missing = [name for name, value in required_vars if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        if cls.LOAD_TYPE not in ["FULL", "INCREMENTAL"]:
            raise ValueError("LOAD_TYPE must be either 'FULL' or 'INCREMENTAL'")

    @classmethod
    def get_bronze_path(
        cls,
        entity: str,
        ingestion_date: str,
        load_type: str,
        event_date: Optional[str] = None,
    ) -> str:
        """
        Generate the S3 path for bronze tier data.

        Args:
            entity: The entity name (e.g., 'customers', 'orders')
            ingestion_date: Date when data was ingested (YYYY-MM-DD)
            load_type: 'full' or 'incremental'
            event_date: Optional event date for partitioning (YYYY-MM-DD)

        Returns:
            str: S3 path for the bronze data
        """
        path = f"{cls.BRONZE_PREFIX}/{entity}/ingestion_date={ingestion_date}/load_type={load_type.lower()}"
        if event_date:
            path += f"/event_date={event_date}"
        return path

    @classmethod
    def get_log_path(cls, run_timestamp: datetime) -> str:
        """
        Generate the S3 path for ingestion logs.

        Args:
            run_timestamp: Timestamp when the run started

        Returns:
            str: S3 path for the log file
        """
        date_str = run_timestamp.strftime("%Y/%m/%d")
        time_str = run_timestamp.strftime("%H%M%S")
        return f"{cls.LOGS_PREFIX}/{date_str}/run-{time_str}.log"
