"""
Unit tests for ingestion configuration.
"""

import importlib
import os
from unittest.mock import patch

import pytest

import ingestion.config as config_module


def _reload_config():
    """
    Reload the ingestion.config module to ensure environment changes are picked up.
    """
    importlib.reload(config_module)
    return config_module.Config


class TestConfig:
    """Test configuration validation and path generation."""

    def test_get_bronze_path_full_load(self):
        """Test bronze path generation for full loads."""
        Config = _reload_config()
        path = Config.get_bronze_path("customers", "2024-01-15", "full")
        expected = "data/bronze/customers/ingestion_date=2024-01-15/load_type=full"
        assert path == expected

    def test_get_bronze_path_incremental_with_event_date(self):
        """Test bronze path generation for incremental loads with event date."""
        Config = _reload_config()
        path = Config.get_bronze_path("orders", "2024-01-15", "incremental", "2024-01-14")
        expected = "data/bronze/orders/ingestion_date=2024-01-15/load_type=incremental/event_date=2024-01-14"
        assert path == expected

    def test_get_log_path(self):
        """Test log path generation."""
        from datetime import datetime

        Config = _reload_config()
        run_time = datetime(2024, 1, 15, 10, 30, 45)
        path = Config.get_log_path(run_time)
        expected = "logs/pipeline/ingestion/2024/01/15/run-103045.log"
        assert path == expected

    @patch.dict(os.environ, {
        "S3_BUCKET_NAME": "test-bucket",
        "RDS_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-secret",
        "PAYMENTS_API_URL": "https://api.example.com/payments",
        "SHIPMENTS_API_URL": "https://api.example.com/shipments",
        "API_KEY_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:api-key",
    })
    def test_validate_config_success(self):
        """Test successful config validation."""
        # Should not raise any exceptions
        Config = _reload_config()
        Config.validate()

    @patch.dict(os.environ, {}, clear=True)
    def test_validate_config_missing_required(self):
        """Test config validation with missing required variables."""
        Config = _reload_config()
        with pytest.raises(ValueError) as exc_info:
            Config.validate()

        error_msg = str(exc_info.value)
        assert "S3_BUCKET_NAME" in error_msg
        assert "RDS_SECRET_ARN" in error_msg

    @patch.dict(os.environ, {
        "S3_BUCKET_NAME": "test-bucket",
        "RDS_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-secret",
        "PAYMENTS_API_URL": "https://api.example.com/payments",
        "SHIPMENTS_API_URL": "https://api.example.com/shipments",
        "API_KEY_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:api-key",
        "LOAD_TYPE": "INVALID",
    })
    def test_validate_config_invalid_load_type(self):
        """Test config validation with invalid load type."""
        Config = _reload_config()
        with pytest.raises(ValueError) as exc_info:
            Config.validate()

        error_msg = str(exc_info.value)
        assert "LOAD_TYPE must be either 'FULL' or 'INCREMENTAL'" in error_msg
