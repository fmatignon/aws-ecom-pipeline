"""
Unit tests for state management (incremental checkpoints).
"""

import pytest
import json
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from ingestion.utils.state import get_last_sync, update_sync
from ingestion.config import Config


class TestStateStore:
    """Test checkpoint management for incremental loads."""

    @patch('ingestion.state.store.get_s3_client')
    def test_get_last_sync_existing_checkpoint(self, mock_client):
        """Test retrieving existing checkpoint."""
        # Mock S3 response
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            'Body': MagicMock()
        }
        mock_s3.get_object.return_value['Body'].read.return_value = json.dumps({
            'customers': {
                'last_sync_timestamp': '2024-01-15T10:30:00'
            }
        }).encode('utf-8')
        mock_client.return_value = mock_s3

        result = get_last_sync('customers')
        assert result == '2024-01-15T10:30:00'

    @patch('ingestion.state.store.get_s3_client')
    def test_get_last_sync_no_checkpoint(self, mock_client):
        """Test retrieving checkpoint when none exists."""
        # Mock S3 response for NoSuchKey
        from botocore.exceptions import ClientError
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        mock_client.return_value = mock_s3

        result = get_last_sync('customers')
        assert result == Config.INITIAL_LOAD_DATE

    @patch('ingestion.state.store.get_s3_client')
    def test_get_last_sync_error_fallback(self, mock_client):
        """Test error handling falls back to initial date."""
        # Mock S3 client error
        mock_client.side_effect = Exception("S3 error")

        result = get_last_sync('customers')
        assert result == Config.INITIAL_LOAD_DATE

    @patch('ingestion.state.store.get_s3_client')
    def test_update_sync_success(self, mock_client):
        """Test successful checkpoint update."""
        # Mock S3 for reading existing checkpoints (empty) and writing
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        mock_client.return_value = mock_s3

        # Should not raise exception
        update_sync('customers', '2024-01-15T10:30:00', 150)

        # Verify put_object was called
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args[1]

        assert call_args['Bucket'] == Config.S3_BUCKET_NAME
        assert call_args['Key'] == f"{Config.LOGS_PREFIX}/checkpoints.json"

        # Verify the JSON content
        written_content = json.loads(call_args['Body'])
        assert 'customers' in written_content
        assert written_content['customers']['last_sync_timestamp'] == '2024-01-15T10:30:00'
        assert written_content['customers']['record_count'] == 150

    @patch('ingestion.state.store.get_s3_client')
    def test_update_sync_error(self, mock_client):
        """Test error handling in checkpoint update."""
        mock_client.side_effect = Exception("S3 error")

        with pytest.raises(Exception) as exc_info:
            update_sync('customers', '2024-01-15T10:30:00', 150)

        assert "S3 error" in str(exc_info.value)
