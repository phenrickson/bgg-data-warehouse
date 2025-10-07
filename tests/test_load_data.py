"""Tests for the BigQuery data loading module."""

from datetime import UTC, datetime
from unittest import mock

import pandas as pd
import polars as pl
import pytest
from google.cloud import bigquery

from src.pipeline.load_data import BigQueryLoader


@pytest.fixture
def mock_config(sample_config):
    """Mock configuration."""
    with mock.patch("src.pipeline.load_data.get_bigquery_config", return_value=sample_config):
        yield sample_config


@pytest.fixture
def loader(mock_config):
    """Create a loader instance with mocked clients."""
    with mock.patch("google.cloud.bigquery.Client") as mock_bq:
        with mock.patch("google.cloud.storage.Client") as mock_gcs:
            mock_bucket = mock.Mock()
            mock_gcs.return_value.bucket.return_value = mock_bucket

            # Initialize with test environment
            loader = BigQueryLoader(environment="test")
            loader.bucket = mock_bucket  # Replace the real bucket
            yield loader


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "game_id": [1, 2, 3],
            "name": ["Game 1", "Game 2", "Game 3"],
            "load_timestamp": [datetime.now(UTC)] * 3,
        }
    )


def test_upload_to_gcs(loader, sample_dataframe):
    """Test uploading data to GCS."""
    mock_blob = mock.Mock()
    mock_file = mock.Mock()
    mock_blob.open.return_value = mock_file
    loader.bucket.blob.return_value = mock_blob

    gcs_uri = loader._upload_to_gcs(sample_dataframe, "test_table")

    # Verify blob creation and upload
    loader.bucket.blob.assert_called_once_with("tmp/test_table.parquet")
    mock_blob.open.assert_called_once_with("wb")

    # Verify returned URI
    assert gcs_uri == f"gs://{loader.config['storage']['bucket']}/tmp/test_table.parquet"


def test_upload_to_gcs_error(loader, sample_dataframe):
    """Test handling GCS upload errors."""
    loader.bucket.blob.side_effect = Exception("Upload failed")

    result = loader._upload_to_gcs(sample_dataframe, "test_table")
    assert result is None


def test_load_from_gcs(loader):
    """Test loading data from GCS to BigQuery."""
    gcs_uri = "gs://test-bucket/tmp/test.parquet"
    table_ref = "project.dataset.table"

    mock_job = mock.Mock()
    loader.client.load_table_from_uri.return_value = mock_job

    result = loader._load_from_gcs(gcs_uri, table_ref)

    # Verify load job configuration
    loader.client.load_table_from_uri.assert_called_once()
    job_config = loader.client.load_table_from_uri.call_args[1]["job_config"]
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    assert job_config.write_disposition == "WRITE_APPEND"

    assert result is True


def test_load_from_gcs_error(loader):
    """Test handling BigQuery load errors."""
    gcs_uri = "gs://test-bucket/tmp/test.parquet"
    table_ref = "project.dataset.table"

    loader.client.load_table_from_uri.side_effect = Exception("Load failed")

    result = loader._load_from_gcs(gcs_uri, table_ref)
    assert result is False


def test_cleanup_gcs(loader):
    """Test cleaning up temporary files from GCS."""
    mock_blob = mock.Mock()
    loader.bucket.blob.return_value = mock_blob

    loader._cleanup_gcs("test_table")

    # Verify blob deletion
    loader.bucket.blob.assert_called_once_with("tmp/test_table.parquet")
    mock_blob.delete.assert_called_once()


def test_cleanup_gcs_error(loader):
    """Test handling cleanup errors."""
    loader.bucket.blob.side_effect = Exception("Cleanup failed")

    with mock.patch("src.pipeline.load_data.logger.error") as mock_logger:
        loader._cleanup_gcs("test_table")
        mock_logger.assert_called_once()


def test_load_table(loader, sample_dataframe):
    """Test complete table loading process."""
    # Mock successful GCS upload
    mock_blob = mock.Mock()
    mock_file = mock.Mock()
    mock_blob.open.return_value = mock_file
    loader.bucket.blob.return_value = mock_blob

    # Mock successful BigQuery load
    mock_job = mock.Mock()
    loader.client.load_table_from_uri.return_value = mock_job

    result = loader.load_table(sample_dataframe, "test_dataset", "test_table")

    assert result is True

    # Verify complete process
    loader.bucket.blob.assert_called()  # Upload to GCS
    loader.client.load_table_from_uri.assert_called_once()  # Load to BigQuery
    mock_blob.delete.assert_called_once()  # Cleanup


def test_load_table_gcs_error(loader, sample_dataframe):
    """Test handling GCS upload failure during table load."""
    loader.bucket.blob.side_effect = Exception("Upload failed")

    result = loader.load_table(sample_dataframe, "test_dataset", "test_table")

    assert result is False
    loader.client.load_table_from_uri.assert_not_called()


def test_load_table_bigquery_error(loader, sample_dataframe):
    """Test handling BigQuery load failure."""
    # Mock successful GCS upload
    mock_blob = mock.Mock()
    mock_file = mock.Mock()
    mock_blob.open.return_value = mock_file
    loader.bucket.blob.return_value = mock_blob

    # Mock BigQuery load failure
    loader.client.load_table_from_uri.side_effect = Exception("Load failed")

    result = loader.load_table(sample_dataframe, "test_dataset", "test_table")

    assert result is False
    mock_blob.delete.assert_called_once()  # Should still cleanup


def test_archive_raw_data(loader):
    """Test archiving raw data."""
    # Mock query result
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [1, 2, 3], "load_timestamp": [datetime.now(UTC)] * 3}
    )
    loader.client.query.return_value = mock_query_result

    # Mock GCS upload
    mock_blob = mock.Mock()
    mock_file = mock.Mock()
    mock_blob.open.return_value = mock_file
    loader.bucket.blob.return_value = mock_blob

    loader.archive_raw_data("games")

    # Verify archival process
    loader.client.query.assert_called_once()
    loader.bucket.blob.assert_called_once()
    mock_blob.open.assert_called_once()


def test_archive_raw_data_no_data(loader):
    """Test archiving when no data is available."""
    # Mock empty query result
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame()
    loader.client.query.return_value = mock_query_result

    loader.archive_raw_data("games")

    # Should not attempt to upload empty data
    loader.bucket.blob.assert_not_called()


def test_archive_raw_data_error(loader):
    """Test handling archival errors."""
    loader.client.query.side_effect = Exception("Query failed")

    with mock.patch("src.pipeline.load_data.logger.error") as mock_logger:
        loader.archive_raw_data("games")
        mock_logger.assert_called_once()
