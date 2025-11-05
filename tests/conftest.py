"""Common test fixtures."""

import os
from pathlib import Path
from unittest import mock

import pytest
from google.auth import credentials
from google.cloud import bigquery


@pytest.fixture
def mock_credentials():
    """Mock GCP credentials."""
    return mock.create_autospec(credentials.Credentials)


@pytest.fixture
def mock_bigquery_client(mock_credentials):
    """Mock BigQuery client."""
    with mock.patch("google.cloud.bigquery.Client", autospec=True) as mock_client:
        client = mock_client.return_value
        client._credentials = mock_credentials
        yield client


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "environments": {
            "dev": {
                "project_id": "test-project",
                "dataset": "test_dataset",
                "location": "US",
            },
            "prod": {
                "project_id": "test-project-prod",
                "dataset": "test_dataset_prod",
                "location": "US",
            },
        },
        "default_environment": "dev",
        "project": {"id": "test-project", "dataset": "test_dataset", "location": "US"},
        "datasets": {
            "raw": "test_raw",
            "transformed": "test_transformed",
            "reporting": "test_reporting",
            "monitoring": "test_monitoring",
        },
        "storage": {"bucket": "test-bucket"},
        "raw_tables": {
            "thing_ids": {
                "name": "thing_ids",
                "description": "Game IDs from BGG with processing status",
                "clustering_fields": ["game_id"],
            },
            "request_log": {
                "name": "request_log",
                "description": "API request tracking log",
                "time_partitioning": "request_timestamp",
            },
            "raw_responses": {
                "name": "raw_responses",
                "description": "Raw API responses before processing",
                "clustering_fields": ["game_id"],
                "time_partitioning": "fetch_timestamp",
            },
        },
        "tables": {
            "games": {
                "name": "games",
                "description": "Core game information",
                "clustering_fields": ["game_id"],
            }
        },
    }


@pytest.fixture
def mock_env():
    """Mock environment variables."""
    with mock.patch.dict(
        os.environ,
        {
            "GCP_PROJECT_ID": "test-project",
            "GCS_BUCKET": "test-bucket",
            "GOOGLE_APPLICATION_CREDENTIALS": str(
                Path.cwd() / "credentials" / "service-account-key.json"
            ),
        },
    ):
        yield


@pytest.fixture
def mock_storage_client(mock_credentials):
    """Mock GCS client."""
    with mock.patch("google.cloud.storage.Client", autospec=True) as mock_client:
        client = mock_client.return_value
        client._credentials = mock_credentials
        yield client


@pytest.fixture
def mock_blob():
    """Mock GCS blob."""
    mock_blob = mock.MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.download_as_string.return_value = b"test data"
    return mock_blob


@pytest.fixture
def mock_bucket(mock_blob):
    """Mock GCS bucket."""
    mock_bucket = mock.MagicMock()
    mock_bucket.blob.return_value = mock_blob
    return mock_bucket


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir
