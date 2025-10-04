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
            "test": {"project_id": "test-project", "dataset": "test_dataset", "location": "US"},
            "dev": {"project_id": "test-project", "dataset": "test_dataset_dev", "location": "US"},
        },
        "datasets": {
            "raw": "test_raw",
            "transformed": "test_transformed",
            "reporting": "test_reporting",
            "monitoring": "test_monitoring",
        },
        "storage": {"bucket": "test-bucket", "temp_prefix": "tmp/", "archive_prefix": "archive/"},
        "loading": {"batch_size": 1000, "max_bad_records": 0, "write_disposition": "WRITE_APPEND"},
        "monitoring": {
            "freshness_threshold_hours": 24,
            "quality_check_schedule": "0 */4 * * *",
            "alert_on_failures": True,
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
