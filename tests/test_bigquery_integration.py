"""Integration tests for BigQuery connectivity."""

import logging
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import get_bigquery_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_credentials():
    """Check if BigQuery credentials exist."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")

    creds_file = Path(creds_path)
    if not creds_file.exists():
        pytest.skip(f"Credentials file not found: {creds_path}")


@pytest.fixture
def config():
    """Get BigQuery configuration."""
    check_credentials()
    return get_bigquery_config()


@pytest.fixture
def bigquery_client(config):
    """Create BigQuery client."""
    check_credentials()
    return bigquery.Client(project=config["project"]["id"])


def test_bigquery_authentication(bigquery_client, config):
    """Test that we can authenticate with BigQuery and access our project."""
    try:
        # Test basic authentication
        query = "SELECT 1"
        result = bigquery_client.query(query).result()
        assert next(result)[0] == 1, "Failed to execute test query"
        logger.info("Successfully authenticated with BigQuery")

        # Test we can access our project's datasets
        datasets = list(bigquery_client.list_datasets())
        assert len(datasets) > 0, "No datasets found in project"
        logger.info(f"Found {len(datasets)} datasets")

        # Verify our core dataset exists
        dataset_names = [ds.dataset_id for ds in datasets]
        core_dataset = config["datasets"]["core"]
        assert core_dataset in dataset_names, f"Core dataset '{core_dataset}' not found"
        logger.info("Successfully verified dataset access")

    except Exception as e:
        pytest.fail(f"Failed to authenticate with BigQuery: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
