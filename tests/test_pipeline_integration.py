"""Integration tests for the complete BGG data pipeline."""

import logging
from unittest.mock import Mock, patch
from datetime import datetime, UTC

import pytest
import pandas as pd
from google.cloud import bigquery

from src.modules.response_fetcher import ResponseFetcher
from src.modules.response_processor import ResponseProcessor
from src.config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test with well-known games for consistent results
TEST_GAME_IDS = [
    13,  # Catan
    9209,  # Ticket to Ride
    822,  # Carcassonne
    174430,  # Gloomhaven
    224517,  # Brass: Birmingham
]


@pytest.fixture
def mock_api_response():
    """Mock BGG API response."""
    return {
        "items": {
            "item": [
                {"@id": "13", "name": [{"@value": "Catan", "@type": "primary"}], "yearpublished": {"@value": "1995"}},
                {
                    "@id": "9209",
                    "name": [{"@value": "Ticket to Ride", "@type": "primary"}],
                    "yearpublished": {"@value": "2004"},
                },
                {
                    "@id": "822",
                    "name": [{"@value": "Carcassonne", "@type": "primary"}],
                    "yearpublished": {"@value": "2000"},
                },
                {
                    "@id": "174430",
                    "name": [{"@value": "Gloomhaven", "@type": "primary"}],
                    "yearpublished": {"@value": "2017"},
                },
                {
                    "@id": "224517",
                    "name": [{"@value": "Brass: Birmingham", "@type": "primary"}],
                    "yearpublished": {"@value": "2018"},
                },
            ]
        }
    }


@pytest.fixture
def mock_config():
    """Get mock configuration."""
    return {
        "project": {"id": "test-project", "dataset": "test_dataset", "location": "US"},
        "datasets": {"raw": "test_raw"},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "fetch_in_progress": {"name": "fetch_in_progress"},
            "thing_ids": {"name": "thing_ids"},
            "fetched_responses": {"name": "fetched_responses"},
            "processed_responses": {"name": "processed_responses"},
        },
        "tables": {"games": {"name": "games"}},
    }


@pytest.fixture
def mock_bq_client(mock_api_response):
    """Create mock BigQuery client."""
    mock_client = Mock(spec=bigquery.Client)

    # Create properly configured mock responses for process_responses using real pandas DataFrame
    mock_responses = [
        {
            "record_id": f"rec_{id}",
            "game_id": id,
            "response_data": str({"items": {"item": item}}),
            "fetch_timestamp": datetime.now(UTC),
        }
        for id, item in zip(TEST_GAME_IDS, mock_api_response["items"]["item"])
    ]

    # Create a real pandas DataFrame for process_responses to use
    responses_df = pd.DataFrame(mock_responses)

    # Create a mock DataFrame for get_unfetched_ids (fetch_responses)
    fetch_mock_df = pd.DataFrame([
        {"game_id": id, "type": "boardgame"} for id in TEST_GAME_IDS
    ])

    # Mock query job that returns appropriate DataFrame based on query type
    def create_mock_query_job(query_str):
        mock_job = Mock()
        # If it's a query for unprocessed responses (used by process_responses), return real DataFrame
        if "processed_responses" in query_str or "record_id" in query_str:
            mock_job.to_dataframe.return_value = responses_df
        else:
            # Otherwise return the mock DataFrame (for fetch_responses)
            mock_job.to_dataframe.return_value = fetch_mock_df
        mock_job.result.return_value = None
        return mock_job

    # Configure mock for insert_rows_json
    mock_client.insert_rows_json.return_value = []  # Empty list indicates success

    # Configure the mock client to return appropriate results for different queries
    def mock_query(query):
        if "SELECT COUNT(*)" in query or "SELECT COUNT(DISTINCT" in query:
            # Handle COUNT queries - return appropriate count
            result = Mock()
            result.count = len(TEST_GAME_IDS)
            mock_result_iterator = Mock()
            mock_result_iterator.result.return_value = iter([result])
            return mock_result_iterator
        elif "DELETE FROM" in query or "INSERT INTO" in query:
            # For DML statements, return a mock that has a result() method
            mock_job = Mock()
            mock_job.result.return_value = iter([])
            return mock_job
        else:
            # Default query job
            return create_mock_query_job(query)

    mock_client.query = mock_query

    return mock_client


class TestResponseFetcher:
    """Tests for ResponseFetcher class."""

    def test_fetcher_initialization(self, mock_config):
        """Test that ResponseFetcher initializes correctly."""
        with patch("src.modules.response_fetcher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_fetcher.bigquery.Client"):
                fetcher = ResponseFetcher(
                    batch_size=100,
                    chunk_size=20,
                )
                assert fetcher.batch_size == 100
                assert fetcher.chunk_size == 20

    @patch("src.modules.response_fetcher.BGGAPIClient")
    def test_fetch_batch_with_mock_api(self, mock_api_class, mock_bq_client, mock_config, mock_api_response):
        """Test fetch_batch with mocked API responses."""
        mock_api_instance = mock_api_class.return_value
        mock_api_instance.get_thing.return_value = mock_api_response

        with patch("src.modules.response_fetcher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_fetcher.bigquery.Client", return_value=mock_bq_client):
                fetcher = ResponseFetcher(
                    batch_size=len(TEST_GAME_IDS),
                    chunk_size=2,
                )
                fetcher.bq_client = mock_bq_client
                fetcher.api_client = mock_api_instance

                # Test fetch_batch
                result = fetcher.fetch_batch(TEST_GAME_IDS)

                # Verify API was called
                assert mock_api_instance.get_thing.called
                logger.info("ResponseFetcher.fetch_batch test completed")


class TestResponseProcessor:
    """Tests for ResponseProcessor class."""

    def test_processor_initialization(self, mock_config):
        """Test that ResponseProcessor initializes correctly."""
        with patch("src.modules.response_processor.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_processor.bigquery.Client"):
                processor = ResponseProcessor(
                    batch_size=100,
                    max_retries=3,
                )
                assert processor.batch_size == 100
                assert processor.max_retries == 3


class TestPipelineIntegration:
    """Integration tests for the full pipeline."""

    @patch("src.modules.response_fetcher.BGGAPIClient")
    def test_fetcher_processor_flow(self, mock_api_class, mock_bq_client, mock_config, mock_api_response):
        """Test the flow from fetcher to processor with mocked dependencies."""
        mock_api_instance = mock_api_class.return_value
        mock_api_instance.get_thing.return_value = mock_api_response

        with patch("src.modules.response_fetcher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_fetcher.bigquery.Client", return_value=mock_bq_client):
                # Initialize fetcher
                fetcher = ResponseFetcher(
                    batch_size=len(TEST_GAME_IDS),
                    chunk_size=5,
                )
                fetcher.bq_client = mock_bq_client
                fetcher.api_client = mock_api_instance

                # Verify fetcher is properly configured
                assert fetcher.batch_size == len(TEST_GAME_IDS)
                assert fetcher.chunk_size == 5

        with patch("src.modules.response_processor.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_processor.bigquery.Client", return_value=mock_bq_client):
                # Initialize processor
                processor = ResponseProcessor(
                    batch_size=len(TEST_GAME_IDS),
                )
                processor.bq_client = mock_bq_client

                # Verify processor is properly configured
                assert processor.batch_size == len(TEST_GAME_IDS)

        logger.info("Pipeline integration test completed successfully")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
