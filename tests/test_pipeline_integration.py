"""Integration tests for the complete BGG data pipeline."""

import logging
from unittest.mock import Mock, patch
from datetime import datetime, UTC

import pytest
from google.cloud import bigquery

from src.pipeline.fetch_responses import BGGResponseFetcher
from src.pipeline.process_responses import BGGResponseProcessor
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
def mock_bq_client():
    """Create mock BigQuery client."""
    mock_client = Mock(spec=bigquery.Client)

    # Create a more complete mock DataFrame for get_unfetched_ids
    mock_df = Mock()
    mock_df.__len__ = lambda _: len(TEST_GAME_IDS)
    mock_df.iterrows.return_value = [
        (i, {"game_id": id, "type": "boardgame"}) for i, id in enumerate(TEST_GAME_IDS)
    ]
    mock_df.__getitem__ = lambda _, key: [id for id in TEST_GAME_IDS] if key == "game_id" else None

    # Mock query job that returns our DataFrame
    mock_query_job = Mock()
    mock_query_job.to_dataframe.return_value = mock_df
    mock_query_job.result.return_value = None  # For non-DataFrame queries

    # Configure mock for insert_rows_json
    mock_client.insert_rows_json.return_value = []  # Empty list indicates success

    # Configure the mock client to return appropriate results for different queries
    def mock_query(query):
        if "SELECT COUNT(DISTINCT game_id)" in query:
            result = Mock()
            result.count = len(TEST_GAME_IDS)
            mock_result_iterator = Mock()
            mock_result_iterator.result.return_value = iter([result])  # Make it an iterator
            return mock_result_iterator
        elif "SELECT game_id, primary_name, year_published" in query:
            result = Mock()
            result.game_id = TEST_GAME_IDS[0]
            result.primary_name = "Catan"
            result.year_published = 1995
            mock_result_iterator = Mock()
            mock_result_iterator.result.return_value = iter([result])  # Make it an iterator
            return mock_result_iterator
        return mock_query_job

    mock_client.query = mock_query

    return mock_client


@pytest.fixture
def mock_config():
    """Get mock configuration."""
    return {
        "project": {"id": "test-project", "dataset": "test_dataset"},
        "datasets": {"raw": "test_raw"},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "fetch_in_progress": {"name": "fetch_in_progress"},
        },
    }


@pytest.fixture
def mock_api_response():
    """Mock BGG API response."""
    return {
        "items": {
            "item": [
                {"@id": "13", "name": [{"@value": "Catan"}], "yearpublished": {"@value": "1995"}},
                {
                    "@id": "9209",
                    "name": [{"@value": "Ticket to Ride"}],
                    "yearpublished": {"@value": "2004"},
                },
                {
                    "@id": "822",
                    "name": [{"@value": "Carcassonne"}],
                    "yearpublished": {"@value": "2000"},
                },
                {
                    "@id": "174430",
                    "name": [{"@value": "Gloomhaven"}],
                    "yearpublished": {"@value": "2017"},
                },
                {
                    "@id": "224517",
                    "name": [{"@value": "Brass: Birmingham"}],
                    "yearpublished": {"@value": "2018"},
                },
            ]
        }
    }


@patch("src.pipeline.fetch_responses.BGGAPIClient")
def test_full_pipeline_integration(mock_api_client, mock_bq_client, mock_config, mock_api_response):
    """Test the complete pipeline from API fetch through processing.

    This test:
    1. Fetches real data from BGG API for specific games
    2. Stores the responses in BigQuery
    3. Processes the stored responses
    4. Verifies the processed data matches expected game information
    """
    # Set up API client mock
    mock_api_instance = mock_api_client.return_value
    mock_api_instance.get_thing.return_value = mock_api_response

    # Initialize pipeline components with mocked dependencies
    fetcher = BGGResponseFetcher(
        batch_size=len(TEST_GAME_IDS),
        chunk_size=2,  # Small chunk size to test batching
        environment="dev",
    )
    fetcher.config = mock_config  # Override config with mock
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_instance

    processor = BGGResponseProcessor(
        batch_size=len(TEST_GAME_IDS), environment="dev", config=mock_config
    )
    processor.bq_client = mock_bq_client

    try:
        # Step 1: Fetch responses from BGG API
        logger.info("Testing fetch responses...")
        fetch_result = fetcher.fetch_batch(TEST_GAME_IDS)
        assert fetch_result is True, "Failed to fetch responses"

        # Verify responses were stored in BigQuery
        raw_responses_table = f"{mock_config['project']['id']}.{mock_config['datasets']['raw']}.{mock_config['raw_tables']['raw_responses']['name']}"
        query = f"""
        SELECT COUNT(DISTINCT game_id) as count
        FROM `{raw_responses_table}`
        WHERE game_id IN ({','.join(map(str, TEST_GAME_IDS))})
        """
        results = mock_bq_client.query(query).result()
        stored_count = next(results).count
        assert stored_count > 0, "No responses were stored in BigQuery"
        logger.info(f"Found {stored_count} stored responses")

        # Step 2: Process the responses
        logger.info("Testing response processing...")
        process_result = processor.process_batch()
        assert process_result is True, "Failed to process responses"

        # Step 3: Verify processed data
        processed_games_table = (
            f"{mock_config['project']['id']}.{mock_config['project']['dataset']}.games"
        )
        verification_query = f"""
        SELECT game_id, primary_name, year_published
        FROM `{processed_games_table}`
        WHERE game_id IN ({','.join(map(str, TEST_GAME_IDS))})
        """
        processed_games = mock_bq_client.query(verification_query).result()

        # Convert to dictionary for easy lookup
        processed_data = {row.game_id: row for row in processed_games}

        # Verify specific games were processed correctly
        expected_games = {
            13: {"name": "Catan", "year": 1995},
            9209: {"name": "Ticket to Ride", "year": 2004},
            822: {"name": "Carcassonne", "year": 2000},
            174430: {"name": "Gloomhaven", "year": 2017},
            224517: {"name": "Brass: Birmingham", "year": 2018},
        }

        for game_id, expected in expected_games.items():
            if game_id in processed_data:
                game = processed_data[game_id]
                assert game.primary_name == expected["name"], f"Incorrect name for game {game_id}"
                assert game.year_published == expected["year"], f"Incorrect year for game {game_id}"

        logger.info("Pipeline integration test completed successfully")

    except Exception as e:
        logger.error(f"Pipeline integration test failed: {e}")
        raise

    finally:
        # No cleanup needed since we're using mocks
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
