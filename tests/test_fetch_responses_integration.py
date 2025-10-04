"""Integration tests for the BGG response fetcher."""

import logging
from unittest import mock
from datetime import datetime

import pytest
import polars as pl
import pandas as pd

from src.pipeline.fetch_responses import BGGResponseFetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def mock_bq_client():
    """Create mock BigQuery client."""
    client = mock.Mock()

    # Mock query result for get_unfetched_ids
    df_mock = pd.DataFrame({"game_id": [13, 9209, 325, 164506, 357212], "type": ["boardgame"] * 5})

    query_result = mock.Mock()
    query_result.to_dataframe.return_value = df_mock
    client.query.return_value = query_result

    # Mock load job for store_response
    load_job = mock.Mock()
    load_job.result.return_value = mock.Mock()
    load_job.errors = None
    load_job_result = mock.Mock()
    load_job_result.input_file_bytes = 1000
    load_job_result.output_rows = 5
    load_job.result.return_value = load_job_result
    client.load_table_from_json.return_value = load_job

    # Mock table schema
    table = mock.Mock()
    schema_field = mock.Mock()
    schema_field.name = "response_data"
    schema_field.is_nullable = True
    table.schema = [schema_field]
    client.get_table.return_value = table

    return client


@pytest.fixture
def mock_api_client():
    """Create mock API client."""
    api_client = mock.Mock()

    def mock_get_thing(game_ids):
        # Simulate API response in the format expected by BGGResponseFetcher
        return {
            "items": {
                "item": [
                    {
                        "@id": str(game_id),
                        "name": {"@value": f"Game {game_id}"},
                        "yearpublished": {"@value": "2020"},
                        "minplayers": {"@value": "2"},
                        "maxplayers": {"@value": "4"},
                    }
                    for game_id in game_ids
                ]
            }
        }

    api_client.get_thing.side_effect = mock_get_thing
    return api_client


def test_fetch_responses_basic_flow(mock_bq_client, mock_api_client):
    """Test basic fetch responses workflow."""
    fetcher = BGGResponseFetcher(
        batch_size=5, chunk_size=2, environment="test"  # Use test environment
    )

    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client

    # Mock get_unfetched_ids to return expected format
    with mock.patch.object(fetcher, "get_unfetched_ids") as mock_get_unfetched:
        mock_get_unfetched.return_value = [
            {"game_id": 13, "type": "boardgame", "priority": "unfetched"},
            {"game_id": 9209, "type": "boardgame", "priority": "unfetched"},
            {"game_id": 325, "type": "boardgame", "priority": "unfetched"},
        ]

        # Mock store_response to avoid actual BigQuery operations
        with mock.patch.object(fetcher, "store_response") as mock_store:
            # Run fetch
            result = fetcher.fetch_batch()

            # Verify result
            assert result is True

            # Verify API calls
            assert mock_api_client.get_thing.call_count > 0

            # Verify store_response was called
            assert mock_store.call_count > 0


def test_fetch_responses_with_no_unfetched_ids(mock_bq_client, mock_api_client):
    """Test behavior when no unfetched IDs are available."""
    fetcher = BGGResponseFetcher(batch_size=5, chunk_size=2, environment="test")

    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client

    # Mock get_unfetched_ids to return empty list
    with mock.patch.object(fetcher, "get_unfetched_ids") as mock_get_unfetched:
        mock_get_unfetched.return_value = []

        # Run fetch
        result = fetcher.fetch_batch()

        # Verify result (should still be True in testing environment)
        assert result is True

        # Verify no API calls were made
        assert mock_api_client.get_thing.call_count == 0


def test_fetch_responses_api_error_handling(mock_bq_client, mock_api_client):
    """Test error handling when API returns no data."""
    fetcher = BGGResponseFetcher(batch_size=5, chunk_size=2, environment="test")

    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client

    # Mock API to return None (no data) - need to clear side_effect first
    mock_api_client.get_thing.side_effect = None
    mock_api_client.get_thing.return_value = None

    # Mock get_unfetched_ids to return expected format
    with mock.patch.object(fetcher, "get_unfetched_ids") as mock_get_unfetched:
        mock_get_unfetched.return_value = [
            {"game_id": 13, "type": "boardgame", "priority": "unfetched"}
        ]

        # Mock store_response to verify it's called with no_response
        with mock.patch.object(fetcher, "store_response") as mock_store:
            # Run fetch
            result = fetcher.fetch_batch()

            # Verify result
            assert result is True

            # Verify API was called
            assert mock_api_client.get_thing.call_count > 0

            # Verify store_response was called
            assert mock_store.call_count > 0
            # Check that store_response was called with no_response_ids parameter
            call_args_list = mock_store.call_args_list
            # Look for a call with no_response_ids parameter
            found_no_response_call = False
            for call_args in call_args_list:
                if len(call_args) >= 2 and call_args[1].get("no_response_ids"):
                    found_no_response_call = True
                    assert 13 in call_args[1]["no_response_ids"]
                    break
            assert (
                found_no_response_call
            ), "Expected store_response to be called with no_response_ids"


def test_fetch_responses_chunking(mock_bq_client, mock_api_client):
    """Test that fetcher properly chunks requests."""
    fetcher = BGGResponseFetcher(
        batch_size=10, chunk_size=3, environment="test"  # Small chunk size to test chunking
    )

    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client

    # Mock get_unfetched_ids to return more games than chunk size
    with mock.patch.object(fetcher, "get_unfetched_ids") as mock_get_unfetched:
        mock_get_unfetched.return_value = [
            {"game_id": i, "type": "boardgame", "priority": "unfetched"}
            for i in range(1, 8)  # 7 games, should require 3 chunks with chunk_size=3
        ]

        # Mock store_response
        with mock.patch.object(fetcher, "store_response") as mock_store:
            # Run fetch
            result = fetcher.fetch_batch()

            # Verify result
            assert result is True

            # Verify API was called multiple times (chunking)
            assert mock_api_client.get_thing.call_count >= 2

            # Verify store_response was called multiple times
            assert mock_store.call_count >= 2


def test_fetch_responses_refresh_vs_unfetched_priority(mock_bq_client, mock_api_client):
    """Test handling of different priority types."""
    fetcher = BGGResponseFetcher(batch_size=5, chunk_size=2, environment="test")

    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client

    # Mock get_unfetched_ids to return mix of priorities
    with mock.patch.object(fetcher, "get_unfetched_ids") as mock_get_unfetched:
        mock_get_unfetched.return_value = [
            {"game_id": 13, "type": "boardgame", "priority": "unfetched"},
            {"game_id": 9209, "type": "boardgame", "priority": "refresh"},
        ]

        # Mock store_response
        with mock.patch.object(fetcher, "store_response") as mock_store:
            # Run fetch
            result = fetcher.fetch_batch()

            # Verify result
            assert result is True

            # Verify API calls
            assert mock_api_client.get_thing.call_count > 0

            # Verify store_response was called
            assert mock_store.call_count > 0

            # Check that is_refresh parameter was used correctly
            # At least one call should have is_refresh=True for refresh priority
            refresh_calls = [
                call
                for call in mock_store.call_args_list
                if len(call[1]) > 0 and call[1].get("is_refresh", False)
            ]
            assert len(refresh_calls) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
