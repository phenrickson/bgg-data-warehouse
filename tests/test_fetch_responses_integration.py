"""Integration tests for the BGG response fetcher."""

import logging
from unittest import mock

import pytest
import polars as pl

from src.pipeline.fetch_responses import BGGResponseFetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_bq_client():
    """Create mock BigQuery client."""
    client = mock.Mock()
    
    # Mock query result
    df_mock = pl.DataFrame({
        "game_id": [13, 9209, 325, 164506, 357212],
        "type": ["boardgame"] * 5
    })
    
    query_result = mock.Mock()
    query_result.to_dataframe.return_value = df_mock
    client.query.return_value = query_result
    
    return client

@pytest.fixture
def mock_api_client():
    """Create mock API client."""
    api_client = mock.Mock()
    
    def mock_get_thing(game_ids):
        # Simulate API response for different game IDs
        responses = {
            13: '{"items":{"item":{"@id":"13","name":"Catan"}}}',
            9209: '{"items":{"item":{"@id":"9209","name":"Ticket to Ride"}}}',
            325: '{"items":{"item":{"@id":"325","name":"Catan: Seafarers"}}}',
            164506: '{"items":{"item":{"@id":"164506","name":"Less Common Game 1"}}}',
            357212: '{"items":{"item":{"@id":"357212","name":"Less Common Game 2"}}}'
        }
        return {
            "items": {
                "item": [
                    {"@id": str(game_id), "name": responses[game_id]}
                    for game_id in game_ids
                ]
            }
        }
    
    api_client.get_thing.side_effect = mock_get_thing
    return api_client

def test_fetch_responses_basic_flow(mock_bq_client, mock_api_client):
    """Test basic fetch responses workflow."""
    fetcher = BGGResponseFetcher(
        batch_size=5,
        chunk_size=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Run fetch
    result = fetcher.fetch_batch()
    
    # Verify result
    assert result is True
    
    # Verify API calls
    assert mock_api_client.get_thing.call_count > 0
    
    # Verify BigQuery insert
    assert mock_bq_client.insert_rows_json.call_count > 0

def test_fetch_responses_rate_limiting(mock_bq_client, mock_api_client):
    """Test rate limiting behavior."""
    fetcher = BGGResponseFetcher(
        batch_size=5,
        chunk_size=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Simulate rate limiting
    mock_api_client.get_thing.side_effect = [
        {"items": {"item": [{"@id": "13", "name": "Catan"}]}},
        Exception("Rate limited"),
        {"items": {"item": [{"@id": "9209", "name": "Ticket to Ride"}]}}
    ]
    
    # Run fetch
    result = fetcher.fetch_batch()
    
    # Verify result
    assert result is True
    
    # Verify retry mechanism
    assert mock_api_client.get_thing.call_count == 3

def test_fetch_responses_error_handling(mock_bq_client, mock_api_client):
    """Test error handling during fetch."""
    fetcher = BGGResponseFetcher(
        batch_size=5,
        chunk_size=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Simulate partial failure
    mock_api_client.get_thing.side_effect = [
        {"items": {"item": [{"@id": "13", "name": "Catan"}]}},
        Exception("API Error"),
        {"items": {"item": [{"@id": "9209", "name": "Ticket to Ride"}]}}
    ]
    
    # Simulate BigQuery insert failure for some rows
    mock_bq_client.insert_rows_json.side_effect = [
        [],  # Successful insert
        ["Error"],  # Failed insert
        []  # Successful insert
    ]
    
    # Run fetch
    result = fetcher.fetch_batch()
    
    # Verify result
    assert result is True
    
    # Verify partial processing
    assert mock_api_client.get_thing.call_count == 3
    assert mock_bq_client.insert_rows_json.call_count == 2

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
