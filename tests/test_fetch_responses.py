"""Tests for the BGG response fetcher."""

import logging
from datetime import datetime, UTC
from unittest.mock import Mock, patch

import pytest

from src.pipeline.fetch_responses import BGGResponseFetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    client = Mock()
    # Mock query result as DataFrame
    # Mock DataFrame with proper length and iteration
    records = [
        {
            "game_id": 13,
            "type": "boardgame"
        },
        {
            "game_id": 9209,
            "type": "boardgame"
        },
        {
            "game_id": 325,
            "type": "boardgame"
        }
    ]
    
    # Create a more robust mock DataFrame
    class MockDataFrame:
        def __init__(self, data):
            self._data = data
        
        def to_dataframe(self):
            return self
        
        def to_dict(self, orient='dict'):
            if orient == 'records':
                return self._data
            return {
                "game_id": [row["game_id"] for row in self._data],
                "type": [row["type"] for row in self._data]
            }
        
        def to_dicts(self):
            return self._data
        
        def __len__(self):
            return len(self._data)
        
        def __iter__(self):
            return iter(self._data)
        
        def iterrows(self):
            for row in self._data:
                yield row
        
        def __getattr__(self, name):
            # Fallback for any missing methods
            if name == 'to_dicts':
                return self.to_dicts
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    
    df_mock = MockDataFrame(records)
    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    client.query.return_value = query_result
    return client

@pytest.fixture
def mock_api_client():
    """Mock BGG API client."""
    client = Mock()
    
    # Mock get_thing method
    client.get_thing = Mock()
    def get_thing_side_effect(game_ids):
        if isinstance(game_ids, list) and len(game_ids) > 0:
            return {
                "items": {
                    "item": [
                        {
                            "@id": str(game_id),
                            "@type": "boardgame",
                            "name": [{"@type": "primary", "@value": f"Game {game_id}"}]
                        }
                        for game_id in game_ids
                    ]
                }
            }
        return None
    client.get_thing.side_effect = get_thing_side_effect
    return client

@pytest.fixture
def mock_id_fetcher():
    """Mock ID fetcher."""
    fetcher = Mock()
    fetcher.update_ids.return_value = None
    return fetcher

def test_get_unfetched_ids(mock_bq_client):
    """Test fetching unfetched game IDs."""
    fetcher = BGGResponseFetcher()
    fetcher.bq_client = mock_bq_client
    
    unfetched = fetcher.get_unfetched_ids()
    
    assert len(unfetched) == 3
    assert unfetched[0]["game_id"] == 13
    assert unfetched[0]["type"] == "boardgame"
    assert unfetched[1]["game_id"] == 9209
    assert unfetched[2]["game_id"] == 325

def test_store_response(mock_bq_client):
    """Test storing API responses."""
    fetcher = BGGResponseFetcher()
    fetcher.bq_client = mock_bq_client
    
    # Test storing single response
    game_ids = [13]
    response_data = '{"items": {"item": {"id": 13, "name": "Test Game"}}}'
    
    fetcher.store_response(game_ids, response_data)
    
    # Verify insert_rows_json was called correctly
    assert mock_bq_client.insert_rows_json.call_count == 1
    args = mock_bq_client.insert_rows_json.call_args
    table_id = args[0][0]  # First positional arg
    rows = args[0][1]      # Second positional arg
    
    assert "raw_responses" in table_id
    assert len(rows) == 1
    assert rows[0]["game_id"] == 13
    assert rows[0]["response_data"] == response_data
    assert rows[0]["processed"] is False
    assert rows[0]["process_attempt"] == 0

def test_fetch_batch_success(mock_bq_client, mock_api_client):
    """Test successful batch fetching."""
    fetcher = BGGResponseFetcher(batch_size=3, chunk_size=2)
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    result = fetcher.fetch_batch()
    
    assert result is True
    # Should have made 2 API calls (2 chunks of size 2)
    assert mock_api_client.get_thing.call_count == 2
    # Should have stored 2 responses
    assert mock_bq_client.insert_rows_json.call_count == 2

def test_fetch_batch_no_unfetched(mock_bq_client, mock_api_client):
    """Test batch fetching with no unfetched games."""
    fetcher = BGGResponseFetcher()
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Mock empty DataFrame
    df_mock = Mock()
    df_mock.to_dict.return_value = []
    df_mock.__len__ = lambda _: 0
    mock_bq_client.query.return_value.to_dataframe.return_value = df_mock
    
    result = fetcher.fetch_batch()
    
    assert result is False
    # Should not have made any API calls
    assert mock_api_client.get_thing.call_count == 0
    # Should not have stored any responses
    assert mock_bq_client.insert_rows_json.call_count == 0

def test_fetch_batch_api_error(mock_bq_client, mock_api_client):
    """Test handling API errors during batch fetching."""
    fetcher = BGGResponseFetcher()
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Mock API error
    mock_api_client.get_thing.side_effect = Exception("API Error")
    
    result = fetcher.fetch_batch()
    
    assert result is True  # Still returns True to continue processing
    # Should have attempted API call
    assert mock_api_client.get_thing.call_count == 1
    # Should not have stored any responses
    assert mock_bq_client.insert_rows_json.call_count == 0

def test_fetch_batch_storage_error(mock_bq_client, mock_api_client):
    """Test handling storage errors during batch fetching."""
    fetcher = BGGResponseFetcher()
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    # Mock storage error
    mock_bq_client.insert_rows_json.side_effect = Exception("Storage Error")
    
    result = fetcher.fetch_batch()
    
    assert result is True  # Still returns True to continue processing
    # Should have made API call
    assert mock_api_client.get_thing.call_count == 1
    # Should have attempted to store response
    assert mock_bq_client.insert_rows_json.call_count == 1

def test_run_dev_environment(mock_bq_client, mock_api_client, mock_id_fetcher):
    """Test running fetcher in dev environment."""
    fetcher = BGGResponseFetcher(environment="dev")
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    fetcher.id_fetcher = mock_id_fetcher
    
    # Mock get_unfetched_ids to return data once then empty
    original_get_unfetched = fetcher.get_unfetched_ids
    call_count = 0
    def mock_get_unfetched():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return original_get_unfetched()
        return []
    fetcher.get_unfetched_ids = mock_get_unfetched
    
    fetcher.run()
    
    # Should not have updated IDs in dev
    assert mock_id_fetcher.update_ids.call_count == 0
    # Should have processed one batch
    assert call_count == 2  # One successful batch + one empty check
    # Should have made API calls
    assert mock_api_client.get_thing.call_count > 0

def test_run_prod_environment(mock_bq_client, mock_api_client, mock_id_fetcher):
    """Test running fetcher in prod environment."""
    fetcher = BGGResponseFetcher(environment="prod")
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    fetcher.id_fetcher = mock_id_fetcher
    
    # Mock get_unfetched_ids to return data once then empty
    original_get_unfetched = fetcher.get_unfetched_ids
    call_count = 0
    def mock_get_unfetched():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return original_get_unfetched()
        return []
    fetcher.get_unfetched_ids = mock_get_unfetched
    
    with patch('pathlib.Path'):  # Mock Path for temp directory
        fetcher.run()
    
    # Should have updated IDs in prod
    assert mock_id_fetcher.update_ids.call_count == 1
    # Should have processed one batch
    assert call_count == 2  # One successful batch + one empty check
    # Should have made API calls
    assert mock_api_client.get_thing.call_count > 0

def test_chunk_processing(mock_bq_client, mock_api_client):
    """Test processing in chunks."""
    # Test with 5 games in chunks of 2
    records = [
        {"game_id": i, "type": "boardgame"}
        for i in range(1, 6)  # games 1-5
    ]
    
    # Create a MockDataFrame with the records using the fixture's MockDataFrame
    df_mock = mock_bq_client.query.return_value.to_dataframe.return_value.__class__(records)
    
    mock_bq_client.query.return_value.to_dataframe.return_value = df_mock
    
    fetcher = BGGResponseFetcher(batch_size=5, chunk_size=2)
    fetcher.bq_client = mock_bq_client
    fetcher.api_client = mock_api_client
    
    result = fetcher.fetch_batch()
    
    assert result is True
    # Should have made 3 API calls (2+2+1 games)
    assert mock_api_client.get_thing.call_count == 3
    # Should have stored 3 responses
    assert mock_bq_client.insert_rows_json.call_count == 3

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
