"""Integration tests for the BGG response processor."""

import logging
import xmltodict
from unittest import mock

import pytest
import polars as pl

from src.pipeline.process_responses import BGGResponseProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_bq_client():
    """Create mock BigQuery client."""
    client = mock.Mock()
    
    # Mock unprocessed responses in raw_responses table format
    responses = [
        {
            "game_id": 13,
            "response_data": repr(xmltodict.parse("""<?xml version="1.0" encoding="utf-8"?>
<items termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
    <item type="boardgame" id="13">
        <name type="primary" value="Catan" />
        <yearpublished value="1995" />
        <minplayers value="3" />
        <maxplayers value="4" />
        <playingtime value="120" />
        <statistics>
            <ratings>
                <average value="7.5" />
                <usersrated value="1000" />
            </ratings>
        </statistics>
    </item>
</items>"""))
        },
        {
            "game_id": 9209,
            "response_data": repr(xmltodict.parse("""<?xml version="1.0" encoding="utf-8"?>
<items termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
    <item type="boardgame" id="9209">
        <name type="primary" value="Ticket to Ride" />
        <yearpublished value="2004" />
        <minplayers value="2" />
        <maxplayers value="5" />
        <playingtime value="60" />
        <statistics>
            <ratings>
                <average value="7.4" />
                <usersrated value="800" />
            </ratings>
        </statistics>
    </item>
</items>"""))
        }
    ]
    
    # Create a mock DataFrame that mimics the raw_responses table
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
                "response_data": [row["response_data"] for row in self._data]
            }
        
        def __len__(self):
            return len(self._data)

    df_mock = MockDataFrame(responses)
    query_result = mock.Mock()
    query_result.to_dataframe.return_value = df_mock
    client.query.return_value = query_result
    
    return client

@pytest.fixture
def mock_processor():
    """Create mock data processor."""
    processor = mock.Mock()
    
    def process_game(game_id, response_data, game_type):
        # Simulate processing for different game IDs
        processed_games = {
            13: {
                "game_id": 13,
                "primary_name": "Catan",
                "type": "boardgame",
                "year_published": 1995
            },
            9209: {
                "game_id": 9209,
                "primary_name": "Ticket to Ride",
                "type": "boardgame", 
                "year_published": 2004
            }
        }
        return processed_games.get(game_id)
    
    processor.process_game.side_effect = process_game
    
    def prepare_for_bigquery(games):
        # Convert processed games to DataFrame
        return {
            "games": pl.DataFrame(games)
        }
    
    processor.prepare_for_bigquery.side_effect = prepare_for_bigquery
    processor.validate_data.return_value = True
    
    return processor

@pytest.fixture
def mock_loader():
    """Create mock data loader."""
    loader = mock.Mock()
    loader.load_games.return_value = True
    return loader

def test_process_responses_basic_flow(mock_bq_client, mock_processor, mock_loader):
    """Test basic process responses workflow."""
    processor = BGGResponseProcessor(
        batch_size=2,
        max_retries=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader
    
    # Mock out time.sleep to prevent actual waiting
    with mock.patch('time.sleep'):
        # Run process
        result = processor.process_batch()
    
    # Verify result
    assert result is True
    
    # Verify processing steps
    assert mock_processor.process_game.call_count == 2
    assert mock_processor.prepare_for_bigquery.call_count == 1
    assert mock_loader.load_games.call_count == 1

def test_process_responses_error_handling(mock_bq_client, mock_processor, mock_loader):
    """Test error handling during processing."""
    processor = BGGResponseProcessor(
        batch_size=2,
        max_retries=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader
    
    # Simulate partial processing failure
    mock_processor.process_game.side_effect = [
        {"game_id": 13, "primary_name": "Catan"},
        None  # Simulate processing failure for second game
    ]
    
    # Mock out time.sleep to prevent actual waiting
    with mock.patch('time.sleep'):
        # Run process
        result = processor.process_batch()
    
    # Verify result
    assert result is True
    
    # Verify partial processing
    assert mock_processor.process_game.call_count == 2
    assert mock_loader.load_games.call_count == 1

def test_process_responses_validation_failure(mock_bq_client, mock_processor, mock_loader):
    """Test handling of data validation failure."""
    processor = BGGResponseProcessor(
        batch_size=2,
        max_retries=2,
        environment="dev"
    )
    
    # Replace clients with mocks
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader
    
    # Simulate validation failure
    mock_processor.validate_data.return_value = False
    
    # Run process
    result = processor.process_batch()
    
    # Verify result
    assert result is True
    
    # Verify no data loading occurred
    assert mock_loader.load_games.call_count == 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
