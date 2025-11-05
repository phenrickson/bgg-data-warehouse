"""Tests for the BGG response processor."""

import logging
from datetime import datetime, UTC
from unittest.mock import Mock, patch

import pytest

from src.pipeline.process_responses import BGGResponseProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    client = Mock()
    # Mock DataFrame
    df_mock = Mock()
    records = [
        {
            "game_id": 13,
            "response_data": '{"items":{"item":{"@id":"13","@type":"boardgame","name":[{"@type":"primary","@value":"Catan"}],"yearpublished":{"@value":"1995"},"statistics":{"ratings":{"average":{"@value":"7.5"},"usersrated":{"@value":"1000"}}}}}}',
            "fetch_timestamp": datetime.now(UTC),
        },
        {
            "game_id": 9209,
            "response_data": '{"items":{"item":{"@id":"9209","@type":"boardgame","name":[{"@type":"primary","@value":"Ticket to Ride"}],"yearpublished":{"@value":"2004"},"statistics":{"ratings":{"average":{"@value":"7.4"},"usersrated":{"@value":"800"}}}}}}',
            "fetch_timestamp": datetime.now(UTC),
        },
    ]

    # Mock DataFrame methods
    df_mock.to_dict.return_value = {"records": records}
    df_mock.to_dict.side_effect = lambda format=None: (
        records if format == "records" else {"records": records}
    )
    df_mock.__len__ = lambda _: len(records)
    df_mock.__iter__ = lambda _: iter(records)
    df_mock.iterrows = lambda: ((i, row) for i, row in enumerate(records))

    # Mock query results
    def create_mock_result(count):
        mock_row = Mock()
        mock_row.count = count
        mock_result = Mock()
        mock_result.__iter__ = lambda _: iter([mock_row])
        mock_result.__next__ = lambda _: mock_row
        return mock_result

    # Set up query side effects for the entire test
    client.query = Mock()
    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = create_mock_result(2)

    client.query.return_value = query_result
    return client


@pytest.fixture
def mock_processor():
    """Mock BGG data processor."""
    processor = Mock()

    # Mock process_game method
    processor.process_game = Mock()

    def process_game_side_effect(game_id, response_data, game_type, load_timestamp=None):
        if game_id == 13:
            return {
                "game_id": 13,
                "type": "boardgame",
                "primary_name": "Catan",
                "year_published": 1995,
                "categories": ["Strategy"],
                "mechanics": ["Dice Rolling"],
                "average_rating": 7.5,
                "users_rated": 1000,
                "load_timestamp": load_timestamp or datetime.now(UTC),
            }
        elif game_id == 9209:
            return {
                "game_id": 9209,
                "type": "boardgame",
                "primary_name": "Ticket to Ride",
                "year_published": 2004,
                "categories": ["Strategy"],
                "mechanics": ["Set Collection"],
                "average_rating": 7.4,
                "users_rated": 800,
                "load_timestamp": load_timestamp or datetime.now(UTC),
            }
        return None

    processor.process_game.side_effect = process_game_side_effect

    # Mock prepare_for_bigquery method
    processor.prepare_for_bigquery = Mock()

    def prepare_for_bigquery_side_effect(games):
        import polars as pl

        return {
            "games": pl.DataFrame(
                [
                    {
                        "game_id": game["game_id"],
                        "type": game["type"],
                        "primary_name": game["primary_name"],
                        "year_published": game["year_published"],
                        "average_rating": game["average_rating"],
                        "users_rated": game["users_rated"],
                        "load_timestamp": game["load_timestamp"],
                    }
                    for game in games
                ]
            )
        }

    processor.prepare_for_bigquery.side_effect = prepare_for_bigquery_side_effect
    processor.validate_data = Mock(return_value=True)

    return processor


@pytest.fixture
def mock_loader():
    """Mock data loader."""
    loader = Mock()
    return loader


def test_get_unprocessed_responses(mock_bq_client):
    """Test fetching unprocessed responses."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client

    # Mock get_unprocessed_count to return a value
    def mock_count_result():
        mock_row = Mock()
        mock_row.count = 2
        return [mock_row]

    mock_bq_client.query().result = Mock(return_value=mock_count_result())

    responses = processor.get_unprocessed_responses()

    assert len(responses) == 2
    assert responses[0]["game_id"] == 13
    assert responses[1]["game_id"] == 9209
    assert "response_data" in responses[0]
    assert "response_data" in responses[1]


def test_process_batch_success(mock_bq_client, mock_processor, mock_loader):
    """Test successful batch processing."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    with patch("time.sleep"):  # Mock sleep to avoid delay
        result = processor.process_batch()

    assert result is True

    # Verify games were processed
    assert mock_processor.process_game.call_count == 2

    # Verify data was prepared
    assert mock_processor.prepare_for_bigquery.call_count == 1

    # Verify only games table was validated
    assert mock_processor.validate_data.call_count == 1

    # Verify data was loaded
    assert mock_loader.load_games.call_count == 1


def test_process_batch_missing_core_data(mock_bq_client, mock_processor, mock_loader):
    """Test processing when core game data is missing."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    # Mock process_game to return None for all games
    mock_processor.process_game.side_effect = [None, None]

    # Mock prepare_for_bigquery to return empty dataframes
    mock_processor.prepare_for_bigquery.return_value = {}

    with patch("time.sleep"):  # Mock sleep to avoid delay
        result = processor.process_batch()

    assert result is True
    # Verify no data was loaded due to missing core table
    assert mock_loader.load_games.call_count == 0


def test_process_batch_validation_failure(mock_bq_client, mock_processor, mock_loader):
    """Test handling validation failure."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    # Mock validate_data to fail
    mock_processor.validate_data.return_value = False

    with patch("time.sleep"):  # Mock sleep to avoid delay
        result = processor.process_batch()

    assert result is True
    # Verify no data was loaded due to validation failure
    assert mock_loader.load_games.call_count == 0


def test_process_batch_streaming_buffer_retry(mock_bq_client, mock_processor, mock_loader):
    """Test handling streaming buffer delay."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    # Create a mock DataFrame with proper len() support
    df_mock = Mock()
    records = [
        {
            "game_id": 13,
            "response_data": '{"items":{"item":{"@id":"13","@type":"boardgame"}}}',
            "fetch_timestamp": datetime.now(UTC),
        }
    ]
    df_mock.to_dict.return_value = {"records": records}
    df_mock.to_dict.side_effect = lambda format=None: (
        records if format == "records" else {"records": records}
    )
    df_mock.__len__ = lambda _: len(records)
    df_mock.__iter__ = lambda _: iter(records)
    df_mock.iterrows = lambda: ((i, row) for i, row in enumerate(records))

    # Mock query results
    def create_mock_result(count):
        mock_row = Mock()
        mock_row.count = count
        mock_result = Mock()
        mock_result.__iter__ = lambda _: iter([mock_row])
        mock_result.__next__ = lambda _: mock_row
        return mock_result

    # Set up query side effects
    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = create_mock_result(1)

    mock_bq_client.query = Mock()
    mock_bq_client.query.side_effect = [
        query_result,  # Initial query
        Mock(side_effect=Exception("Streaming buffer error")),  # First update fails
        Mock(),  # Second update succeeds
        Mock(result=Mock(return_value=create_mock_result(1))),  # Verify query
    ]

    with patch("time.sleep") as mock_sleep:  # Mock sleep to avoid delay
        result = processor.process_batch()

    assert result is True
    # Verify sleep was called for streaming buffer
    assert mock_sleep.call_count == 1
    assert mock_sleep.call_args[0][0] == 30  # 30 second delay


def test_process_batch_all_failures(mock_bq_client, mock_processor, mock_loader):
    """Test handling all games failing to process."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    # Mock process_game to return None for all games
    mock_processor.process_game.side_effect = [None, None]

    # Mock prepare_for_bigquery to return empty dataframes
    mock_processor.prepare_for_bigquery.return_value = {}

    # Mock validate_data to fail
    mock_processor.validate_data.return_value = False

    with patch("time.sleep"):  # Mock sleep to avoid delay
        result = processor.process_batch()

    assert result is True
    # Verify no data was loaded
    assert mock_loader.load_games.call_count == 0


def test_run_completion(mock_bq_client, mock_processor, mock_loader):
    """Test complete pipeline run."""
    processor = BGGResponseProcessor()
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader

    # Create mock DataFrame
    df_mock = Mock()
    records = [
        {
            "game_id": 13,
            "response_data": '{"items":{"item":{"@id":"13","@type":"boardgame"}}}',
            "fetch_timestamp": datetime.now(UTC),
        }
    ]
    df_mock.to_dict.return_value = {"records": records}
    df_mock.to_dict.side_effect = lambda format=None: (
        records if format == "records" else {"records": records}
    )
    df_mock.__len__ = lambda _: len(records)
    df_mock.__iter__ = lambda _: iter(records)
    df_mock.iterrows = lambda: ((i, row) for i, row in enumerate(records))

    # Mock query results
    def create_mock_result(count):
        mock_row = Mock()
        mock_row.count = count
        mock_result = Mock()
        mock_result.__iter__ = lambda _: iter([mock_row])
        mock_result.__next__ = lambda _: mock_row
        return mock_result

    # Set up query side effects
    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = create_mock_result(1)

    empty_result = Mock()
    empty_result.to_dataframe.return_value = df_mock
    empty_result.result.return_value = create_mock_result(0)

    mock_bq_client.query = Mock()
    mock_bq_client.query.side_effect = [
        Mock(result=Mock(return_value=create_mock_result(1))),  # Initial count
        query_result,  # Get unprocessed responses
        Mock(),  # Update query
        Mock(result=Mock(return_value=create_mock_result(1))),  # Verify query
        Mock(result=Mock(return_value=create_mock_result(0))),  # Second count (empty)
    ]

    with patch("time.sleep"):  # Mock sleep to avoid delay
        processor.run()

    # Verify pipeline completed after processing all data
    assert mock_loader.load_games.call_count == 1
    assert mock_bq_client.query.call_count == 5  # All expected queries were made


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
