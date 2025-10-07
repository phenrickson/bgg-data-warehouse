"""Tests for the BGG response processor."""

import logging
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import polars as pl
import pytest

from src.pipeline.process_responses import BGGResponseProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client with realistic response structure."""
    client = Mock()

    # Mock DataFrame that behaves like pandas DataFrame from BigQuery
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

    # Mock pandas-like DataFrame behavior - rows need to be subscriptable like pandas Series
    class MockRow:
        def __init__(self, data):
            self.data = data
            # Make it subscriptable
            for key, value in data.items():
                setattr(self, key, value)

        def __getitem__(self, key):
            return self.data[key]

        def get(self, key, default=None):
            return self.data.get(key, default)

    df_mock.iterrows.return_value = [(0, MockRow(records[0])), (1, MockRow(records[1]))]
    df_mock.__len__ = lambda _: len(records)
    df_mock.__iter__ = lambda _: iter(records)

    # Mock query results
    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = iter([Mock(count=2)])  # For count queries
    query_result.num_dml_affected_rows = 2  # For DML operations

    client.query.return_value = query_result
    return client


@pytest.fixture
def mock_processor():
    """Mock BGG data processor with updated interface."""
    processor = Mock()

    # Mock process_game method with load_timestamp parameter
    def process_game_side_effect(game_id, response_data, game_type, load_timestamp=None):
        if game_id == 13:
            return {
                "game_id": 13,
                "type": "boardgame",
                "primary_name": "Catan",
                "year_published": 1995,
                "categories": [{"id": 1, "name": "Strategy"}],
                "mechanics": [{"id": 1, "name": "Dice Rolling"}],
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
                "categories": [{"id": 2, "name": "Strategy"}],
                "mechanics": [{"id": 2, "name": "Set Collection"}],
                "average_rating": 7.4,
                "users_rated": 800,
                "load_timestamp": load_timestamp or datetime.now(UTC),
            }
        return None

    processor.process_game.side_effect = process_game_side_effect

    # Mock prepare_for_bigquery to return Polars DataFrames
    def prepare_for_bigquery_side_effect(games):
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
    processor.validate_data.return_value = True

    return processor


@pytest.fixture
def mock_loader():
    """Mock data loader."""
    loader = Mock()
    loader.load_games.return_value = None
    return loader


@pytest.fixture
def processor_instance(mock_bq_client, mock_processor, mock_loader):
    """Create processor instance with mocked dependencies."""
    processor = BGGResponseProcessor(environment="test")
    processor.bq_client = mock_bq_client
    processor.processor = mock_processor
    processor.loader = mock_loader
    return processor


def test_get_unprocessed_count(processor_instance, mock_bq_client):
    """Test getting count of unprocessed responses."""
    # Mock query result for count
    count_result = Mock()
    count_result.result.return_value = iter([Mock(count=5)])
    mock_bq_client.query.return_value = count_result

    count = processor_instance.get_unprocessed_count()

    assert count == 5
    assert mock_bq_client.query.called


def test_get_unprocessed_count_error_handling(processor_instance, mock_bq_client):
    """Test error handling in get_unprocessed_count."""
    mock_bq_client.query.side_effect = Exception("BigQuery error")

    count = processor_instance.get_unprocessed_count()

    assert count == 0


def test_get_unprocessed_responses_success(processor_instance):
    """Test successful retrieval of unprocessed responses."""
    responses = processor_instance.get_unprocessed_responses()

    assert len(responses) == 2
    assert responses[0]["game_id"] == 13
    assert responses[1]["game_id"] == 9209
    assert "response_data" in responses[0]
    assert "fetch_timestamp" in responses[0]


def test_get_unprocessed_responses_empty_data(processor_instance, mock_bq_client):
    """Test handling of empty response data."""

    # Mock DataFrame with empty response_data
    class MockRow:
        def __init__(self, data):
            self.data = data
            for key, value in data.items():
                setattr(self, key, value)

        def __getitem__(self, key):
            return self.data[key]

    df_mock = Mock()
    df_mock.iterrows.return_value = [
        (0, MockRow({"game_id": 13, "response_data": "", "fetch_timestamp": datetime.now(UTC)})),
        (
            1,
            MockRow({"game_id": 14, "response_data": "   ", "fetch_timestamp": datetime.now(UTC)}),
        ),  # whitespace only
    ]

    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = None  # For update queries
    mock_bq_client.query.return_value = query_result

    responses = processor_instance.get_unprocessed_responses()

    # Should return empty list since all responses are empty
    assert len(responses) == 0
    # Should have called update query to mark as no_response
    assert mock_bq_client.query.call_count >= 2


def test_get_unprocessed_responses_parse_error(processor_instance, mock_bq_client):
    """Test handling of response data parsing errors."""

    # Mock DataFrame with invalid JSON
    class MockRow:
        def __init__(self, data):
            self.data = data
            for key, value in data.items():
                setattr(self, key, value)

        def __getitem__(self, key):
            return self.data[key]

    df_mock = Mock()
    df_mock.iterrows.return_value = [
        (
            0,
            MockRow(
                {
                    "game_id": 13,
                    "response_data": "invalid json",
                    "fetch_timestamp": datetime.now(UTC),
                }
            ),
        )
    ]

    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    query_result.result.return_value = None  # For update queries
    mock_bq_client.query.return_value = query_result

    responses = processor_instance.get_unprocessed_responses()

    # Should return empty list due to parse error
    assert len(responses) == 0
    # Should have called update query to mark as parse_error
    assert mock_bq_client.query.call_count >= 2


def test_process_batch_success(processor_instance):
    """Test successful batch processing."""
    with patch("time.sleep"):  # Mock sleep to avoid delay
        result = processor_instance.process_batch()

    assert result is True

    # Verify games were processed with correct parameters
    assert processor_instance.processor.process_game.call_count == 2

    # Check that load_timestamp was passed
    call_args = processor_instance.processor.process_game.call_args_list
    for call in call_args:
        args, kwargs = call
        # The implementation passes game_id, response_data as positional args
        # and game_type, load_timestamp as keyword arguments
        assert len(args) == 2  # game_id, response_data
        assert "game_type" in kwargs
        assert "load_timestamp" in kwargs

    # Verify data preparation and validation
    assert processor_instance.processor.prepare_for_bigquery.call_count == 1
    assert processor_instance.processor.validate_data.call_count == 1

    # Verify data loading
    assert processor_instance.loader.load_games.call_count == 1


def test_process_batch_no_responses(processor_instance, mock_bq_client):
    """Test processing when no unprocessed responses exist."""
    # Mock empty DataFrame
    df_mock = Mock()
    df_mock.iterrows.return_value = []

    query_result = Mock()
    query_result.to_dataframe.return_value = df_mock
    mock_bq_client.query.return_value = query_result

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    # Should return True in test environment even with no responses
    assert result is True
    assert processor_instance.processor.process_game.call_count == 0


def test_process_batch_processing_failures(processor_instance):
    """Test handling of game processing failures."""
    # Mock process_game to return None (failure)
    processor_instance.processor.process_game.side_effect = [None, None]

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    # Should return True in test environment even with failures
    assert result is True
    # Should not load any data since no games processed
    assert processor_instance.loader.load_games.call_count == 0


def test_process_batch_validation_failure(processor_instance):
    """Test handling of data validation failure."""
    processor_instance.processor.validate_data.return_value = False

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    # Should return True in test environment even on validation failure
    assert result is True
    # Should not load data due to validation failure
    assert processor_instance.loader.load_games.call_count == 0


def test_process_batch_processing_exception(processor_instance):
    """Test handling of exceptions during game processing."""
    processor_instance.processor.process_game.side_effect = Exception("Processing error")

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    # Should return True in test environment even with exceptions
    assert result is True
    assert processor_instance.loader.load_games.call_count == 0


def test_process_batch_dml_affected_rows_mismatch(processor_instance, mock_bq_client):
    """Test handling of DML affected rows count mismatch."""
    # Mock query result with mismatched affected rows
    query_result = Mock()
    query_result.result.return_value = None
    query_result.num_dml_affected_rows = 1  # Less than expected 2

    # Set up the mock to return different results for different queries
    def query_side_effect(query_str):
        if "UPDATE" in query_str and "processed = TRUE" in query_str:
            return query_result
        elif "SELECT game_id, processed, process_status" in query_str:
            # Mock check query result
            check_result = Mock()
            check_result.result.return_value = [
                Mock(game_id=13, processed=True, process_status="success"),
                Mock(game_id=9209, processed=False, process_status=None),
            ]
            return check_result
        else:
            # Return original mock for other queries
            return processor_instance.bq_client.query.return_value

    mock_bq_client.query.side_effect = query_side_effect

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    assert result is True


def test_process_batch_update_query_failure(processor_instance, mock_bq_client):
    """Test handling of update query failures."""
    # Mock successful processing but failed update
    original_query = mock_bq_client.query

    def query_side_effect(query_str):
        if "UPDATE" in query_str and "processed = TRUE" in query_str:
            raise Exception("Update failed")
        return original_query.return_value

    mock_bq_client.query.side_effect = query_side_effect

    with patch("time.sleep"):
        result = processor_instance.process_batch()

    # Should return False due to update failure in all environments
    assert result is False


def test_run_pipeline_completion(processor_instance):
    """Test complete pipeline run until completion."""
    # Mock get_unprocessed_count to return decreasing values
    count_values = [2, 0]  # First call: 2 unprocessed, second call: 0 (completion)
    processor_instance.get_unprocessed_count = Mock(side_effect=count_values)

    with patch("time.sleep"):
        processor_instance.run()

    # Should have checked count twice
    assert processor_instance.get_unprocessed_count.call_count == 2


def test_run_pipeline_batch_failure(processor_instance):
    """Test pipeline behavior when batch processing fails."""
    processor_instance.get_unprocessed_count = Mock(return_value=1)

    # Mock process_batch to fail in non-test environment
    original_env = processor_instance.environment
    processor_instance.environment = "prod"  # Non-test environment
    processor_instance.processor.validate_data.return_value = False

    try:
        with patch("time.sleep"):
            processor_instance.run()
    finally:
        processor_instance.environment = original_env


def test_environment_specific_behavior():
    """Test environment-specific behavior differences."""
    # Test dev environment
    dev_processor = BGGResponseProcessor(environment="dev")
    dev_processor.bq_client = Mock()
    dev_processor.processor = Mock()
    dev_processor.loader = Mock()

    # Mock empty responses
    dev_processor.get_unprocessed_responses = Mock(return_value=[])

    with patch("time.sleep"):
        result = dev_processor.process_batch()

    # Should return True in dev environment
    assert result is True

    # Test prod environment
    prod_processor = BGGResponseProcessor(environment="prod")
    prod_processor.bq_client = Mock()
    prod_processor.processor = Mock()
    prod_processor.loader = Mock()

    # Mock empty responses
    prod_processor.get_unprocessed_responses = Mock(return_value=[])

    with patch("time.sleep"):
        result = prod_processor.process_batch()

    # Should return False in prod environment with no responses
    assert result is False


def test_convert_dataframe_to_list_mock_handling(processor_instance):
    """Test _convert_dataframe_to_list with various mock types."""
    # Test with mock that has to_dict returning list
    mock_df = Mock()
    mock_df.to_dict.return_value = [
        {"game_id": 1, "response_data": "data1"},
        {"game_id": 2, "response_data": "data2"},
    ]

    result = processor_instance._convert_dataframe_to_list(mock_df)
    assert len(result) == 2
    assert result[0]["game_id"] == 1

    # Test with mock that has to_dict returning dict
    mock_df2 = Mock()
    mock_df2.to_dict.return_value = {"game_id": [1, 2], "response_data": ["data1", "data2"]}

    result2 = processor_instance._convert_dataframe_to_list(mock_df2)
    assert len(result2) == 2
    assert result2[0]["game_id"] == 1


def test_streaming_buffer_retry_logic(processor_instance, mock_bq_client):
    """Test retry logic for streaming buffer errors."""
    # Mock initial query success, then streaming buffer error, then success
    call_count = 0

    def query_side_effect(query_str):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            # First call: get unprocessed responses
            return processor_instance.bq_client.query.return_value
        elif call_count == 2:
            # Second call: streaming buffer error on update
            raise Exception("Streaming buffer error")
        else:
            # Subsequent calls: success
            result = Mock()
            result.result.return_value = None
            result.num_dml_affected_rows = 2
            return result

    mock_bq_client.query.side_effect = query_side_effect

    with patch("time.sleep") as mock_sleep:
        result = processor_instance.process_batch()

    # Should return False due to streaming buffer error
    assert result is False
    # Should have attempted the initial query and the failed update
    assert call_count >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
