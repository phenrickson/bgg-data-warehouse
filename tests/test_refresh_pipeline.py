"""Tests for the refresh pipeline."""

from datetime import datetime, timedelta
import pytest
from unittest.mock import Mock, patch

from src.pipeline.refresh_games import RefreshPipeline


@pytest.fixture
def mock_config():
    return {
        "base_interval_days": 7,
        "decay_factor": 2.0,
        "max_interval_days": 90,
    }


@pytest.fixture
def mock_bigquery_config():
    return {
        "project": {"id": "test-project", "dataset": "bgg_data_test"},
        "datasets": {"raw": "raw_test", "bgg_data": "bgg_data_test"},
        "raw_tables": {"thing_ids": {"name": "thing_ids"}},
    }


@pytest.fixture
def refresh_pipeline(mock_config, mock_bigquery_config):
    with (
        patch("src.pipeline.refresh_games.get_refresh_config", return_value=mock_config),
        patch("src.pipeline.base.get_bigquery_config", return_value=mock_bigquery_config),
        patch("src.pipeline.base.bigquery.Client"),
        patch("src.pipeline.refresh_games.BGGResponseFetcher") as mock_fetcher_class,
        patch("src.pipeline.refresh_games.BGGResponseProcessor") as mock_processor_class,
    ):
        # Create mock instances
        mock_fetcher = Mock()
        mock_processor = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        mock_processor_class.return_value = mock_processor

        pipeline = RefreshPipeline()
        pipeline.execute_query = Mock()

        # Ensure the mocked components are attached
        pipeline.fetcher = mock_fetcher
        pipeline.processor = mock_processor

        return pipeline


def test_get_refresh_batch(refresh_pipeline):
    """Test getting a batch of games due for refresh."""
    # Mock query results
    mock_results = [Mock(game_id=i) for i in range(1, 4)]
    refresh_pipeline.execute_query.return_value = mock_results

    # Get refresh batch
    result = refresh_pipeline.get_refresh_batch(batch_size=3)

    # Verify results
    assert result == [1, 2, 3]
    refresh_pipeline.execute_query.assert_called_once()
    call_args = refresh_pipeline.execute_query.call_args
    assert "next_refresh_due < CURRENT_TIMESTAMP()" in call_args[0][0]
    assert call_args[1]["params"]["batch_size"] == 3


def test_update_refresh_tracking(refresh_pipeline):
    """Test updating refresh tracking columns."""
    # Test with valid game IDs
    game_ids = [1, 2, 3]

    # Call the method under test
    refresh_pipeline.update_refresh_tracking(game_ids)

    # Verify query execution
    refresh_pipeline.execute_query.assert_called_once()
    call_args = refresh_pipeline.execute_query.call_args
    query = call_args[0][0]
    params = call_args[1]["params"]

    # Verify query content
    assert "UPDATE `test-project.raw_test.raw_responses`" in query
    assert "last_refresh_timestamp" in query
    assert "refresh_count" in query
    assert "next_refresh_due" in query
    assert "FROM `test-project.bgg_data_test.games`" in query

    # Verify parameters
    assert params["base_interval"] == 7
    assert params["decay_factor"] == 2.0
    assert params["max_interval"] == 90

    # Test with empty game IDs
    refresh_pipeline.execute_query.reset_mock()
    refresh_pipeline.update_refresh_tracking([])
    refresh_pipeline.execute_query.assert_not_called()


def test_execute_no_games(refresh_pipeline):
    """Test execute when no games need refresh."""
    # Mock empty refresh batch
    refresh_pipeline.get_refresh_batch = Mock(return_value=[])

    # Execute pipeline
    result = refresh_pipeline.execute()

    # Verify result
    assert result["status"] == "complete"
    assert result["games_refreshed"] == 0
    assert "No games need refresh" in result["message"]
    assert isinstance(result["duration_seconds"], float)

    # Verify no further processing
    refresh_pipeline.fetcher.fetch_batch.assert_not_called()
    refresh_pipeline.processor.run.assert_not_called()


def test_execute_with_games(refresh_pipeline):
    """Test execute with games to refresh."""
    # Mock refresh batch
    game_ids = [1, 2, 3]
    refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)

    # Mock update_refresh_tracking to avoid the query execution
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Execute pipeline
        result = refresh_pipeline.execute(batch_size=3)

        # Verify result
        assert result["status"] == "complete"
        assert result["games_refreshed"] == 3
        assert "Successfully refreshed 3 games" in result["message"]
        assert isinstance(result["duration_seconds"], float)

        # Verify processing steps
        refresh_pipeline.fetcher.fetch_batch.assert_called_once_with(game_ids)
        refresh_pipeline.processor.run.assert_called_once()
        refresh_pipeline.update_refresh_tracking.assert_called_once_with(game_ids)
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking


def test_execute_respects_batch_size(refresh_pipeline):
    """Test that execute respects the batch size parameter."""
    # Mock update_refresh_tracking to avoid the query execution
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Test with different batch sizes
        for batch_size in [1, 5, 10]:
            # Mock the refresh batch to return some game IDs
            game_ids = list(range(batch_size))
            refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)

            # Execute pipeline
            refresh_pipeline.execute(batch_size=batch_size)

            # Verify batch size was respected
            refresh_pipeline.get_refresh_batch.assert_called_once_with(batch_size)

            # Reset mocks for next iteration
            refresh_pipeline.get_refresh_batch.reset_mock()
            refresh_pipeline.fetcher.fetch_batch.reset_mock()
            refresh_pipeline.processor.run.reset_mock()
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking


def test_execute_handles_fetcher_error(refresh_pipeline):
    """Test execute handles fetcher errors gracefully."""
    # Mock refresh batch
    game_ids = [1, 2, 3]
    refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)

    # Mock update_refresh_tracking to avoid the query execution
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Mock fetcher error
        refresh_pipeline.fetcher.fetch_batch = Mock(side_effect=Exception("Fetcher Error"))

        # Execute should raise the error
        with pytest.raises(Exception) as exc:
            refresh_pipeline.execute()
        assert "Fetcher Error" in str(exc.value)

        # Verify fetcher was called but processor and update weren't
        refresh_pipeline.fetcher.fetch_batch.assert_called_once_with(game_ids)
        refresh_pipeline.processor.run.assert_not_called()
        refresh_pipeline.update_refresh_tracking.assert_not_called()
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking


def test_execute_handles_processor_error(refresh_pipeline):
    """Test execute handles processor errors gracefully."""
    # Mock refresh batch
    game_ids = [1, 2, 3]
    refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)

    # Mock update_refresh_tracking to avoid the query execution
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Mock processor error
        refresh_pipeline.processor.run = Mock(side_effect=Exception("Processor Error"))

        # Execute should raise the error
        with pytest.raises(Exception) as exc:
            refresh_pipeline.execute()
        assert "Processor Error" in str(exc.value)

        # Verify fetcher was called but update wasn't
        refresh_pipeline.fetcher.fetch_batch.assert_called_once_with(game_ids)
        refresh_pipeline.processor.run.assert_called_once()
        refresh_pipeline.update_refresh_tracking.assert_not_called()
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking


def test_max_games_limit(refresh_pipeline):
    """Test that max_games limit is respected in get_refresh_batch."""
    # Set max_games on the pipeline
    refresh_pipeline.max_games = 5

    # Mock query for processed games count
    mock_count_result = [Mock(count=3)]
    # Only return 2 games since remaining capacity is 2 (5 - 3 = 2)
    mock_games_result = [Mock(game_id=i) for i in range(1, 3)]  # Only 2 games
    refresh_pipeline.execute_query.side_effect = [
        mock_count_result,  # First call for processed count
        mock_games_result,  # Second call for games to refresh
    ]

    # Get refresh batch - should limit to remaining capacity (5 - 3 = 2)
    result = refresh_pipeline.get_refresh_batch(batch_size=10)

    # Should return exactly 2 games (remaining capacity)
    assert len(result) == 2
    assert result == [1, 2]

    # Verify both queries were called
    assert refresh_pipeline.execute_query.call_count == 2

    # Check that effective batch size was used in the second query
    second_call_params = refresh_pipeline.execute_query.call_args_list[1][1]["params"]
    assert second_call_params["batch_size"] == 2  # min(10, 2)


def test_max_games_capacity_exceeded(refresh_pipeline):
    """Test behavior when max_games capacity is already exceeded."""
    # Set max_games on the pipeline
    refresh_pipeline.max_games = 5

    # Mock query for processed games count - already at limit
    mock_count_result = [Mock(count=5)]
    refresh_pipeline.execute_query.return_value = mock_count_result

    # Get refresh batch - should return empty list
    result = refresh_pipeline.get_refresh_batch(batch_size=10)

    # Should return empty list
    assert result == []

    # Only the count query should have been called
    assert refresh_pipeline.execute_query.call_count == 1
