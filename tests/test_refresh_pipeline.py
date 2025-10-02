"""Tests for the refresh pipeline."""

from datetime import datetime, timedelta
import pytest
from unittest.mock import Mock, patch

from src.pipeline.refresh_data import RefreshPipeline


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
        "project": {"id": "test-project"},
        "datasets": {"raw": "raw", "bgg_data": "bgg_data"},
        "raw_tables": {"thing_ids": {"name": "thing_ids"}},
    }


@pytest.fixture
def refresh_pipeline(mock_config, mock_bigquery_config):
    with (
        patch("src.pipeline.refresh_data.get_refresh_config", return_value=mock_config),
        patch("src.pipeline.base.get_bigquery_config", return_value=mock_bigquery_config),
        patch("src.pipeline.base.bigquery.Client"),
        patch("src.pipeline.base.BGGAPIClient") as mock_api_client,
        patch("src.pipeline.base.BGGDataProcessor"),
        patch("src.pipeline.base.BigQueryLoader") as mock_loader,
    ):

        pipeline = RefreshPipeline()
        pipeline.execute_query = Mock()
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
    assert "UPDATE raw.raw_responses" in query
    assert "last_refresh_timestamp" in query
    assert "refresh_count" in query
    assert "next_refresh_due" in query

    # Verify parameters
    assert params["game_ids"] == game_ids
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
    refresh_pipeline.api_client.fetch_items.assert_not_called()


def test_execute_with_games(refresh_pipeline):
    """Test execute with games to refresh."""
    # Mock refresh batch
    game_ids = [1, 2, 3]
    refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Mock API responses
        mock_responses = [{"id": i, "data": f"game_{i}"} for i in game_ids]
        refresh_pipeline.api_client.fetch_items = Mock(return_value=mock_responses)

        # Execute pipeline
        result = refresh_pipeline.execute(batch_size=3)

        # Verify result
        assert result["status"] == "complete"
        assert result["games_refreshed"] == 3
        assert "Successfully refreshed 3 games" in result["message"]
        assert isinstance(result["duration_seconds"], float)

        # Verify processing
        refresh_pipeline.api_client.fetch_items.assert_called_once_with(game_ids)
        refresh_pipeline.loader.load_games.assert_called_once_with(mock_responses)
        refresh_pipeline.update_refresh_tracking.assert_called_once_with(game_ids)
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking


def test_execute_respects_batch_size(refresh_pipeline):
    """Test that execute respects the batch size parameter."""
    # Test with different batch sizes
    for batch_size in [1, 5, 10]:
        # Mock the refresh batch to return some game IDs
        game_ids = list(range(batch_size))
        refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)

        # Mock API responses
        mock_responses = [{"id": i, "data": f"game_{i}"} for i in game_ids]
        refresh_pipeline.api_client.fetch_items = Mock(return_value=mock_responses)

        # Execute pipeline
        refresh_pipeline.execute(batch_size=batch_size)

        # Verify batch size was respected
        refresh_pipeline.get_refresh_batch.assert_called_once_with(batch_size)

        # Reset mocks for next iteration
        refresh_pipeline.get_refresh_batch.reset_mock()
        refresh_pipeline.api_client.fetch_items.reset_mock()


def test_execute_handles_api_error(refresh_pipeline):
    """Test execute handles API errors gracefully."""
    # Mock refresh batch
    game_ids = [1, 2, 3]
    refresh_pipeline.get_refresh_batch = Mock(return_value=game_ids)
    original_update_tracking = refresh_pipeline.update_refresh_tracking
    refresh_pipeline.update_refresh_tracking = Mock()

    try:
        # Mock API error
        refresh_pipeline.api_client.fetch_items = Mock(side_effect=Exception("API Error"))

        # Execute should raise the error
        with pytest.raises(Exception) as exc:
            refresh_pipeline.execute()
        assert "API Error" in str(exc.value)

        # Verify no updates were made
        refresh_pipeline.loader.load_games.assert_not_called()
        refresh_pipeline.update_refresh_tracking.assert_not_called()
    finally:
        # Restore original method
        refresh_pipeline.update_refresh_tracking = original_update_tracking
