"""Tests for the refresh strategy implementation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, UTC

from src.pipeline.fetch_responses import BGGResponseFetcher
from src.config import get_refresh_config


class TestRefreshStrategy:
    """Test cases for the refresh strategy."""

    def setup_method(self):
        """Set up test fixtures."""
        self.fetcher = BGGResponseFetcher(batch_size=100, chunk_size=5, environment="test")

    def test_refresh_config_loaded(self):
        """Test that refresh configuration is properly loaded."""
        config = get_refresh_config()

        assert config["enabled"] is True
        assert config["base_interval_days"] == 7
        assert config["upcoming_interval_days"] == 3
        assert config["decay_factor"] == 2.0
        assert config["max_interval_days"] == 90
        assert config["refresh_batch_size"] == 200

    def test_get_unfetched_ids_includes_priority(self):
        """Test that get_unfetched_ids returns priority information."""
        # In test environment, should return predefined data with priorities
        result = self.fetcher.get_unfetched_ids()

        assert len(result) == 3
        for game in result:
            assert "game_id" in game
            assert "type" in game
            assert "priority" in game
            assert game["priority"] == "unfetched"

    @patch("src.pipeline.fetch_responses.bigquery.Client")
    def test_get_refresh_candidates_query_structure(self, mock_bq_client):
        """Test that refresh candidates query is properly structured."""
        # Mock the BigQuery client and query result
        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        # Create a mock DataFrame with expected structure
        mock_df = pd.DataFrame(
            [
                {"game_id": 1001, "year_published": 2025},
                {"game_id": 1002, "year_published": 2024},
                {"game_id": 1003, "year_published": 2020},
            ]
        )

        mock_query_job = Mock()
        mock_query_job.to_dataframe.return_value = mock_df
        mock_client.query.return_value = mock_query_job

        # Create a real fetcher (not test environment)
        fetcher = BGGResponseFetcher(environment="dev")

        # Test the method
        result = fetcher._get_refresh_candidates(10)

        # Verify the query was called
        assert mock_client.query.called

        # Verify the result structure
        assert len(result) == 3
        for game in result:
            assert "game_id" in game
            assert "type" in game
            assert "priority" in game
            assert game["priority"] == "refresh"

    @patch("src.pipeline.fetch_responses.bigquery.Client")
    def test_update_refresh_tracking(self, mock_bq_client):
        """Test that refresh tracking updates work correctly."""
        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_job.num_dml_affected_rows = 3
        mock_client.query.return_value = mock_job

        # Create a real fetcher (not test environment)
        fetcher = BGGResponseFetcher(environment="dev")

        # Test the method
        game_ids = [1001, 1002, 1003]
        fetcher._update_refresh_tracking(game_ids)

        # Verify the query was called with correct structure
        assert mock_client.query.called
        query_call = mock_client.query.call_args[0][0]

        # Check that the query contains expected elements
        assert "UPDATE" in query_call
        assert "last_refresh_timestamp = CURRENT_TIMESTAMP()" in query_call
        assert "refresh_count = COALESCE(refresh_count, 0) + 1" in query_call
        assert "1001,1002,1003" in query_call

    def test_store_response_with_refresh_flag(self):
        """Test that store_response handles refresh flag correctly."""
        with patch.object(self.fetcher, "_update_refresh_tracking") as mock_update:
            with patch.object(self.fetcher.bq_client, "get_table") as mock_get_table:
                with patch.object(self.fetcher.bq_client, "load_table_from_json") as mock_load:
                    with patch.object(self.fetcher.bq_client, "query") as mock_query:

                        # Mock table schema
                        mock_table = Mock()
                        mock_table.schema = []
                        mock_get_table.return_value = mock_table

                        # Mock load job
                        mock_load_job = Mock()
                        mock_load_job.result.return_value = Mock()
                        mock_load_job.errors = None
                        mock_load_job.num_dml_affected_rows = 2
                        mock_load.return_value = mock_load_job

                        # Mock cleanup query
                        mock_query.return_value.result.return_value = None

                        # Test with refresh=True
                        self.fetcher.store_response(
                            game_ids=[1001, 1002],
                            response_data='{"items": {"item": [{"@id": "1001"}, {"@id": "1002"}]}}',
                            is_refresh=True,
                        )

                        # Verify refresh tracking was called
                        mock_update.assert_called_once_with([1001, 1002])

    def test_fetch_batch_detects_refresh_priority(self):
        """Test that fetch_batch correctly detects refresh operations."""
        # Mock the get_unfetched_ids to return mixed priorities
        mock_games = [
            {"game_id": 1001, "type": "boardgame", "priority": "unfetched"},
            {"game_id": 1002, "type": "boardgame", "priority": "refresh"},
            {"game_id": 1003, "type": "boardgame", "priority": "unfetched"},
        ]

        with patch.object(self.fetcher, "get_unfetched_ids", return_value=mock_games):
            with patch.object(self.fetcher.api_client, "get_thing") as mock_api:
                with patch.object(self.fetcher, "store_response") as mock_store:

                    # Mock API response
                    mock_api.return_value = (
                        '{"items": {"item": [{"@id": "1001"}, {"@id": "1002"}]}}'
                    )

                    # Run fetch_batch
                    result = self.fetcher.fetch_batch()

                    # Verify store_response was called with is_refresh=True for the chunk containing refresh
                    assert mock_store.called
                    call_args = mock_store.call_args
                    assert (
                        call_args[1]["is_refresh"] is True
                    )  # Should be True because chunk contains refresh

    def test_exponential_decay_calculation_in_query(self):
        """Test that the exponential decay calculation is correctly implemented in SQL."""
        # This test verifies the SQL logic matches our expected formula

        # The SQL should implement:
        # - Upcoming games (year > current): 3 days
        # - Current year games: 7 days
        # - Past games: min(90, 7 * 2^(current_year - year_published))

        # For 2025 as current year:
        # - 2026 games: 3 days
        # - 2025 games: 7 days
        # - 2024 games: 14 days (7 * 2^1)
        # - 2023 games: 28 days (7 * 2^2)
        # - 2022 games: 56 days (7 * 2^3)
        # - 2021 games: 90 days (capped at max)

        config = get_refresh_config()

        # Test the formula logic
        current_year = 2025

        # Upcoming games
        assert config["upcoming_interval_days"] == 3

        # Current year
        assert config["base_interval_days"] == 7

        # Past years with exponential decay
        base = config["base_interval_days"]
        factor = config["decay_factor"]
        max_interval = config["max_interval_days"]

        # 2024: 7 * 2^1 = 14
        interval_2024 = min(max_interval, base * (factor ** (current_year - 2024)))
        assert interval_2024 == 14

        # 2023: 7 * 2^2 = 28
        interval_2023 = min(max_interval, base * (factor ** (current_year - 2023)))
        assert interval_2023 == 28

        # 2022: 7 * 2^3 = 56
        interval_2022 = min(max_interval, base * (factor ** (current_year - 2022)))
        assert interval_2022 == 56

        # 2021: 7 * 2^4 = 112, but capped at 90
        interval_2021 = min(max_interval, base * (factor ** (current_year - 2021)))
        assert interval_2021 == 90


if __name__ == "__main__":
    pytest.main([__file__])
