"""
Integration tests for the BGG Data Warehouse pipeline.

This test suite verifies the end-to-end data loading process,
ensuring that the entire pipeline works as expected from ID fetching
to data warehouse loading.
"""

import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import pandas as pd

from src.id_fetcher.fetcher import BGGIDFetcher
from src.api_client.client import BGGAPIClient
from src.pipeline.fetch_responses import BGGResponseFetcher
from src.pipeline.process_responses import BGGResponseProcessor
from src.quality_monitor.monitor import DataQualityMonitor


class TestDataWarehouseIntegration:
    @pytest.fixture
    def test_config(self):
        """
        Provide a test configuration with controlled parameters.
        """
        return {
            "environment": "test",
            "max_games_to_fetch": 50,
            "batch_size": 10,
            "chunk_size": 5,
            "game_type": "boardgame",
            "project": {"id": "test-project", "dataset": "test_data"},
            "datasets": {"raw": "test_raw", "transformed": "test_transformed"},
            "raw_tables": {
                "thing_ids": {"name": "test_thing_ids"},
                "raw_responses": {"name": "test_raw_responses"},
            },
            "tables": {"games": {"name": "games"}},
        }

    @pytest.fixture
    def mock_game_ids(self):
        """Generate mock game IDs for testing."""
        return list(range(1, 51))  # 50 mock game IDs

    @pytest.fixture
    def mock_game_responses(self, mock_game_ids):
        """Generate mock game responses."""
        return {
            game_id: {
                "items": {
                    "item": {
                        "@id": str(game_id),
                        "name": {"@value": f"Game {game_id}"},
                        "yearpublished": {"@value": str(2000 + (game_id % 20))},
                        "minplayers": {"@value": str(1 + (game_id % 4))},
                        "maxplayers": {"@value": str(2 + (game_id % 6))},
                    }
                }
            }
            for game_id in mock_game_ids
        }

    def test_id_fetching(self, test_config, mock_game_ids):
        """Test game ID fetching mechanism."""
        with patch.object(BGGIDFetcher, "download_ids") as mock_download:
            # Simulate ID download
            mock_download.return_value = Mock(spec=os.PathLike)

            with patch.object(BGGIDFetcher, "parse_ids") as mock_parse:
                # Return mock game IDs - parse_ids returns list of dicts
                mock_parse.return_value = [
                    {"game_id": game_id, "type": "boardgame"}
                    for game_id in mock_game_ids[: test_config["max_games_to_fetch"]]
                ]

                id_fetcher = BGGIDFetcher()
                game_ids = id_fetcher.fetch_game_ids(
                    {
                        "max_games_to_fetch": test_config["max_games_to_fetch"],
                        "game_type": test_config.get("game_type", "boardgame"),
                    }
                )

                # fetch_game_ids returns list of integers
                assert len(game_ids) > 0, "No game IDs retrieved"
                assert (
                    len(game_ids) <= test_config["max_games_to_fetch"]
                ), "Exceeded max games to fetch"
                assert all(
                    isinstance(game_id, int) for game_id in game_ids
                ), "Game IDs should be integers"

    def test_response_fetching(self, test_config, mock_game_ids, mock_game_responses):
        """Test game response fetching mechanism."""
        with patch.object(BGGResponseFetcher, "get_unfetched_ids") as mock_unfetched:
            # get_unfetched_ids returns list of dicts with priority field
            mock_unfetched.return_value = [
                {"game_id": game_id, "type": "boardgame", "priority": "unfetched"}
                for game_id in mock_game_ids[: test_config["batch_size"]]
            ]

            with patch.object(BGGAPIClient, "get_thing") as mock_get_thing:
                # Simulate API responses
                mock_get_thing.return_value = {
                    "items": {
                        "item": [
                            {"@id": str(game_id)}
                            for game_id in mock_game_ids[: test_config["batch_size"]]
                        ]
                    }
                }

                response_fetcher = BGGResponseFetcher(
                    batch_size=test_config["batch_size"],
                    chunk_size=test_config["chunk_size"],
                    environment="testing",
                )

                with patch.object(response_fetcher, "store_response") as mock_store:
                    fetch_success = response_fetcher.fetch_batch()

                    assert fetch_success, "Failed to fetch game responses"
                    assert mock_store.call_count > 0, "No responses stored"

    def test_response_processing(self, test_config, mock_game_ids, mock_game_responses):
        """Test game response processing mechanism."""
        response_processor = BGGResponseProcessor(config=test_config, environment="test")

        with patch.object(response_processor, "get_unprocessed_responses") as mock_get_responses:
            # Mock unprocessed responses
            mock_get_responses.return_value = [
                {
                    "game_id": game_id,
                    "response_data": mock_game_responses[game_id],
                    "fetch_timestamp": datetime.now(),
                }
                for game_id in mock_game_ids[:5]  # Process 5 games
            ]

            with (
                patch.object(response_processor.processor, "process_game") as mock_process_game,
                patch.object(response_processor.processor, "prepare_for_bigquery") as mock_prepare,
                patch.object(response_processor.processor, "validate_data") as mock_validate,
            ):

                # Mock successful game processing - return different games for each call
                mock_process_game.side_effect = [
                    {
                        "game_id": game_id,
                        "primary_name": f"Test Game {game_id}",
                        "year_published": 2020,
                        "min_players": 2,
                        "max_players": 4,
                    }
                    for game_id in mock_game_ids[:5]
                ]

                # Mock data preparation and validation
                mock_prepare.return_value = {"games": []}
                mock_validate.return_value = True

                with patch.object(response_processor.loader, "load_games") as mock_load:
                    with patch.object(response_processor.bq_client, "query") as mock_query:
                        # Mock the update query
                        mock_job = Mock()
                        mock_job.result.return_value = None
                        mock_job.num_dml_affected_rows = 5
                        mock_query.return_value = mock_job

                        # Process batch
                        success = response_processor.process_batch()

                        assert success, "Processing batch failed"
                        assert mock_process_game.call_count > 0, "No games processed"
                        assert mock_load.call_count > 0, "No games loaded"

    def test_data_quality_monitoring(self, test_config):
        """Test data quality monitoring mechanism."""
        quality_monitor = DataQualityMonitor(config=test_config)

        with patch.object(quality_monitor, "check_completeness") as mock_completeness:
            with patch.object(quality_monitor, "check_freshness") as mock_freshness:
                with patch.object(quality_monitor, "check_validity") as mock_validity:

                    # Mock all checks to pass
                    mock_completeness.return_value = True
                    mock_freshness.return_value = True
                    mock_validity.return_value = True

                    # Run all quality checks
                    quality_results = quality_monitor.run_all_checks()

                    # Verify the actual return structure
                    expected_checks = [
                        "games_completeness",
                        "responses_completeness",
                        "games_freshness",
                        "responses_freshness",
                        "games_validity",
                    ]

                    for check in expected_checks:
                        assert check in quality_results, f"Missing check: {check}"
                        assert quality_results[check] is True, f"Check {check} failed"

                    # Verify all checks passed
                    assert all(quality_results.values()), "Some quality checks failed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
