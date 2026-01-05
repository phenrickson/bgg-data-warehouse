"""Tests for ResponseRefresher module."""

import logging
import os
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import get_bigquery_config
from src.modules.response_refresher import ResponseRefresher

# Load environment variables from .env file
load_dotenv(override=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_credentials():
    """Check if BigQuery credentials exist."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")

    creds_file = Path(creds_path)
    if not creds_file.exists():
        pytest.skip(f"Credentials file not found: {creds_path}")


@pytest.fixture
def mock_config():
    """Get mock configuration."""
    return {
        "project": {"id": "test-project", "dataset": "test_dataset", "location": "US"},
        "datasets": {"raw": "test_raw"},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "fetch_in_progress": {"name": "fetch_in_progress"},
            "thing_ids": {"name": "thing_ids"},
            "fetched_responses": {"name": "fetched_responses"},
            "processed_responses": {"name": "processed_responses"},
        },
        "tables": {"games": {"name": "games"}},
        "refresh_policy": {
            "batch_size": 1000,
            "intervals": [
                {"name": "recent", "min_age_years": 0, "max_age_years": 2, "refresh_days": 7},
                {"name": "established", "min_age_years": 2, "max_age_years": 5, "refresh_days": 30},
                {"name": "classic", "min_age_years": 5, "max_age_years": 10, "refresh_days": 90},
                {"name": "vintage", "min_age_years": 10, "max_age_years": None, "refresh_days": 180},
            ],
        },
    }


@pytest.fixture
def mock_bq_client():
    """Create mock BigQuery client."""
    mock_client = Mock(spec=bigquery.Client)

    # Create mock games DataFrame
    games_df = pd.DataFrame([
        {"game_id": 13, "year_published": 1995, "last_fetch_timestamp": None, "refresh_category": "vintage"},
        {"game_id": 174430, "year_published": 2017, "last_fetch_timestamp": None, "refresh_category": "recent"},
        {"game_id": 224517, "year_published": 2018, "last_fetch_timestamp": None, "refresh_category": "recent"},
    ])

    # Mock query job
    def mock_query(query):
        mock_job = Mock()
        if "SELECT COUNT" in query:
            result = Mock()
            result.count = 3
            mock_job.result.return_value = iter([result])
        elif "DELETE FROM" in query or "INSERT INTO" in query:
            mock_job.result.return_value = iter([])
        else:
            mock_job.to_dataframe.return_value = games_df
            mock_job.result.return_value = iter([])
        return mock_job

    mock_client.query = mock_query
    mock_client.insert_rows_json.return_value = []

    return mock_client


class TestResponseRefresherUnit:
    """Unit tests for ResponseRefresher class."""

    def test_refresher_initialization(self, mock_config):
        """Test that ResponseRefresher initializes correctly."""
        with patch("src.modules.response_refresher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_refresher.bigquery.Client"):
                with patch("src.modules.response_refresher.ResponseFetcher"):
                    refresher = ResponseRefresher(
                        chunk_size=5,
                        dry_run=True,
                    )
                    assert refresher.chunk_size == 5
                    assert refresher.dry_run is True
                    assert refresher.batch_size == 1000
                    logger.info("ResponseRefresher initialization test passed")

    def test_refresh_intervals_loaded(self, mock_config):
        """Test that refresh intervals are loaded from config."""
        with patch("src.modules.response_refresher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_refresher.bigquery.Client"):
                with patch("src.modules.response_refresher.ResponseFetcher"):
                    refresher = ResponseRefresher(
                        chunk_size=5,
                    )
                    assert len(refresher.refresh_intervals) == 4

                    interval_names = [i["name"] for i in refresher.refresh_intervals]
                    assert "recent" in interval_names
                    assert "established" in interval_names
                    assert "classic" in interval_names
                    assert "vintage" in interval_names
                    logger.info("Refresh intervals loaded correctly")

    def test_dry_run_mode(self, mock_config, mock_bq_client):
        """Test that dry run mode doesn't make changes."""
        with patch("src.modules.response_refresher.get_bigquery_config", return_value=mock_config):
            with patch("src.modules.response_refresher.bigquery.Client", return_value=mock_bq_client):
                with patch("src.modules.response_refresher.ResponseFetcher"):
                    refresher = ResponseRefresher(
                        chunk_size=5,
                        dry_run=True,
                    )
                    refresher.bq_client = mock_bq_client

                    # Get games to refresh in dry run mode
                    games = refresher.get_games_to_refresh()

                    # Should return games but not actually modify anything
                    assert isinstance(games, list)
                    logger.info("Dry run mode test passed")


class TestResponseRefresherIntegration:
    """Integration tests for ResponseRefresher (require credentials)."""

    @pytest.fixture
    def config(self):
        """Get BigQuery configuration."""
        check_credentials()
        return get_bigquery_config()

    @pytest.fixture
    def bigquery_client(self, config):
        """Create BigQuery client."""
        check_credentials()
        return bigquery.Client(project=config["project"]["id"])

    @pytest.fixture
    def refresher(self):
        """Create ResponseRefresher instance for testing."""
        check_credentials()
        return ResponseRefresher(chunk_size=5, dry_run=True)

    def test_config_loading(self, refresher):
        """Test that refresh policy configuration loads correctly."""
        assert refresher.batch_size == 1000, "Batch size should be 1000"
        assert len(refresher.refresh_intervals) == 4, "Should have 4 refresh intervals"

        # Verify interval configuration
        interval_names = [i["name"] for i in refresher.refresh_intervals]
        assert "recent" in interval_names
        assert "established" in interval_names
        assert "classic" in interval_names
        assert "vintage" in interval_names

        logger.info(f"Config loaded: {len(refresher.refresh_intervals)} refresh intervals")

    def test_games_table_exists(self, bigquery_client, config):
        """Test that the games table exists."""
        project_id = config["project"]["id"]
        games_table = f"{project_id}.core.games"

        try:
            table = bigquery_client.get_table(games_table)
            assert table is not None
            logger.info(f"Games table exists: {games_table}")

            # Get count of games
            count_query = f"SELECT COUNT(DISTINCT game_id) as count FROM `{games_table}`"
            result = bigquery_client.query(count_query).result()
            count = next(result).count
            logger.info(f"  Found {count} unique games in database")

        except Exception as e:
            pytest.fail(f"Games table not found or inaccessible: {e}")

    def test_get_games_to_refresh_query(self, refresher):
        """Test the query logic for finding games to refresh (without actually fetching)."""
        try:
            # This will execute the query to find games that need refresh
            games_to_refresh = refresher.get_games_to_refresh()

            # Verify structure
            assert isinstance(games_to_refresh, list), "Should return a list"

            if len(games_to_refresh) > 0:
                # Verify structure of returned games
                first_game = games_to_refresh[0]
                assert "game_id" in first_game, "Should have game_id"
                assert "year_published" in first_game, "Should have year_published"
                assert "refresh_category" in first_game, "Should have refresh_category"

                logger.info(f"Query returned {len(games_to_refresh)} games to refresh")

                # Log breakdown by category
                categories = {}
                for game in games_to_refresh:
                    cat = game["refresh_category"]
                    categories[cat] = categories.get(cat, 0) + 1

                for category, count in categories.items():
                    logger.info(f"  - {category}: {count} games")
            else:
                logger.info("Query executed successfully (no games need refresh yet)")

        except Exception as e:
            pytest.fail(f"Failed to query games for refresh: {e}")

    def test_dry_run_no_api_calls(self, refresher):
        """Test that we can instantiate and configure refresher without making API calls."""
        assert refresher.chunk_size == 5
        assert refresher.dry_run is True
        assert refresher.api_client is not None
        assert refresher.bq_client is not None

        logger.info("Refresher instantiated without errors")
        logger.info(f"  Chunk size: {refresher.chunk_size}")
        logger.info(f"  Batch size: {refresher.batch_size}")

    @pytest.mark.integration
    def test_full_refresh_integration(self, config):
        """
        INTEGRATION TEST - This will actually fetch and process games!
        Only run this if you want to test the full pipeline.

        Run with: pytest tests/test_refresh_games.py::TestResponseRefresherIntegration::test_full_refresh_integration -v -m integration
        """
        check_credentials()

        logger.warning("=" * 60)
        logger.warning("INTEGRATION TEST - Will fetch real data from BGG API")
        logger.warning("=" * 60)

        try:
            refresher = ResponseRefresher(
                chunk_size=5,
                dry_run=False,  # Actually run
            )
            # Limit to small batch for testing
            refresher.batch_size = 10

            # Run the refresh
            result = refresher.run()

            if result:
                logger.info("Refresh completed successfully")
            else:
                logger.info("Refresh ran but found no games to refresh")

        except Exception as e:
            pytest.fail(f"Integration test failed: {e}")


if __name__ == "__main__":
    # Run unit tests only (skip integration tests)
    pytest.main([__file__, "-v", "-m", "not integration"])
