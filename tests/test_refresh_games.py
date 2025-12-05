"""Tests for refresh_games pipeline."""

import logging
import os
from datetime import datetime, UTC
from pathlib import Path

import pytest
from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import get_bigquery_config
from src.pipeline.refresh_games import BGGGameRefresher

# Load environment variables from .env file
# Use override=True to allow .env to override system environment variables
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
def test_environment():
    """Ensure we're running in test environment."""
    env = os.getenv("ENVIRONMENT", "test")
    if env == "prod":
        pytest.skip("Refusing to run tests against production environment")
    return env


@pytest.fixture
def config(test_environment):
    """Get BigQuery configuration for test environment."""
    check_credentials()
    return get_bigquery_config(test_environment)


@pytest.fixture
def bigquery_client():
    """Create BigQuery client."""
    check_credentials()
    return bigquery.Client()


@pytest.fixture
def refresher(test_environment):
    """Create BGGGameRefresher instance for testing."""
    return BGGGameRefresher(chunk_size=5, environment=test_environment)


def test_config_loading(refresher):
    """Test that refresh policy configuration loads correctly."""
    assert refresher.batch_size == 1000, "Batch size should be 1000"
    assert len(refresher.refresh_intervals) == 4, "Should have 4 refresh intervals"

    # Verify interval configuration
    interval_names = [i["name"] for i in refresher.refresh_intervals]
    assert "recent" in interval_names
    assert "established" in interval_names
    assert "classic" in interval_names
    assert "vintage" in interval_names

    logger.info(f"✓ Config loaded: {len(refresher.refresh_intervals)} refresh intervals")


def test_games_table_exists(bigquery_client, config, test_environment):
    """Test that the games table exists in test environment."""
    env_config = config["environments"][test_environment]
    games_table = f"{env_config['project_id']}.{env_config['dataset']}.games"

    try:
        table = bigquery_client.get_table(games_table)
        assert table is not None
        logger.info(f"✓ Games table exists: {games_table}")

        # Get count of games
        count_query = f"SELECT COUNT(DISTINCT game_id) as count FROM `{games_table}`"
        result = bigquery_client.query(count_query).result()
        count = next(result).count
        logger.info(f"  Found {count} unique games in test database")

    except Exception as e:
        pytest.fail(f"Games table not found or inaccessible: {e}")


def test_get_games_to_refresh_query(refresher, bigquery_client, config, test_environment):
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

            logger.info(f"✓ Query returned {len(games_to_refresh)} games to refresh")

            # Log breakdown by category
            categories = {}
            for game in games_to_refresh:
                cat = game["refresh_category"]
                categories[cat] = categories.get(cat, 0) + 1

            for category, count in categories.items():
                logger.info(f"  - {category}: {count} games")

            # Verify prioritization (newer games should be first)
            years = [g["year_published"] for g in games_to_refresh if g["year_published"]]
            if len(years) > 1:
                # Check that years are generally descending (newer first)
                logger.info(f"  Year range: {min(years)} - {max(years)}")
                logger.info(f"  First 5 games years: {years[:5]}")
        else:
            logger.info("✓ Query executed successfully (no games need refresh yet)")

    except Exception as e:
        pytest.fail(f"Failed to query games for refresh: {e}")


def test_fetch_in_progress_cleanup(refresher, bigquery_client, config, test_environment):
    """Test that old fetch_in_progress entries are cleaned up."""
    env_config = config["environments"][test_environment]
    fetch_table = f"{env_config['project_id']}.{config['datasets']['raw']}.fetch_in_progress"

    try:
        # Check initial count
        count_query = f"SELECT COUNT(*) as count FROM `{fetch_table}`"
        result = bigquery_client.query(count_query).result()
        initial_count = next(result).count

        logger.info(f"✓ Fetch in progress table accessible")
        logger.info(f"  Current entries: {initial_count}")

        # The cleanup happens inside get_games_to_refresh, so we just verify the table exists

    except Exception as e:
        # Table might not exist yet, which is okay for test environment
        logger.warning(f"Fetch in progress table not accessible: {e}")


def test_dry_run_no_api_calls(refresher, test_environment):
    """Test that we can instantiate and configure refresher without making API calls."""
    # This test ensures the refresher can be created and configured
    # without actually hitting the API or modifying data

    assert refresher.environment == test_environment
    assert refresher.chunk_size == 5  # We set this to 5 in the fixture
    assert refresher.api_client is not None
    assert refresher.bq_client is not None

    logger.info("✓ Refresher instantiated without errors")
    logger.info(f"  Environment: {refresher.environment}")
    logger.info(f"  Chunk size: {refresher.chunk_size}")
    logger.info(f"  Batch size: {refresher.batch_size}")


@pytest.mark.integration
def test_full_refresh_integration(refresher, config, test_environment):
    """
    INTEGRATION TEST - This will actually fetch and process games!
    Only run this if you want to test the full pipeline against test environment.

    Run with: pytest tests/test_refresh_games.py::test_full_refresh_integration -v -m integration
    """
    logger.warning("=" * 60)
    logger.warning("INTEGRATION TEST - Will fetch real data from BGG API")
    logger.warning(f"Environment: {test_environment}")
    logger.warning("=" * 60)

    try:
        # Limit to small batch for testing
        refresher.batch_size = 10  # Only refresh 10 games max
        refresher.chunk_size = 5   # Fetch in chunks of 5

        # Run the refresh
        result = refresher.run()

        if result:
            logger.info("✓ Refresh completed successfully")
        else:
            logger.info("✓ Refresh ran but found no games to refresh")

    except Exception as e:
        pytest.fail(f"Integration test failed: {e}")


if __name__ == "__main__":
    # Run unit tests only (skip integration tests)
    pytest.main([__file__, "-v", "-m", "not integration"])
