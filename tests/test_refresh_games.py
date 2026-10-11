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
from src.modules.response_refresher import (
    ResponseRefresher,
    _bucket_case_sql,
    _year_bounds,
)

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


# Mirrors config/bigquery.yaml: half-open [min_age_years, max_age_years).
INTERVALS = [
    {"name": "future", "max_age_years": 0, "refresh_days": 14},
    {"name": "recent", "min_age_years": 0, "max_age_years": 3, "refresh_days": 14},
    {"name": "established", "min_age_years": 3, "max_age_years": 6, "refresh_days": 60},
    {"name": "classic", "min_age_years": 6, "max_age_years": 11, "refresh_days": 180},
    {"name": "vintage", "min_age_years": 11, "refresh_days": 365},
    {"name": "unpublished", "year_published_null": True, "refresh_days": 60},
]


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
            "intervals": INTERVALS,
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
        if "COUNT(*)" in query:
            mock_job.result.return_value = iter([
                Mock(refresh_category="vintage", games=1),
                Mock(refresh_category="classic", games=2),
            ])
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
                    assert refresher.refresh_intervals == INTERVALS

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


class TestBuckets:
    """Bucket assignment by year_published (current year 2026)."""

    def test_year_bounds_are_half_open_ages(self):
        bounds = {i["name"]: _year_bounds(i, 2026) for i in INTERVALS if "year_published_null" not in i}
        assert bounds == {
            "future": (2027, None),
            "recent": (2024, 2026),
            "established": (2021, 2023),
            "classic": (2016, 2020),
            "vintage": (None, 2015),
        }

    @pytest.mark.parametrize("year, bucket", [
        (2030, "future"), (2027, "future"),
        (2026, "recent"), (2024, "recent"),
        (2023, "established"), (2021, "established"),
        (2020, "classic"), (2016, "classic"),
        (2015, "vintage"), (-3000, "vintage"),
        (None, "unpublished"),
    ])
    def test_every_year_lands_in_one_bucket(self, year, bucket):
        def matches(interval):
            if interval.get("year_published_null"):
                return year is None
            first, last = _year_bounds(interval, 2026)
            return year is not None and (first is None or year >= first) and (last is None or year <= last)

        assert [i["name"] for i in INTERVALS if matches(i)] == [bucket]

    def test_case_sql_follows_config_order(self):
        sql = _bucket_case_sql(INTERVALS, 2026)
        whens = [line.strip() for line in sql.splitlines() if "THEN '" in line]
        assert whens == [
            "WHEN year_published >= 2027 THEN 'future'",
            "WHEN year_published BETWEEN 2024 AND 2026 THEN 'recent'",
            "WHEN year_published BETWEEN 2021 AND 2023 THEN 'established'",
            "WHEN year_published BETWEEN 2016 AND 2020 THEN 'classic'",
            "WHEN year_published <= 2015 THEN 'vintage'",
            "WHEN year_published IS NULL THEN 'unpublished'",
        ]
        assert "WHEN year_published BETWEEN 2021 AND 2023 THEN 60" in sql


class TestSelection:
    """The selection and count queries the refresher sends."""

    @pytest.fixture
    def refresher(self, mock_config, mock_bq_client):
        with patch("src.modules.response_refresher.get_bigquery_config", return_value=mock_config),              patch("src.modules.response_refresher.bigquery.Client"),              patch("src.modules.response_refresher.ResponseFetcher"):
            r = ResponseRefresher(chunk_size=5, dry_run=True)
        r.bq_client = Mock(wraps=mock_bq_client)
        return r

    def _queries(self, refresher):
        return [c.args[0] for c in refresher.bq_client.query.call_args_list]

    def test_selection_is_one_scan_ordered_by_overdue_ratio(self, refresher):
        refresher.get_games_to_refresh()
        (select,) = [q for q in self._queries(refresher) if "ORDER BY" in q]
        assert "UNION ALL" not in select
        assert select.count("core.games") == 1
        order_by = select[select.index("ORDER BY"):]
        assert order_by.index("last_fetch_timestamp IS NULL DESC") < order_by.index("overdue_ratio DESC")
        assert "year_published DESC" not in select

    def test_counts_are_grouped_by_bucket(self, refresher):
        counts = refresher.count_games_needing_refresh()
        (count,) = self._queries(refresher)
        assert "GROUP BY refresh_category" in count
        assert counts == {"vintage": 1, "classic": 2, "total": 3}


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
        assert refresher.batch_size == 5000
        assert [
            {k: v for k, v in i.items() if k != "description"}
            for i in refresher.refresh_intervals
        ] == INTERVALS

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
