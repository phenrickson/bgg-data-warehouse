"""Shared test configuration and fixtures."""

import os
from pathlib import Path
from unittest import mock

import pytest
from google.cloud import bigquery

@pytest.fixture(autouse=True)
def mock_env():
    """Mock environment variables."""
    with mock.patch.dict(os.environ, {
        "GCP_PROJECT_ID": "test-project",
        "GCS_BUCKET": "test-bucket",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json"
    }):
        yield

@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    client = mock.Mock(spec=bigquery.Client)
    client.project = "test-project"
    return client

@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary directory for test data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def sample_config():
    """Create a sample configuration dictionary."""
    return {
        "project": {
            "id": "test-project",
            "location": "US"
        },
        "datasets": {
            "raw": "test_raw",
            "transformed": "test_transformed",
            "reporting": "test_reporting",
            "monitoring": "test_monitoring"
        },
        "tables": {
            "raw": {
                "games": "games",
                "request_log": "request_log",
                "thing_ids": "thing_ids"
            },
            "transformed": {
                "dim_games": "dim_games",
                "dim_categories": "dim_categories",
                "dim_mechanics": "dim_mechanics",
                "fact_ratings": "fact_ratings",
                "fact_weights": "fact_weights"
            },
            "reporting": {
                "game_metrics": "game_metrics",
                "trending": "trending",
                "rankings": "rankings"
            }
        },
        "storage": {
            "bucket": "test-bucket",
            "temp_prefix": "tmp/",
            "archive_prefix": "archive/"
        },
        "loading": {
            "batch_size": 100,
            "max_bad_records": 0,
            "write_disposition": "WRITE_APPEND"
        },
        "monitoring": {
            "freshness_threshold_hours": 24,
            "quality_check_schedule": "0 */4 * * *",
            "alert_on_failures": True
        }
    }

@pytest.fixture
def sample_game_data():
    """Create sample game data for testing."""
    return {
        "game_id": 13,
        "name": "Catan",
        "year_published": 1995,
        "min_players": 3,
        "max_players": 4,
        "playing_time": 120,
        "min_age": 10,
        "description": "Build, trade, settle!",
        "thumbnail": "thumbnail.jpg",
        "image": "image.jpg",
        "categories": ["Strategy", "Negotiation"],
        "mechanics": ["Dice Rolling", "Trading"],
        "families": ["Base Game"],
        "average": 7.5,
        "num_ratings": 1000,
        "owned": 500,
        "weight": 2.5,
        "load_timestamp": "2025-06-09T00:00:00Z"
    }

@pytest.fixture
def sample_api_response():
    """Create a sample BGG API response."""
    return {
        "items": {
            "item": {
                "@id": "13",
                "@type": "boardgame",
                "name": [
                    {
                        "@type": "primary",
                        "@value": "Catan"
                    },
                    {
                        "@type": "alternate",
                        "@value": "Settlers of Catan"
                    }
                ],
                "yearpublished": {"@value": "1995"},
                "minplayers": {"@value": "3"},
                "maxplayers": {"@value": "4"},
                "playingtime": {"@value": "120"},
                "minage": {"@value": "10"},
                "description": "Build, trade, settle!",
                "thumbnail": "thumbnail.jpg",
                "image": "image.jpg",
                "link": [
                    {"@type": "boardgamecategory", "@value": "Strategy"},
                    {"@type": "boardgamecategory", "@value": "Negotiation"},
                    {"@type": "boardgamemechanic", "@value": "Dice Rolling"},
                    {"@type": "boardgamemechanic", "@value": "Trading"},
                    {"@type": "boardgamefamily", "@value": "Base Game"}
                ],
                "statistics": {
                    "ratings": {
                        "average": {"@value": "7.5"},
                        "usersrated": {"@value": "1000"},
                        "owned": {"@value": "500"},
                        "averageweight": {"@value": "2.5"}
                    }
                }
            }
        }
    }

@pytest.fixture
def mock_responses():
    """Create mock HTTP responses."""
    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text
    
    return {
        "success": MockResponse(200, "<items><item id='13'></item></items>"),
        "rate_limit": MockResponse(429, "Too Many Requests"),
        "error": MockResponse(500, "Internal Server Error"),
        "not_found": MockResponse(404, "Not Found")
    }
