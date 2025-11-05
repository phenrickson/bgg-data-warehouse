"""Tests for bulk processing of BGG API requests."""

import logging
import os
from typing import List
from unittest.mock import patch

import pytest

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor
from src.id_fetcher.fetcher import BGGIDFetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock API responses
CATAN_TTR_RESPONSE = {
    "items": {
        "item": [
            {
                "@id": "13",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "CATAN", "@sortindex": "1"}],
                "yearpublished": {"@value": "1995"},
                "description": "Settle the island of Catan",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "100000"},
                        "average": {"@value": "7.5"},
                        "bayesaverage": {"@value": "7.2"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "100"}]
                        },
                    }
                },
            },
            {
                "@id": "9209",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "Ticket to Ride", "@sortindex": "1"}],
                "yearpublished": {"@value": "2004"},
                "description": "Build train routes across America",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "80000"},
                        "average": {"@value": "7.8"},
                        "bayesaverage": {"@value": "7.4"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "80"}]
                        },
                    }
                },
            },
        ]
    }
}

LESS_COMMON_GAMES_RESPONSE = {
    "items": {
        "item": [
            {
                "@id": "340834",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "Game A", "@sortindex": "1"}],
                "yearpublished": {"@value": "2020"},
                "description": "Description A",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "1000"},
                        "average": {"@value": "7.0"},
                        "bayesaverage": {"@value": "6.8"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "1000"}]
                        },
                    }
                },
            },
            {
                "@id": "164506",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "Game B", "@sortindex": "1"}],
                "yearpublished": {"@value": "2018"},
                "description": "Description B",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "2000"},
                        "average": {"@value": "7.2"},
                        "bayesaverage": {"@value": "7.0"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "800"}]
                        },
                    }
                },
            },
            {
                "@id": "357212",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "Game C", "@sortindex": "1"}],
                "yearpublished": {"@value": "2021"},
                "description": "Description C",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "1500"},
                        "average": {"@value": "7.5"},
                        "bayesaverage": {"@value": "7.2"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "600"}]
                        },
                    }
                },
            },
        ]
    }
}

CATAN_SEAFARERS_RESPONSE = {
    "items": {
        "item": [
            {
                "@id": "13",
                "@type": "boardgame",
                "name": [{"@type": "primary", "@value": "CATAN", "@sortindex": "1"}],
                "yearpublished": {"@value": "1995"},
                "description": "Settle the island of Catan",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "100000"},
                        "average": {"@value": "7.5"},
                        "bayesaverage": {"@value": "7.2"},
                        "ranks": {
                            "rank": [{"@type": "subtype", "@name": "boardgame", "@value": "100"}]
                        },
                    }
                },
            },
            {
                "@id": "325",
                "@type": "boardgameexpansion",
                "name": [{"@type": "primary", "@value": "Catan: Seafarers", "@sortindex": "1"}],
                "yearpublished": {"@value": "1997"},
                "description": "Expand Catan across the seas",
                "statistics": {
                    "ratings": {
                        "usersrated": {"@value": "20000"},
                        "average": {"@value": "7.3"},
                        "bayesaverage": {"@value": "7.1"},
                        "ranks": {
                            "rank": [
                                {"@type": "family", "@name": "boardgameexpansion", "@value": "50"}
                            ]
                        },
                    }
                },
            },
        ]
    }
}


@pytest.fixture
def api_client():
    """Create a BGG API client for testing."""
    # Set test API token
    os.environ["BGG_API_TOKEN"] = "test-token"
    client = BGGAPIClient()
    yield client
    # Clean up
    del os.environ["BGG_API_TOKEN"]


@pytest.fixture
def id_fetcher(api_client):
    """Create ID fetcher with mocked API client."""
    fetcher = BGGIDFetcher()
    fetcher.api_client = api_client
    return fetcher


def test_processor_with_popular_games(api_client):
    """Test processing popular games."""
    processor = BGGDataProcessor()

    # Mock API response
    with patch.object(api_client, "get_thing", return_value=CATAN_TTR_RESPONSE):
        # Process Catan
        result1 = processor.process_game(13, CATAN_TTR_RESPONSE, "boardgame")
        assert result1 is not None
        assert result1["game_id"] == 13
        assert result1["primary_name"].upper() == "CATAN"
        assert result1["type"] == "boardgame"
        assert result1["year_published"] == 1995

        # Process Ticket to Ride
        result2 = processor.process_game(9209, CATAN_TTR_RESPONSE, "boardgame")
        assert result2 is not None
        assert result2["game_id"] == 9209
        assert result2["primary_name"] == "Ticket to Ride"
        assert result2["type"] == "boardgame"
        assert result2["year_published"] == 2004


def test_processor_with_less_common_games(api_client):
    """Test processing less common games."""
    processor = BGGDataProcessor()

    # Mock API response
    with patch.object(api_client, "get_thing", return_value=LESS_COMMON_GAMES_RESPONSE):
        # Process each game
        for game_id in [340834, 164506, 357212]:
            result = processor.process_game(game_id, LESS_COMMON_GAMES_RESPONSE, "boardgame")
            assert result is not None
            assert result["game_id"] == game_id
            assert result["primary_name"] != "Unknown"
            assert result["type"] == "boardgame"
            assert result["year_published"] is not None

            # Verify we got meaningful data
            assert result["description"] != ""
            assert result["users_rated"] is not None
            assert result["average_rating"] is not None
            assert isinstance(result["categories"], list)
            assert isinstance(result["mechanics"], list)

            logger.info(
                f"Processed game {game_id}: {result['primary_name']} ({result['year_published']})"
            )


def test_processor_with_mixed_popularity(api_client):
    """Test processing a mix of popular and less common games."""
    processor = BGGDataProcessor()

    # Create combined response with all games
    combined_response = {
        "items": {
            "item": [
                *CATAN_TTR_RESPONSE["items"]["item"],
                *LESS_COMMON_GAMES_RESPONSE["items"]["item"],
            ]
        }
    }

    # Mock API response
    with patch.object(api_client, "get_thing", return_value=combined_response):
        # Process each game
        game_ids = [13, 340834, 164506, 357212]
        for game_id in game_ids:
            result = processor.process_game(game_id, combined_response, "boardgame")
            assert result is not None
            assert result["game_id"] == game_id

            # Log game details
            logger.info(
                f"Game {game_id}: {result['primary_name']} "
                f"(Year: {result['year_published']}, "
                f"Rating: {result['average_rating']:.1f}, "
                f"Users Rated: {result['users_rated']})"
            )


def test_processor_with_game_and_expansion(api_client):
    """Test processing both a game and its expansion."""
    processor = BGGDataProcessor()

    # Mock API response
    with patch.object(api_client, "get_thing", return_value=CATAN_SEAFARERS_RESPONSE):
        # Process base game
        result1 = processor.process_game(13, CATAN_SEAFARERS_RESPONSE, "boardgame")
        assert result1 is not None
        assert result1["game_id"] == 13
        assert result1["type"] == "boardgame"
        assert "CATAN" in result1["primary_name"].upper()

        # Process expansion
        result2 = processor.process_game(325, CATAN_SEAFARERS_RESPONSE, "boardgameexpansion")
        assert result2 is not None
        assert result2["game_id"] == 325
        assert result2["type"] == "boardgameexpansion"
        assert "Seafarers" in result2["primary_name"]


def test_data_completeness(api_client):
    """Test completeness of data for all games."""
    processor = BGGDataProcessor()

    # Create combined response with all games
    combined_response = {
        "items": {
            "item": [
                *CATAN_TTR_RESPONSE["items"]["item"],
                *LESS_COMMON_GAMES_RESPONSE["items"]["item"],
            ]
        }
    }

    # Test different batch sizes
    batches = [
        [340834],  # Single game
        [340834, 164506],  # Two games
        [340834, 164506, 357212],  # Three games
        [13, 340834, 164506, 357212],  # Four games
    ]

    for batch in batches:
        # Mock API response
        with patch.object(api_client, "get_thing", return_value=combined_response):
            # Process each game
            for game_id in batch:
                result = processor.process_game(game_id, combined_response, "boardgame")
                assert result is not None

                # Verify all important fields are present
                assert result["game_id"] == game_id
                assert result["primary_name"] != "Unknown"
                assert result["year_published"] is not None
                assert result["description"] != ""
                assert result["users_rated"] >= 0
                assert 0 <= result["average_rating"] <= 10
                assert len(result["categories"]) >= 0
                assert len(result["mechanics"]) >= 0

                logger.info(
                    f"Batch size {len(batch)}, " f"Game {game_id}: {result['primary_name']}"
                )


def test_id_fetching_and_processing(id_fetcher, api_client):
    """Test the complete flow of fetching IDs and processing them."""
    # Mock ID fetcher response
    mock_ids = [{"game_id": 13, "type": "boardgame"}, {"game_id": 9209, "type": "boardgame"}]

    with patch.object(id_fetcher, "fetch_ids", return_value=mock_ids):
        # Fetch IDs
        fetched_ids = id_fetcher.fetch_ids()
        assert len(fetched_ids) == 2
        assert fetched_ids[0]["game_id"] == 13
        assert fetched_ids[1]["game_id"] == 9209

        # Mock API response for processing
        with patch.object(api_client, "get_thing", return_value=CATAN_TTR_RESPONSE):
            processor = BGGDataProcessor()

            # Process each game
            for game in fetched_ids:
                result = processor.process_game(game["game_id"], CATAN_TTR_RESPONSE, game["type"])
                assert result is not None
                assert result["game_id"] == game["game_id"]
                assert result["type"] == game["type"]

                logger.info(f"Successfully processed game {game['game_id']}")


def test_real_api_fetching():
    """Test fetching and processing data from the real API."""
    # Create fetcher with real API client
    fetcher = BGGIDFetcher()
    processor = BGGDataProcessor()

    # Fetch some known game IDs
    game_ids = [13, 9209]  # Catan and Ticket to Ride

    # Get data from API
    api_response = fetcher.api_client.get_thing(game_ids)
    assert api_response is not None, "Failed to get response from API"

    # Process each game
    for game_id in game_ids:
        result = processor.process_game(game_id, api_response, "boardgame")
        assert result is not None
        assert result["game_id"] == game_id
        assert result["primary_name"] != "Unknown"
        assert result["type"] == "boardgame"
        assert result["year_published"] is not None

        # Verify we got meaningful data
        assert result["description"] != ""
        assert result["users_rated"] > 0
        assert 0 <= result["average_rating"] <= 10
        assert len(result["categories"]) > 0
        assert len(result["mechanics"]) > 0

        logger.info(
            f"Successfully fetched and processed game {game_id}: "
            f"{result['primary_name']} ({result['year_published']})"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
