"""Integration tests for the BGG data processor."""

import logging
from datetime import datetime, UTC

import pytest

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test with multiple games to verify processing
TEST_GAME_IDS = [13, 174430, 224517]  # Mix of old and new games


def test_processor_with_api_responses():
    """Test that processor can handle real API responses."""
    # Get real API responses
    client = BGGAPIClient()
    processor = BGGDataProcessor()

    # Fetch test games
    response = client.get_thing(TEST_GAME_IDS)
    assert response is not None, "Failed to get API response"

    # Process each game
    for game_id in TEST_GAME_IDS:
        processed = processor.process_game(
            game_id=game_id,
            api_response=response,
            game_type="boardgame",
            load_timestamp=datetime.now(UTC),
        )

        assert processed is not None, f"Failed to process game {game_id}"

        # Verify all required fields are present and have correct types
        assert isinstance(processed["game_id"], int), "game_id should be int"
        assert isinstance(processed["type"], str), "type should be str"
        assert isinstance(processed["primary_name"], str), "primary_name should be str"
        assert isinstance(processed["description"], str), "description should be str"
        assert isinstance(
            processed["year_published"], (int, type(None))
        ), "year_published should be int or None"
        assert isinstance(processed["min_players"], int), "min_players should be int"
        assert isinstance(processed["max_players"], int), "max_players should be int"
        assert isinstance(processed["playing_time"], int), "playing_time should be int"
        assert isinstance(processed["min_age"], int), "min_age should be int"

        # Verify statistics
        assert isinstance(processed["users_rated"], int), "users_rated should be int"
        assert isinstance(processed["average_rating"], float), "average_rating should be float"
        assert isinstance(processed["bayes_average"], float), "bayes_average should be float"

        # Verify linked entities are lists
        assert isinstance(processed["categories"], list), "categories should be list"
        assert isinstance(processed["mechanics"], list), "mechanics should be list"
        assert isinstance(processed["designers"], list), "designers should be list"
        assert isinstance(processed["publishers"], list), "publishers should be list"

        # Verify at least some linked entities exist
        assert len(processed["categories"]) > 0, "No categories found"
        assert len(processed["mechanics"]) > 0, "No mechanics found"

        # Verify poll data
        assert isinstance(processed["suggested_players"], list), "suggested_players should be list"
        assert isinstance(
            processed["language_dependence"], list
        ), "language_dependence should be list"
        assert isinstance(processed["suggested_age"], list), "suggested_age should be list"

        # Verify rankings
        assert isinstance(processed["rankings"], list), "rankings should be list"
        assert len(processed["rankings"]) > 0, "No rankings found"

        logger.info(f"Successfully processed game {game_id}")


def test_processor_prepare_for_bigquery():
    """Test that processed games can be prepared for BigQuery."""
    client = BGGAPIClient()
    processor = BGGDataProcessor()

    # Get and process test games
    response = client.get_thing(TEST_GAME_IDS)
    processed_games = []

    for game_id in TEST_GAME_IDS:
        game = processor.process_game(
            game_id=game_id,
            api_response=response,
            game_type="boardgame",
            load_timestamp=datetime.now(UTC),
        )
        if game:
            processed_games.append(game)

    assert len(processed_games) > 0, "No games were processed"

    # Prepare for BigQuery
    try:
        dataframes = processor.prepare_for_bigquery(processed_games)

        # Verify all expected tables are present
        expected_tables = {
            "games",
            "alternate_names",
            "categories",
            "mechanics",
            "game_categories",
            "game_mechanics",
            "rankings",
        }
        assert all(table in dataframes for table in expected_tables), "Missing expected tables"

        # Verify games table has all processed games
        assert len(dataframes["games"]) == len(processed_games), "Not all games in games table"

        # Verify data validation
        assert processor.validate_data(
            dataframes["games"], "games"
        ), "Games table validation failed"

        logger.info("Successfully prepared data for BigQuery")

    except Exception as e:
        pytest.fail(f"Failed to prepare data for BigQuery: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
