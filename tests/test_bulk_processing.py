"""Tests for bulk processing of BGG API requests."""

import logging
import time
from typing import List

import pytest
import requests
import xmltodict

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_games(game_ids: List[int]) -> dict:
    """Helper function to fetch games from BGG API.

    Args:
        game_ids: List of game IDs to fetch

    Returns:
        Parsed API response
    """
    # Convert IDs to comma-separated string
    ids_str = ",".join(str(id) for id in game_ids)

    # Construct URL
    url = f"https://boardgamegeek.com/xmlapi2/thing?id={ids_str}&type=boardgame,boardgameexpansion&stats=1"

    # Make request with retries
    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            response = requests.get(url)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", retry_delay))
                logger.warning(f"Rate limited, waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            # Handle accepted but not ready
            if response.status_code == 202:
                logger.info("Request accepted, waiting for data...")
                time.sleep(retry_delay)
                continue

            response.raise_for_status()
            return xmltodict.parse(response.text)

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Request failed, retrying in {retry_delay} seconds: {e}")
            time.sleep(retry_delay)

    raise Exception("Failed to fetch games after maximum retries")


def test_processor_with_popular_games():
    """Test processing popular games from real API data."""
    processor = BGGDataProcessor()

    # Fetch Catan and Ticket to Ride
    data = fetch_games([13, 9209])

    # Process Catan
    result1 = processor.process_game(13, data, "boardgame")
    assert result1 is not None
    assert result1["game_id"] == 13
    assert result1["primary_name"].upper() == "CATAN"
    assert result1["type"] == "boardgame"
    assert result1["year_published"] == 1995

    # Process Ticket to Ride
    result2 = processor.process_game(9209, data, "boardgame")
    assert result2 is not None
    assert result2["game_id"] == 9209
    assert result2["primary_name"] == "Ticket to Ride"
    assert result2["type"] == "boardgame"
    assert result2["year_published"] == 2004


def test_processor_with_less_common_games():
    """Test processing less common games from real API data."""
    processor = BGGDataProcessor()

    # Fetch less common games
    data = fetch_games([340834, 164506, 357212])

    # Process each game
    for game_id in [340834, 164506, 357212]:
        result = processor.process_game(game_id, data, "boardgame")
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


def test_processor_with_mixed_popularity():
    """Test processing a mix of popular and less common games."""
    processor = BGGDataProcessor()

    # Fetch mix of popular and less common games
    game_ids = [13, 340834, 164506, 357212]
    data = fetch_games(game_ids)

    # Process each game
    for game_id in game_ids:
        result = processor.process_game(game_id, data, "boardgame")
        assert result is not None
        assert result["game_id"] == game_id

        # Log game details
        logger.info(
            f"Game {game_id}: {result['primary_name']} "
            f"(Year: {result['year_published']}, "
            f"Rating: {result['average_rating']:.1f}, "
            f"Users Rated: {result['users_rated']})"
        )


def test_processor_with_game_and_expansion():
    """Test processing both a game and its expansion."""
    processor = BGGDataProcessor()

    # Fetch Catan and Seafarers expansion
    data = fetch_games([13, 325])

    # Process base game
    result1 = processor.process_game(13, data, "boardgame")
    assert result1 is not None
    assert result1["game_id"] == 13
    assert result1["type"] == "boardgame"
    assert "CATAN" in result1["primary_name"].upper()

    # Process expansion
    result2 = processor.process_game(325, data, "boardgameexpansion")
    assert result2 is not None
    assert result2["game_id"] == 325
    assert result2["type"] == "boardgameexpansion"
    assert "Seafarers" in result2["primary_name"]


def test_data_completeness():
    """Test completeness of data for all games."""
    processor = BGGDataProcessor()

    # Test different batch sizes
    batches = [
        [340834],  # Single game
        [340834, 164506],  # Two games
        [340834, 164506, 357212],  # Three games
        [13, 340834, 164506, 357212],  # Four games
    ]

    for batch in batches:
        data = fetch_games(batch)

        # Verify we got all games
        items = data["items"]["item"]
        if not isinstance(items, list):
            items = [items]
        assert len(items) == len(batch)

        # Process each game
        for game_id in batch:
            result = processor.process_game(game_id, data, "boardgame")
            assert result is not None

            # Verify all important fields are present
            assert result["game_id"] == game_id
            assert result["primary_name"] != "Unknown"
            assert result["year_published"] is not None
            assert result["description"] != ""
            assert result["users_rated"] >= 0
            assert 0 <= result["average_rating"] <= 10
            assert len(result["categories"]) > 0
            assert len(result["mechanics"]) > 0
            # Rankings might be empty for some games
            if result["rankings"]:
                # Verify ranking structure if present
                for rank in result["rankings"]:
                    assert "type" in rank
                    assert "name" in rank
                    assert "value" in rank

            logger.info(f"Batch size {len(batch)}, Game {game_id}: {result['primary_name']}")


def test_rate_limiting():
    """Test rate limiting behavior with multiple requests."""
    # Make several requests in quick succession
    game_batches = [[340834, 164506, 357212], [13, 9209], [325, 177736]]

    start_time = time.time()

    for batch in game_batches:
        data = fetch_games(batch)
        assert "items" in data
        assert "item" in data["items"]

        # Log timing
        elapsed = time.time() - start_time
        logger.info(f"Fetched batch of {len(batch)} games after {elapsed:.2f}s")

        # Brief pause to avoid rate limiting
        time.sleep(0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
