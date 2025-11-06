"""Integration tests for BGG API connectivity."""

import logging
import pytest

from src.api_client.client import BGGAPIClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test with multiple games to verify API response structure
TEST_GAME_IDS = [13, 174430, 224517]  # Mix of old and new games


def test_bgg_api_response_structure():
    """Test that BGG API responses contain all required fields for processing."""
    client = BGGAPIClient()

    # Test batch API request
    response = client.get_thing(TEST_GAME_IDS)
    assert response is not None, "No response from BGG API"
    assert "items" in response, "Invalid response format"
    assert "item" in response["items"], "No items in response"

    # Get the game data
    items = response["items"]["item"]
    assert isinstance(items, list), "Multiple games should return a list"
    assert len(items) > 0, "No games returned"

    # Check each game has required fields
    for game in items:
        # Basic game info
        assert "@id" in game, "Game ID missing"
        assert "@type" in game, "Game type missing"
        assert "name" in game, "Game name missing"
        assert "yearpublished" in game, "Year published missing"
        assert "minplayers" in game, "Min players missing"
        assert "maxplayers" in game, "Max players missing"
        assert "playingtime" in game, "Playing time missing"
        assert "minplaytime" in game, "Min playtime missing"
        assert "maxplaytime" in game, "Max playtime missing"
        assert "minage" in game, "Min age missing"
        assert "description" in game, "Description missing"

        # Statistics section
        stats = game.get("statistics", {}).get("ratings", {})
        assert "average" in stats, "Average rating missing"
        assert "bayesaverage" in stats, "Bayes average missing"
        assert "usersrated" in stats, "Users rated missing"
        assert "ranks" in stats, "Rankings missing"

        # Links section (categories, mechanics, etc.)
        links = game.get("link", [])
        if isinstance(links, dict):
            links = [links]
        link_types = {link.get("@type") for link in links}
        assert "boardgamecategory" in link_types, "Categories missing"
        assert "boardgamemechanic" in link_types, "Mechanics missing"

        # Polls
        polls = game.get("poll", [])
        if not isinstance(polls, list):
            polls = [polls]
        poll_names = {poll.get("@name") for poll in polls}
        assert "suggested_numplayers" in poll_names, "Player count poll missing"

    logger.info("Successfully verified API response structure")


def test_bgg_api_rate_limiting():
    """Test that we can handle BGG API rate limiting."""
    client = BGGAPIClient()

    # Make multiple rapid requests to trigger rate limiting
    for _ in range(3):
        response = client.get_thing([TEST_GAME_IDS[0]])
        assert response is not None, "Failed to handle rate limiting"

    logger.info("Successfully handled BGG API rate limiting")


def test_bgg_api_error_handling():
    """Test that we handle API errors gracefully."""
    client = BGGAPIClient()

    # Test with invalid game ID
    response = client.get_thing([-1])
    assert response is not None, "Failed to handle invalid game ID"

    # Test with multiple IDs including an invalid one
    response = client.get_thing([TEST_GAME_IDS[0], -1])
    assert response is not None, "Failed to handle mixed valid/invalid IDs"

    logger.info("Successfully handled BGG API error cases")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
