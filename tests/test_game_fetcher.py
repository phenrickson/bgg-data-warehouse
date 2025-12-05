"""Test script for GameFetcher module."""

import json
import logging
from src.modules.game_fetcher_processor import GameFetcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_single_game():
    """Test fetching a single game."""
    logger.info("=" * 80)
    logger.info("Testing GameFetcher with a single game")
    logger.info("=" * 80)

    # Initialize the processor
    processor = GameFetcher()

    # Test with Gloomhaven (game_id: 174430)
    game_id = 174430
    logger.info(f"Fetching game features for game_id {game_id} (Gloomhaven)...")

    game_features = processor.fetch_game_features(game_id)

    if game_features:
        logger.info("✓ Successfully fetched game features!")
        logger.info("Game Features:")
        logger.info("-" * 80)

        # Log core game info
        logger.info(f"Game ID: {game_features['game_id']}")
        logger.info(f"Name: {game_features['name']}")
        logger.info(f"Year: {game_features['year_published']}")
        logger.info(f"Average Rating: {game_features['average_rating']}")
        logger.info(f"Bayes Average: {game_features['bayes_average']}")
        logger.info(f"Complexity (Weight): {game_features['average_weight']}")
        logger.info(f"Users Rated: {game_features['users_rated']}")
        logger.info(f"Players: {game_features['min_players']}-{game_features['max_players']}")
        logger.info(f"Playtime: {game_features['min_playtime']}-{game_features['max_playtime']} min")
        logger.info(f"Min Age: {game_features['min_age']}+")

        # Log arrays
        logger.info(f"Categories ({len(game_features['categories'])}): {', '.join(game_features['categories'][:5])}...")
        logger.info(f"Mechanics ({len(game_features['mechanics'])}): {', '.join(game_features['mechanics'][:5])}...")
        logger.info(f"Designers ({len(game_features['designers'])}): {', '.join(game_features['designers'])}")
        logger.info(f"Publishers ({len(game_features['publishers'])}): {', '.join(game_features['publishers'][:3])}...")
        logger.info(f"Artists ({len(game_features['artists'])}): {', '.join(game_features['artists'][:3])}...")
        logger.info(f"Families ({len(game_features['families'])}): {', '.join(game_features['families'][:3])}...")

        # Save to file
        output_file = f"game_features_{game_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(game_features, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Full output saved to {output_file}")

    else:
        logger.error("✗ Failed to fetch game features")
        return False

    return True


def test_multiple_games():
    """Test fetching multiple games."""
    logger.info("=" * 80)
    logger.info("Testing GameFetcher with multiple games")
    logger.info("=" * 80)

    # Initialize the processor
    processor = GameFetcher()

    # Test with a few popular games
    game_ids = [
        174430,  # Gloomhaven
        161936,  # Pandemic Legacy: Season 1
        167791,  # Terraforming Mars
    ]

    logger.info(f"Fetching game features for {len(game_ids)} games...")

    results = processor.fetch_multiple_game_features(game_ids)

    logger.info("Results:")
    logger.info("-" * 80)

    for game_id, game_features in results.items():
        if game_features:
            logger.info(f"✓ {game_id}: {game_features['name']} ({game_features['year_published']})")
        else:
            logger.error(f"✗ {game_id}: Failed to fetch")

    successful = sum(1 for v in results.values() if v is not None)
    logger.info(f"Successfully fetched {successful}/{len(game_ids)} games")

    return successful > 0


if __name__ == "__main__":
    logger.info("Starting GameFetcher tests...")

    # Test single game
    success1 = test_single_game()

    # Test multiple games
    success2 = test_multiple_games()

    logger.info("=" * 80)
    if success1 and success2:
        logger.info("✓ All tests completed successfully!")
    else:
        logger.error("✗ Some tests failed")
    logger.info("=" * 80)
