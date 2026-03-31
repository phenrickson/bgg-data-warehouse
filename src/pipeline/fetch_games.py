"""Pipeline script for fetching and processing specific games on demand.

Reads game IDs from the GAME_IDS environment variable (comma-separated),
fetches their data from the BGG API, and processes responses into
normalized BigQuery tables.

Usage:
    GAME_IDS=467694,12345 python -m src.pipeline.fetch_games
"""

import logging
import os
from typing import List, Optional

from dotenv import load_dotenv

from ..modules.response_refresher import ResponseRefresher
from ..modules.response_processor import ResponseProcessor
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def parse_game_ids(game_ids_str: Optional[str]) -> List[int]:
    """Parse comma-separated game IDs string into a list of integers.

    Args:
        game_ids_str: Comma-separated string of game IDs (e.g., "467694,12345")

    Returns:
        Deduplicated list of integer game IDs

    Raises:
        ValueError: If input is empty or contains non-integer values
    """
    if not game_ids_str or not game_ids_str.strip():
        raise ValueError("No game IDs provided. Set the GAME_IDS environment variable.")

    ids = []
    for part in game_ids_str.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            raise ValueError(f"Invalid game ID: '{part}'. Game IDs must be integers.")

    if not ids:
        raise ValueError("No game IDs provided. Set the GAME_IDS environment variable.")

    return list(dict.fromkeys(ids))


def main() -> None:
    """Main entry point for on-demand game fetching."""
    game_ids_str = os.environ.get("GAME_IDS", "")
    game_ids = parse_game_ids(game_ids_str)

    logger.info(f"Starting on-demand fetch for {len(game_ids)} game(s): {game_ids}")

    # Step 1: Fetch responses from BGG API
    logger.info("=" * 80)
    logger.info("Step 1: Fetching responses from BGG API")
    logger.info("=" * 80)
    refresher = ResponseRefresher(chunk_size=20)
    games_to_refresh = [{"game_id": gid} for gid in game_ids]
    responses_fetched = refresher.fetch_batch(games_to_refresh)

    if responses_fetched:
        logger.info("Responses fetched - proceeding to process them")
    else:
        logger.info("No responses fetched - checking for unprocessed responses anyway")

    # Step 2: Process responses into normalized tables
    logger.info("=" * 80)
    logger.info("Step 2: Processing responses into normalized tables")
    logger.info("=" * 80)
    response_processor = ResponseProcessor(
        batch_size=100,
    )
    responses_processed = response_processor.run()

    # Summary
    logger.info("=" * 80)
    logger.info("On-demand fetch pipeline completed")
    logger.info("=" * 80)
    logger.info(f"Summary:")
    logger.info(f"  - Game IDs requested: {game_ids}")
    logger.info(f"  - Responses fetched: {'Yes' if responses_fetched else 'No'}")
    logger.info(f"  - Responses processed: {'Yes' if responses_processed else 'No'}")


if __name__ == "__main__":
    main()
