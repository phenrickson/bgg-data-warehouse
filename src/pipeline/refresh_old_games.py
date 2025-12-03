"""Pipeline script for refreshing stale game data.

This script identifies games that need refreshing based on their publication year
and last fetch time, then fetches updated data from BGG.
"""

import logging
import os

from dotenv import load_dotenv

from ..modules.response_refresher import ResponseRefresher
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for refreshing old games."""
    environment = os.getenv("ENVIRONMENT", "test")
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    logger.info(f"Starting refresh_old_games pipeline in {environment} environment")
    if dry_run:
        logger.info("Running in DRY RUN mode - no data will be fetched")

    # Refresh stale games
    refresher = ResponseRefresher(
        chunk_size=20,
        environment=environment,
        dry_run=dry_run,
    )
    games_refreshed = refresher.run()

    if games_refreshed:
        logger.info("Games were refreshed - process_responses should be run next")
    else:
        logger.info("No games refreshed")

    # Summary
    logger.info("refresh_old_games pipeline completed")
    if games_refreshed:
        logger.info("Next step: Run process_responses to process the refreshed data")


if __name__ == "__main__":
    main()
