"""Pipeline script for fetching new games.

This script:
1. Fetches new game IDs from BGG and stores them in thing_ids table
2. Fetches API responses for those new games and stores in raw_responses
"""

import logging
import os

from dotenv import load_dotenv

from ..modules.id_fetcher import IDFetcher
from ..modules.response_fetcher import ResponseFetcher
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for fetching new games."""
    environment = os.getenv("ENVIRONMENT", "test")
    logger.info(f"Starting fetch_new_games pipeline in {environment} environment")

    # Step 1: Fetch new IDs from BGG
    logger.info("Step 1: Fetching new game IDs from BGG")
    id_fetcher = IDFetcher(environment=environment)
    ids_fetched = id_fetcher.run()

    if ids_fetched:
        logger.info("New IDs were fetched - proceeding to fetch responses")
    else:
        logger.info("No new IDs found - checking for unfetched responses anyway")

    # Step 2: Fetch responses for unfetched games (new or previously failed)
    logger.info("Step 2: Fetching responses for unfetched games")
    response_fetcher = ResponseFetcher(
        batch_size=1000,
        chunk_size=20,
        environment=environment,
    )
    responses_fetched = response_fetcher.run()

    if responses_fetched:
        logger.info("Responses were fetched - process_responses should be run next")
    else:
        logger.info("No responses fetched")

    # Summary
    logger.info("fetch_new_games pipeline completed")
    if ids_fetched or responses_fetched:
        logger.info("Next step: Run process_responses to process the fetched data")


if __name__ == "__main__":
    main()
