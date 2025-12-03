"""Pipeline script for fetching and processing new games.

This script runs the complete pipeline:
1. Fetches new game IDs from BGG and stores them in thing_ids table
2. Fetches API responses for those new games and stores in raw_responses
3. Processes those responses into normalized tables (games, categories, mechanics, etc.)
"""

import logging
import os

from dotenv import load_dotenv

from ..modules.id_fetcher import IDFetcher
from ..modules.response_fetcher import ResponseFetcher
from ..modules.response_processor import ResponseProcessor
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for fetching and processing new games."""
    environment = os.getenv("ENVIRONMENT", "test")
    logger.info(f"Starting fetch_new_games pipeline in {environment} environment")

    # Step 1: Fetch new IDs from BGG
    logger.info("=" * 80)
    logger.info("Step 1: Fetching new game IDs from BGG")
    logger.info("=" * 80)
    id_fetcher = IDFetcher(environment=environment)
    ids_fetched = id_fetcher.run()

    if ids_fetched:
        logger.info("New IDs were fetched - proceeding to fetch responses")
    else:
        logger.info("No new IDs found - checking for unfetched responses anyway")

    # Step 2: Fetch responses for unfetched games (new or previously failed)
    logger.info("=" * 80)
    logger.info("Step 2: Fetching responses for unfetched games")
    logger.info("=" * 80)
    response_fetcher = ResponseFetcher(
        batch_size=1000,
        chunk_size=20,
        environment=environment,
    )
    responses_fetched = response_fetcher.run()

    if responses_fetched:
        logger.info("Responses were fetched - proceeding to process them")
    else:
        logger.info("No responses fetched - checking for unprocessed responses anyway")

    # Step 3: Process unprocessed responses
    logger.info("=" * 80)
    logger.info("Step 3: Processing responses into normalized tables")
    logger.info("=" * 80)
    response_processor = ResponseProcessor(
        batch_size=100,
        environment=environment,
    )
    responses_processed = response_processor.run()

    # Summary
    logger.info("=" * 80)
    logger.info("fetch_new_games pipeline completed")
    logger.info("=" * 80)
    logger.info(f"Summary:")
    logger.info(f"  - New IDs fetched: {'Yes' if ids_fetched else 'No'}")
    logger.info(f"  - Responses fetched: {'Yes' if responses_fetched else 'No'}")
    logger.info(f"  - Responses processed: {'Yes' if responses_processed else 'No'}")

    if ids_fetched or responses_fetched or responses_processed:
        logger.info("Pipeline completed successfully with new data")
    else:
        logger.info("Pipeline completed - no new data to process")


if __name__ == "__main__":
    main()
