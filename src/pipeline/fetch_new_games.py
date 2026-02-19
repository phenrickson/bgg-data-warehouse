"""Pipeline script for fetching and processing new games.

This script fetches API responses for unfetched game IDs in thing_ids table,
then processes those responses into normalized tables.

Note: This script assumes thing_ids is already populated. Run fetch_thing_ids.py
first to discover and upload new game IDs.

Steps:
1. Fetches API responses for unfetched games in thing_ids
2. Processes those responses into normalized tables (games, categories, mechanics, etc.)
"""

import logging

from dotenv import load_dotenv

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
    logger.info("Starting fetch_new_games pipeline")

    # Step 1: Fetch responses for unfetched games
    logger.info("=" * 80)
    logger.info("Step 1: Fetching responses for unfetched games")
    logger.info("=" * 80)
    response_fetcher = ResponseFetcher(
        batch_size=1000,
        chunk_size=20,
    )
    responses_fetched = response_fetcher.run()

    if responses_fetched:
        logger.info("Responses were fetched - proceeding to process them")
    else:
        logger.info("No responses fetched - checking for unprocessed responses anyway")

    # Step 2: Process unprocessed responses
    logger.info("=" * 80)
    logger.info("Step 2: Processing responses into normalized tables")
    logger.info("=" * 80)
    response_processor = ResponseProcessor(
        batch_size=100,
    )
    responses_processed = response_processor.run()

    # Summary
    logger.info("=" * 80)
    logger.info("fetch_new_games pipeline completed")
    logger.info("=" * 80)
    logger.info("Summary:")
    logger.info(f"  - Responses fetched: {'Yes' if responses_fetched else 'No'}")
    logger.info(f"  - Responses processed: {'Yes' if responses_processed else 'No'}")

    if responses_fetched or responses_processed:
        logger.info("Pipeline completed successfully with new data")
    else:
        logger.info("Pipeline completed - no new data to process")


if __name__ == "__main__":
    main()
