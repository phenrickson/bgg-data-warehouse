"""Pipeline script for refreshing and processing stale game data.

This script runs the complete refresh pipeline:
1. Identifies games that need refreshing based on publication year and last fetch time
2. Fetches updated data from BGG and stores in raw_responses
3. Processes those refreshed responses into normalized tables (games, categories, mechanics, etc.)
"""

import argparse
import logging
import os

from dotenv import load_dotenv

from ..modules.response_refresher import ResponseRefresher
from ..modules.response_processor import ResponseProcessor
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for refreshing and processing old games."""
    parser = argparse.ArgumentParser(
        description="Refresh and process stale game data from BoardGameGeek"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode without fetching or processing data",
    )
    parser.add_argument(
        "--environment",
        default=os.getenv("ENVIRONMENT", "test"),
        choices=["test", "prod"],
        help="Environment to run in (default: test)",
    )
    args = parser.parse_args()

    environment = args.environment
    dry_run = args.dry_run

    logger.info(f"Starting refresh_old_games pipeline in {environment} environment")
    if dry_run:
        logger.info("Running in DRY RUN mode - no data will be fetched or processed")

    # Step 1: Refresh stale games
    logger.info("=" * 80)
    logger.info("Step 1: Identifying and refreshing stale games")
    logger.info("=" * 80)
    refresher = ResponseRefresher(
        chunk_size=20,
        environment=environment,
        dry_run=dry_run,
    )
    games_refreshed = refresher.run()

    if games_refreshed:
        logger.info("Games were refreshed - proceeding to process them")
    else:
        logger.info("No games refreshed - checking for unprocessed responses anyway")

    # Step 2: Process unprocessed responses (skip if dry run)
    if not dry_run:
        logger.info("=" * 80)
        logger.info("Step 2: Processing responses into normalized tables")
        logger.info("=" * 80)
        response_processor = ResponseProcessor(
            batch_size=100,
            environment=environment,
        )
        responses_processed = response_processor.run()
    else:
        logger.info("[DRY RUN] Skipping processing step")
        responses_processed = False

    # Summary
    logger.info("=" * 80)
    logger.info("refresh_old_games pipeline completed")
    logger.info("=" * 80)
    logger.info(f"Summary:")
    logger.info(f"  - Games refreshed: {'Yes' if games_refreshed else 'No'}")
    logger.info(f"  - Responses processed: {'Yes' if responses_processed else 'No (dry run)' if dry_run else 'No'}")

    if games_refreshed or responses_processed:
        logger.info("Pipeline completed successfully with refreshed data")
    else:
        logger.info("Pipeline completed - no data to refresh")


if __name__ == "__main__":
    main()
