"""Pipeline script for processing raw BGG API responses.

This script processes stored raw responses and loads them into normalized tables.
"""

import logging
import os

from dotenv import load_dotenv

from ..modules.response_processor import ResponseProcessor
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for the response processor."""
    import argparse

    parser = argparse.ArgumentParser(description="Process BGG API responses")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of responses to process in each batch (default: 100)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default=None,
        help="Environment to use (prod/dev/test). Defaults to ENVIRONMENT env var or 'dev'",
    )

    args = parser.parse_args()

    environment = args.environment or os.getenv("ENVIRONMENT", "dev")
    logger.info(f"Starting process_responses pipeline in {environment} environment")

    processor = ResponseProcessor(
        batch_size=args.batch_size,
        environment=environment
    )
    responses_processed = processor.run()

    if responses_processed:
        logger.info("Responses were processed - pipeline completed successfully")
    else:
        logger.info("No responses processed - no data to process")


if __name__ == "__main__":
    main()
