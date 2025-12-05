"""Manual script for processing responses without triggering via Pub/Sub.

This script can be used to:
1. Re-process responses without re-fetching
2. Test processing locally
3. Process responses when Cloud Function is unavailable
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
    """Main entry point for manual response processing."""
    environment = os.getenv("ENVIRONMENT", "test")
    logger.info(f"Starting manual response processing in {environment} environment")

    # Initialize processor
    processor = ResponseProcessor(
        batch_size=100,
        environment=environment,
    )

    # Process all unprocessed responses
    responses_processed = processor.run()

    if responses_processed:
        logger.info("Manual processing completed successfully")
    else:
        logger.info("No responses to process")


if __name__ == "__main__":
    main()
