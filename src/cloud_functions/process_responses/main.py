"""Cloud Function for processing BGG API responses.

This function is triggered by Pub/Sub messages and processes unprocessed responses
from the raw_responses table into normalized BigQuery tables.
"""

import base64
import logging
import os
import sys

# Add the project root to the path so we can import from src
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.modules.response_processor import ResponseProcessor
from src.utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def process_responses(event, context):
    """Cloud Function entry point for processing responses.

    This function is triggered by Pub/Sub messages and runs the ResponseProcessor
    to process all unprocessed responses from the raw_responses table.

    Args:
        event: The Pub/Sub event payload
        context: The Cloud Function context
    """
    # Get environment from environment variable
    environment = os.getenv("ENVIRONMENT", "prod")

    logger.info("=" * 80)
    logger.info(f"Cloud Function triggered for environment: {environment}")
    logger.info("=" * 80)

    try:
        # Decode Pub/Sub message (optional - we don't use the content)
        if 'data' in event:
            message = base64.b64decode(event['data']).decode('utf-8')
            logger.info(f"Pub/Sub message: {message}")

        # Initialize and run the processor
        processor = ResponseProcessor(
            batch_size=100,
            environment=environment,
        )

        # Process all unprocessed responses
        responses_processed = processor.run()

        if responses_processed:
            logger.info("Processing completed successfully")
        else:
            logger.info("No responses to process")

        logger.info("=" * 80)
        logger.info("Cloud Function execution completed")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Cloud Function failed: {e}")
        raise
