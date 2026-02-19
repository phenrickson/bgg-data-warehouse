"""Pipeline script for fetching and storing BGG thing IDs.

This script discovers new game IDs by scraping BGG's sitemaps directly
and uploads them to the thing_ids table.

Environment variables:
- BROWSER_HEADLESS: Set to "false" to run browser in visible mode (default: true)
"""

import logging

from dotenv import load_dotenv

from ..modules.id_fetcher import IDFetcher
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for fetching thing IDs."""
    logger.info("Starting fetch_thing_ids pipeline")
    logger.info("Fetching game IDs directly from BGG sitemaps")

    id_fetcher = IDFetcher()
    ids_fetched = id_fetcher.run(use_browser=True)

    if ids_fetched:
        logger.info("fetch_thing_ids completed: new IDs were added to thing_ids table")
    else:
        logger.info("fetch_thing_ids completed: no new IDs found")


if __name__ == "__main__":
    main()
