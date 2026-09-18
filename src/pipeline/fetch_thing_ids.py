"""Pipeline script for fetching and storing BGG thing IDs.

Discovers new game IDs and uploads them to the thing_ids table. Two methods:

- probe (default): walks the numeric ID space above the known frontier via the
  authenticated XML API. No browser, no Cloudflare surface.
- sitemap: crawls BGG's sitemaps with a stealth browser. Sees the whole catalog
  rather than just the frontier, so it still catches items added below the
  frontier, but depends on getting past Cloudflare.

Environment variables:
- BGG_API_TOKEN: required for probe (BGG's XML API rejects anonymous requests)
- BROWSER_HEADLESS: set to "false" to run the browser visibly (sitemap only)
"""

import argparse
import logging

from dotenv import load_dotenv

from ..modules.id_fetcher import SOURCE_PROBE, SOURCE_SITEMAP, IDFetcher
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def main() -> None:
    """Main entry point for fetching thing IDs."""
    parser = argparse.ArgumentParser(description="Discover new BGG thing IDs")
    parser.add_argument(
        "--source",
        choices=[SOURCE_PROBE, SOURCE_SITEMAP],
        default=SOURCE_PROBE,
        help="Discovery method (default: %(default)s)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=0,
        help=(
            "For %s: how many IDs below the known max to re-probe (skipping ones "
            "already in raw.thing_ids) before walking the frontier. 0 = frontier only "
            "(default: %%(default)s)" % SOURCE_PROBE
        ),
    )
    args = parser.parse_args()

    logger.info(
        "Starting fetch_thing_ids pipeline (source=%s, lookback=%d)", args.source, args.lookback
    )

    id_fetcher = IDFetcher()
    ids_fetched = id_fetcher.run(source=args.source, lookback=args.lookback)

    if ids_fetched:
        logger.info("fetch_thing_ids completed: new IDs were added to thing_ids table")
    else:
        logger.info("fetch_thing_ids completed: no new IDs found")


if __name__ == "__main__":
    main()
