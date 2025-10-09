"""Script to load games into the dev data warehouse."""

import logging
import sys

from ..pipeline.fetch_data import BGGPipeline
from ..utils.logging_config import setup_logging
from ..warehouse.setup_bigquery import BigQuerySetup

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def load_games(game_ids: list[int] = None, batch_size: int = 100) -> None:
    """Load games into the dev data warehouse.

    Args:
        game_ids: Optional list of specific game IDs to load. If None, loads unprocessed games.
        batch_size: Number of games to process in each batch
    """
    try:
        # Set up dev environment
        logger.info("Setting up BigQuery dev environment...")
        setup = BigQuerySetup(environment="dev")
        setup.setup_warehouse()

        # Initialize pipeline in dev mode
        pipeline = BGGPipeline(batch_size=batch_size, environment="dev")

        if game_ids:
            # Load specific games
            logger.info(f"Loading {len(game_ids)} specific games...")
            pipeline.process_specific_games(game_ids)
        else:
            # Load unprocessed games
            logger.info("Loading unprocessed games...")
            pipeline.run()

    except Exception as e:
        logger.error(f"Failed to load games: {e}")
        raise


def main():
    """Main function."""
    # Check command line arguments
    if len(sys.argv) == 1:
        # No arguments - load unprocessed games
        logger.info("No game IDs provided - loading unprocessed games")
        load_games()
    else:
        # Load specific games
        try:
            game_ids = [int(id) for id in sys.argv[1:]]
            load_games(game_ids=game_ids)
        except ValueError:
            logger.error("Error: Game IDs must be integers")
            sys.exit(1)


if __name__ == "__main__":
    main()
