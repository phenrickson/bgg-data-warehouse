"""Pipeline module for fetching and updating BGG game IDs."""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from ..id_fetcher.fetcher import BGGIDFetcher
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class BGGIDUpdater:
    """Updates BGG game IDs in the data warehouse."""

    def __init__(self, environment: str = "prod") -> None:
        """Initialize the ID updater.

        Args:
            environment: Environment to use (prod/dev/test)
        """
        self.config = get_bigquery_config(environment)
        self.environment = environment
        self.id_fetcher = BGGIDFetcher(self.config)

    def run(self) -> bool:
        """Run the ID update process.

        Returns:
            bool: True if new IDs were found and added, False otherwise
        """
        logger.info("Starting BGG ID update process")

        try:
            # Create temp directory for ID updates
            temp_dir = Path("temp")
            temp_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Download and parse IDs
                ids_file = self.id_fetcher.download_ids(temp_dir)
                all_games = self.id_fetcher.parse_ids(ids_file)

                # Get existing IDs and find new ones
                existing_ids = self.id_fetcher.get_existing_ids()
                new_games = [
                    game
                    for game in all_games
                    if (game["game_id"], game["type"]) not in existing_ids
                ]

                if new_games:
                    logger.info(f"Found {len(new_games)} new game IDs")
                    self.id_fetcher.upload_new_ids(new_games)
                    logger.info("ID update completed successfully - new IDs added")
                    return True
                else:
                    logger.info("No new game IDs found")
                    return False

            finally:
                # Cleanup temp directory
                if temp_dir.exists():
                    for file in temp_dir.glob("*"):
                        file.unlink()
                    temp_dir.rmdir()

        except Exception as e:
            logger.error(f"ID update failed: {e}")
            raise


def main() -> None:
    """Main entry point for the ID updater."""

    environment = os.getenv("ENVIRONMENT", "test")
    logger.info(f"Starting ID updater in {environment} environment")

    updater = BGGIDUpdater(environment=environment)
    has_new_ids = updater.run()

    if has_new_ids:
        logger.info("New IDs were added - fetch_responses should be triggered")
    else:
        logger.info("No new IDs - no further action needed")


if __name__ == "__main__":
    main()
