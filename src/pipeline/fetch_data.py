"""Main pipeline module for fetching BGG data."""

import logging
from pathlib import Path
from typing import List, Optional, Set

from ..api_client.client import BGGAPIClient
from ..data_processor.processor import BGGDataProcessor
from ..id_fetcher.fetcher import BGGIDFetcher
from ..pipeline.load_data import BigQueryLoader
from ..config import get_bigquery_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BGGPipeline:
    """Pipeline for fetching and processing BGG data."""

    def __init__(self, batch_size: int = 100) -> None:
        """Initialize the pipeline.
        
        Args:
            batch_size: Number of games to process in each batch
        """
        self.config = get_bigquery_config()
        self.batch_size = batch_size
        self.id_fetcher = BGGIDFetcher()
        self.api_client = BGGAPIClient()
        self.processor = BGGDataProcessor()
        self.loader = BigQueryLoader()

    def get_unprocessed_ids(self) -> Set[int]:
        """Get IDs that haven't been processed yet.
        
        Returns:
            Set of unprocessed game IDs
        """
        query = f"""
        SELECT game_id
        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['tables']['raw']['thing_ids']}`
        WHERE NOT processed
        ORDER BY game_id
        LIMIT {self.batch_size}
        """
        
        try:
            df = self.api_client.client.query(query).to_dataframe()
            return set(df["game_id"].tolist())
        except Exception as e:
            logger.error("Failed to fetch unprocessed IDs: %s", e)
            return set()

    def mark_ids_as_processed(self, game_ids: Set[int], success: bool = True) -> None:
        """Mark game IDs as processed in BigQuery.
        
        Args:
            game_ids: Set of game IDs to mark
            success: Whether processing was successful
        """
        ids_str = ", ".join(str(id) for id in game_ids)
        query = f"""
        UPDATE `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['tables']['raw']['thing_ids']}`
        SET 
            processed = {str(success).lower()},
            process_timestamp = CURRENT_TIMESTAMP()
        WHERE game_id IN ({ids_str})
        """
        
        try:
            self.api_client.client.query(query).result()
            logger.info("Marked %d IDs as processed", len(game_ids))
        except Exception as e:
            logger.error("Failed to mark IDs as processed: %s", e)

    def process_games(self, game_ids: Set[int]) -> List[dict]:
        """Process a batch of games.
        
        Args:
            game_ids: Set of game IDs to process
            
        Returns:
            List of processed game data
        """
        processed_games = []
        
        for game_id in game_ids:
            try:
                # Fetch game data from API
                response = self.api_client.get_thing(game_id)
                if not response:
                    logger.warning("No response for game %d", game_id)
                    continue

                # Process the response
                processed = self.processor.process_game(game_id, response)
                if processed:
                    processed_games.append(processed)
                    logger.info("Successfully processed game %d", game_id)
                else:
                    logger.warning("Failed to process game %d", game_id)

            except Exception as e:
                logger.error("Error processing game %d: %s", game_id, e)

        return processed_games

    def run(self) -> None:
        """Run the pipeline."""
        logger.info("Starting BGG data pipeline")

        try:
            # Update game IDs from BGG
            temp_dir = Path("temp")
            self.id_fetcher.update_ids(temp_dir)

            # For testing, just process a few specific IDs
            game_ids = {110, 111, 112}  # These IDs worked in previous run
            logger.info("Using test IDs: %s", game_ids)

            logger.info("Processing %d games", len(game_ids))

            # Process games
            processed_games = self.process_games(game_ids)
            if not processed_games:
                logger.warning("No games were successfully processed")
                return

            # Prepare data for BigQuery
            games_df, categories_df, mechanics_df = self.processor.prepare_for_bigquery(
                processed_games
            )

            # Validate data
            if not all([
                self.processor.validate_data(games_df, "games"),
                self.processor.validate_data(categories_df, "categories"),
                self.processor.validate_data(mechanics_df, "mechanics")
            ]):
                logger.error("Data validation failed")
                return

            # Load data to BigQuery
            success = all([
                self.loader.load_table(
                    games_df,
                    self.config["datasets"]["raw"],
                    self.config["tables"]["raw"]["games"]
                ),
                self.loader.load_table(
                    categories_df,
                    self.config["datasets"]["raw"],
                    self.config["tables"]["raw"]["categories"]
                ),
                self.loader.load_table(
                    mechanics_df,
                    self.config["datasets"]["raw"],
                    self.config["tables"]["raw"]["mechanics"]
                )
            ])

            if success:
                # Mark games as processed
                processed_ids = {game["game_id"] for game in processed_games}
                self.mark_ids_as_processed(processed_ids)
                logger.info("Pipeline completed successfully")
                logger.info("Processed %d games", len(processed_games))
            else:
                logger.error("Failed to load data to BigQuery")
                return
            
            # Log API request statistics
            stats = self.api_client.get_request_stats(minutes=60)
            logger.info("API Stats (last hour): %s", stats)

        except Exception as e:
            logger.error("Pipeline failed: %s", e)
            raise

        finally:
            # Cleanup
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()

def main() -> None:
    """Main entry point for the pipeline."""
    # Use smaller batch size for testing
    pipeline = BGGPipeline(batch_size=5)
    pipeline.run()

if __name__ == "__main__":
    main()
