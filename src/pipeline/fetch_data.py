"""Main pipeline module for fetching BGG data."""

import logging
from pathlib import Path
from typing import List, Optional, Set

from google.cloud import bigquery

from ..api_client.client import BGGAPIClient
from ..data_processor.processor import BGGDataProcessor
from ..id_fetcher.fetcher import BGGIDFetcher
from ..pipeline.load_data import DataLoader
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
        self.loader = DataLoader()
        self.bq_client = bigquery.Client()

    def get_unprocessed_ids(self) -> List[dict]:
        """Get IDs that haven't been processed yet.
        
        Returns:
            List of dictionaries containing unprocessed game IDs and their types
        """
        query = f"""
        SELECT game_id, type
        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
        WHERE NOT processed
        ORDER BY game_id
        LIMIT {self.batch_size}
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            return [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
        except Exception as e:
            logger.error("Failed to fetch unprocessed IDs: %s", e)
            return []

    def mark_ids_as_processed(self, game_ids: List[int], success: bool = True) -> None:
        """Mark game IDs as processed in BigQuery.
        
        Args:
            game_ids: List of game IDs to mark
            success: Whether processing was successful
        """
        ids_str = ", ".join(str(id) for id in game_ids)
        query = f"""
        UPDATE `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
        SET 
            processed = {str(success).lower()},
            process_timestamp = CURRENT_TIMESTAMP()
        WHERE game_id IN ({ids_str})
        """
        
        try:
            self.bq_client.query(query).result()
            logger.info("Marked %d IDs as processed", len(game_ids))
        except Exception as e:
            logger.error("Failed to mark IDs as processed: %s", e)

    def process_games(self, games: List[dict]) -> List[dict]:
        """Process a batch of games.
        
        Args:
            games: List of dictionaries containing game IDs and types to process
            
        Returns:
            List of processed game data
        """
        processed_games = []
        
        for game in games:
            game_id = game["game_id"]
            game_type = game["type"]
            try:
                # Fetch game data from API
                response = self.api_client.get_thing(game_id)
                if not response:
                    logger.warning("No response for game %d", game_id)
                    continue

                # Process the response with type information
                processed = self.processor.process_game(game_id, response, game_type)
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

            # Get unprocessed IDs from the database
            unprocessed_games = self.get_unprocessed_ids()
            if not unprocessed_games:
                logger.info("No unprocessed games found")
                return

            logger.info("Processing %d games", len(unprocessed_games))

            # Process games
            processed_games = self.process_games(unprocessed_games)
            if not processed_games:
                logger.warning("No games were successfully processed")
                return

            # Prepare data for BigQuery
            dataframes = self.processor.prepare_for_bigquery(processed_games)

            # Validate data
            if not all([
                self.processor.validate_data(df, table_name)
                for table_name, df in dataframes.items()
            ]):
                logger.error("Data validation failed")
                return

            # Load data to BigQuery
            try:
                self.loader.load_games(processed_games)
                success = True
            except Exception as e:
                logger.error("Failed to load data: %s", e)
                success = False

            if success:
                # Mark games as processed
                processed_ids = [game["game_id"] for game in processed_games]
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
