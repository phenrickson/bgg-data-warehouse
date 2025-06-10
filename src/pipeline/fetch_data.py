"""Main pipeline module for fetching BGG data."""

import logging
from pathlib import Path
from typing import List

from google.cloud import bigquery

from ..id_fetcher.fetcher import BGGIDFetcher
from ..api_client.client import BGGAPIClient
from ..data_processor.processor import BGGDataProcessor
from ..pipeline.load_data import DataLoader
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

class BGGPipeline:
    """Pipeline for fetching and processing BGG data."""

    def __init__(self, batch_size: int = 100, environment: str = "prod") -> None:
        """Initialize the pipeline.
        
        Args:
            batch_size: Number of games to process in each batch
            environment: Environment to use (prod/dev/test)
        """
        self.config = get_bigquery_config()
        self.batch_size = batch_size
        self.environment = environment
        self.id_fetcher = BGGIDFetcher()
        self.api_client = BGGAPIClient()
        self.processor = BGGDataProcessor()
        self.loader = DataLoader(environment=environment)
        self.bq_client = bigquery.Client()

    def get_unprocessed_ids(self) -> List[dict]:
        """Get IDs that haven't been processed yet.
        
        Returns:
            List of dictionaries containing unprocessed game IDs and their types
        """
        try:
            # First check total number of boardgames
            count_query = f"""
            SELECT COUNT(*) as total
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE type = 'boardgame'
            """
            count_df = self.bq_client.query(count_query).to_dataframe()
            total_boardgames = count_df["total"].iloc[0]
            logger.info(f"Total boardgames in thing_ids table: {total_boardgames}")
            
            # Check number of processed boardgames
            processed_query = f"""
            SELECT COUNT(*) as processed
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE processed = TRUE AND type = 'boardgame'
            """
            processed_df = self.bq_client.query(processed_query).to_dataframe()
            processed_boardgames = processed_df["processed"].iloc[0]
            logger.info(f"Processed boardgames: {processed_boardgames}")
            logger.info(f"Unprocessed boardgames: {total_boardgames - processed_boardgames}")

            # Get unprocessed boardgames
            query = f"""
            SELECT game_id, type
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE NOT processed
            AND type = 'boardgame'
            ORDER BY game_id
            LIMIT {self.batch_size}
            """
            
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Query returned {len(df)} records")
            if len(df) > 0:
                logger.info("Sample of records:")
                logger.info(df.head())
                
            return [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
        except Exception as e:
            logger.error(f"Failed to fetch unprocessed IDs: {e}")
            return []

    def process_specific_games(self, game_ids: List[int]) -> None:
        """Process and load specific games.
        
        Args:
            game_ids: List of game IDs to process
        """
        try:
            # Query for game types
            ids_str = ", ".join(str(id) for id in game_ids)
            query = f"""
            SELECT game_id, type
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE game_id IN ({ids_str})
            """
            
            df = self.bq_client.query(query).to_dataframe()
            games = [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
            
            if not games:
                logger.error("No games found with the provided IDs")
                return
                
            # Process and load games
            self.process_and_load_batch(games)
            
        except Exception as e:
            logger.error(f"Failed to process specific games: {e}")
            raise

    def process_and_load_batch(self, games: List[dict]) -> bool:
        """Process and load a batch of games.
        
        Args:
            games: List of games to process and load
            
        Returns:
            bool: Whether the batch was processed and loaded successfully
        """
        if not games:
            logger.info("No games to process in this batch")
            return False

        # Process games
        processed_games = []
        games_loaded = 0
        total_games = len(games)
        
        logger.info(f"Processing {total_games} games...")
        for game in games:
            game_id = game["game_id"]
            game_type = game["type"]
            try:
                # Fetch game data from API
                logger.info(f"Fetching data for game {game_id}...")
                response = self.api_client.get_thing(game_id)
                if not response:
                    logger.warning(f"No data returned for game {game_id}")
                    continue

                # Process the response with type information
                logger.info(f"Processing game {game_id} (type: {game_type})...")
                processed = self.processor.process_game(game_id, response, game_type)
                if processed:
                    processed_games.append(processed)
                    games_loaded += 1
                    logger.info(f"Successfully processed game {game_id} ({games_loaded}/{total_games})")
                else:
                    logger.warning(f"Failed to process game {game_id}")

            except Exception as e:
                logger.error(f"Failed to process game {game_id}: {e}")

        if not processed_games:
            logger.warning("No games were successfully processed in this batch")
            return False

        # Prepare data for BigQuery
        dataframes = self.processor.prepare_for_bigquery(processed_games)

        # Validate data
        if not all([
            self.processor.validate_data(df, table_name)
            for table_name, df in dataframes.items()
        ]):
            logger.error("Data validation failed for this batch")
            return False

        # Load data to BigQuery
        try:
            self.loader.load_games(processed_games)
            success = True
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            success = False

        if success:
            # Mark games as processed
            processed_ids = [game["game_id"] for game in processed_games]
            self.mark_ids_as_processed(processed_ids)
            logger.info("Batch completed successfully")
            logger.info(f"Processed {len(processed_games)} games")
            
            # Log API request statistics
            stats = self.api_client.get_request_stats(minutes=60)
            logger.info(f"API Stats (last hour): {stats}")
            
            return True
        else:
            logger.error("Failed to load batch to BigQuery")
            return False

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
            logger.info(f"Marked {len(game_ids)} IDs as processed")
        except Exception as e:
            logger.error(f"Failed to mark IDs as processed: {e}")

    def run(self) -> None:
        """Run the pipeline."""
        logger.info("Starting BGG data pipeline")

        try:
            # Only fetch new IDs in production
            if self.environment == "prod":
                temp_dir = Path("temp")
                self.id_fetcher.update_ids(temp_dir)
                try:
                    while True:
                        # Get unprocessed IDs from the database
                        unprocessed_games = self.get_unprocessed_ids()
                        if not unprocessed_games:
                            logger.info("No more unprocessed games found")
                            break

                        # Process and load batch
                        if not self.process_and_load_batch(unprocessed_games):
                            continue

                    logger.info("Pipeline completed - all games processed")
                finally:
                    # Cleanup
                    if temp_dir.exists():
                        for file in temp_dir.glob("*"):
                            file.unlink()
                        temp_dir.rmdir()
            else:
                # In dev/test, just process unprocessed games
                while True:
                    unprocessed_games = self.get_unprocessed_ids()
                    if not unprocessed_games:
                        logger.info("No more unprocessed games found")
                        break

                    # Process and load batch
                    if not self.process_and_load_batch(unprocessed_games):
                        continue

                logger.info("Pipeline completed - all games processed")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

def main() -> None:
    """Main entry point for the pipeline."""
    # Use smaller batch size for testing
    pipeline = BGGPipeline(batch_size=50)
    pipeline.run()

if __name__ == "__main__":
    main()
