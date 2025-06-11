"""Base pipeline module for BGG data processing."""

import logging
from typing import List, Dict, Any

from google.cloud import bigquery
import pandas as pd

from ..api_client.client import BGGAPIClient
from ..data_processor.processor import BGGDataProcessor
from ..pipeline.load_data import DataLoader
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

class BaseBGGPipeline:
    """Base pipeline for processing BGG data."""

    def __init__(self, batch_size: int = 100, environment: str = "prod") -> None:
        """Initialize the pipeline.
        
        Args:
            batch_size: Number of games to process in each batch
            environment: Environment to use (prod/dev)
        """
        self.config = get_bigquery_config()
        self.batch_size = batch_size
        self.environment = environment
        self.api_client = BGGAPIClient()
        self.processor = BGGDataProcessor()
        self.loader = DataLoader(environment=environment)
        self.bq_client = bigquery.Client()

    def get_unprocessed_ids(self, limit: int = None) -> List[dict]:
        """Get IDs that haven't been processed yet.
        
        Args:
            limit: Optional limit on number of IDs to return. If None, uses batch_size.
            
        Returns:
            List of dictionaries containing unprocessed game IDs and their types
        """
        try:
            # First check total number of records
            count_query = f"""
            SELECT COUNT(*) as total
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            """
            count_df = self.bq_client.query(count_query).to_dataframe()
            total_records = count_df["total"].iloc[0]
            logger.info(f"Total records in thing_ids table: {total_records}")
            
            # Check number of processed records
            processed_query = f"""
            SELECT COUNT(*) as processed
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE processed = TRUE
            """
            processed_df = self.bq_client.query(processed_query).to_dataframe()
            processed_records = processed_df["processed"].iloc[0]
            logger.info(f"Processed records: {processed_records}")
            logger.info(f"Unprocessed records: {total_records - processed_records}")

            # Get unprocessed IDs
            query = f"""
            SELECT game_id, type
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            WHERE NOT processed
            ORDER BY game_id
            LIMIT {limit or self.batch_size}
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

    def process_games(self, games: List[dict]) -> List[dict]:
        """Process a batch of games.
        
        Args:
            games: List of dictionaries containing game IDs and types to process
            
        Returns:
            List of processed game data
        """
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

        return processed_games

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
        processed_games = self.process_games(games)
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
