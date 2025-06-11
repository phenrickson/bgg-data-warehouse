"""Pipeline module for processing raw BGG API responses."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Optional

from google.cloud import bigquery

from ..data_processor.processor import BGGDataProcessor
from ..pipeline.load_data import DataLoader
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

class BGGResponseProcessor:
    """Processes raw BGG API responses into BigQuery tables."""
    
    def __init__(self, batch_size: int = 100, max_retries: int = 3, environment: str = "prod") -> None:
        """Initialize the processor.
        
        Args:
            batch_size: Number of responses to process in each batch
            max_retries: Maximum number of retry attempts per response
            environment: Environment to use (prod/dev/test)
        """
        self.config = get_bigquery_config()
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.environment = environment
        self.processor = BGGDataProcessor()
        self.loader = DataLoader(environment=environment)
        self.bq_client = bigquery.Client()

    def get_unprocessed_responses(self) -> List[Dict]:
        """Get responses that haven't been processed yet.
        
        Returns:
            List of dictionaries containing unprocessed responses
        """
        try:
            query = f"""
            SELECT game_id, response_data
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}`
            WHERE NOT processed
            AND process_attempt < {self.max_retries}
            ORDER BY fetch_timestamp
            LIMIT {self.batch_size}
            """
            
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Found {len(df)} unprocessed responses")
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to fetch unprocessed responses: {e}")
            return []

    def mark_response_processed(
        self,
        game_id: int,
        success: bool = True,
        error: Optional[str] = None
    ) -> None:
        """Mark a response as processed in BigQuery.
        
        Args:
            game_id: ID of the game to mark
            success: Whether processing was successful
            error: Optional error message if processing failed
        """
        now = datetime.now(UTC)
        query = f"""
        UPDATE `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}`
        SET 
            processed = {str(success).lower()},
            process_timestamp = TIMESTAMP("{now.isoformat()}"),
            process_status = {"NULL" if not error else f"'{error}'"},
            process_attempt = process_attempt + 1
        WHERE game_id = {game_id}
        """
        
        try:
            self.bq_client.query(query).result()
            logger.info(f"Marked response {game_id} as processed (success={success})")
        except Exception as e:
            logger.error(f"Failed to mark response as processed: {e}")

    def process_batch(self) -> bool:
        """Process a batch of responses.
        
        Returns:
            bool: Whether any responses were processed
        """
        # Get unprocessed responses
        responses = self.get_unprocessed_responses()
        if not responses:
            logger.info("No unprocessed responses found")
            return False
            
        try:
            # Process all responses in batch
            game_ids = [r["game_id"] for r in responses]
            logger.info(f"Processing {len(responses)} responses...")
            
            # Convert responses back to dicts
            processed_games = []
            failed_games = []
            
            for response in responses:
                try:
                    processed = self.processor.process_game(
                        response["game_id"],
                        eval(response["response_data"]),
                        "boardgame"
                    )
                    if processed:
                        processed_games.append(processed)
                    else:
                        failed_games.append(response["game_id"])
                except Exception as e:
                    logger.error(f"Failed to process game {response['game_id']}: {e}")
                    failed_games.append(response["game_id"])
            
            if not processed_games:
                logger.warning("No games were successfully processed in this batch")
                # Mark all as failed
                for game_id in game_ids:
                    self.mark_response_processed(
                        game_id,
                        success=False,
                        error="Failed to process any games in batch"
                    )
                return True
            
            # Prepare all data for BigQuery at once
            logger.info(f"Preparing BigQuery data for {len(processed_games)} games...")
            dataframes = self.processor.prepare_for_bigquery(processed_games)
            
            # Only validate core game data - other tables are optional
            validation_success = True
            if 'games' in dataframes:
                validation_success = self.processor.validate_data(dataframes['games'], 'games')
            else:
                validation_success = False
                logger.error("No core game data found in processed results")
            
            if not validation_success:
                logger.error("Data validation failed for batch")
                # Mark all as failed
                for game_id in game_ids:
                    self.mark_response_processed(
                        game_id,
                        success=False,
                        error="Data validation failed"
                    )
                return True
            
            # Load all games in one transaction
            logger.info(f"Loading {len(processed_games)} games to BigQuery...")
            self.loader.load_games(processed_games)
            
            # Wait for streaming buffer (30 seconds is usually enough)
            import time
            logger.info("Waiting for streaming buffer to clear...")
            time.sleep(30)
            
            # Mark successful games as processed
            successful_ids = [game["game_id"] for game in processed_games]
            for game_id in successful_ids:
                try:
                    self.mark_response_processed(game_id, success=True)
                except Exception as e:
                    logger.error(f"Failed to mark game {game_id} as processed: {e}")
            
            # Mark failed games
            for game_id in failed_games:
                try:
                    self.mark_response_processed(
                        game_id,
                        success=False,
                        error="Failed to process game data"
                    )
                except Exception as e:
                    logger.error(f"Failed to mark game {game_id} as failed: {e}")
            
            logger.info(f"Successfully processed {len(successful_ids)} games")
            logger.info(f"Failed to process {len(failed_games)} games")
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            # Mark all as failed
            for game_id in game_ids:
                self.mark_response_processed(
                    game_id,
                    success=False,
                    error=str(e)[:1000]
                )
        
        return True

    def run(self) -> None:
        """Run the processor pipeline."""
        logger.info("Starting BGG response processor")
        
        try:
            while self.process_batch():
                pass
            logger.info("Processor completed - all responses processed")
                
        except Exception as e:
            logger.error(f"Processor failed: {e}")
            raise

def main() -> None:
    """Main entry point for the processor."""
    processor = BGGResponseProcessor(
        batch_size=100,
        max_retries=3,
    )
    processor.run()

if __name__ == "__main__":
    main()
