"""Pipeline module for fetching and storing raw BGG API responses."""

import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import List, Dict

from google.cloud import bigquery

from ..id_fetcher.fetcher import BGGIDFetcher
from ..api_client.client import BGGAPIClient
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

class BGGResponseFetcher:
    """Fetches and stores raw BGG API responses."""
    
    def __init__(self, batch_size: int = 1000, chunk_size: int = 20, environment: str = "prod") -> None:
        """Initialize the fetcher.
        
        Args:
            batch_size: Number of games to fetch in each batch from BigQuery
            chunk_size: Number of games to request in each API call
            environment: Environment to use (prod/dev/test)
        """
        self.config = get_bigquery_config()
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.environment = environment
        self.id_fetcher = BGGIDFetcher()
        self.api_client = BGGAPIClient()
        self.bq_client = bigquery.Client()

    def get_unfetched_ids(self) -> List[Dict]:
        """Get IDs that haven't had responses fetched yet.
        
        Returns:
            List of dictionaries containing unfetched game IDs and their types
        """
        try:
            query = f"""
            WITH thing_ids AS (
                SELECT game_id, type
                FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}`
            ),
            raw_responses AS (
                SELECT game_id
                FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}`
            )
            SELECT game_id, type
            FROM thing_ids
            WHERE NOT EXISTS (
                SELECT 1 
                FROM raw_responses
                WHERE thing_ids.game_id = raw_responses.game_id
            )
            AND type = 'boardgame'
            ORDER BY game_id
            LIMIT {self.batch_size}
            """
            
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Found {len(df)} unfetched games")
            
            # Handle both Pandas and Polars DataFrames
            if hasattr(df, 'to_dict'):
                # Polars DataFrame
                return [{"game_id": row['game_id'], "type": row['type']} for row in df.to_dicts()]
            else:
                # Pandas DataFrame
                return [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
        except Exception as e:
            logger.error(f"Failed to fetch unprocessed IDs: {e}")
            return []

    def store_response(self, game_ids: List[int], response_data: str) -> None:
        """Store raw API response in BigQuery.
        
        Args:
            game_ids: List of game IDs in the response
            response_data: Raw API response data
        """
        now = datetime.now(UTC)
        rows = [{
            "game_id": game_id,
            "response_data": response_data,
            "fetch_timestamp": now.isoformat(),
            "processed": False,
            "process_timestamp": None,
            "process_status": None,
            "process_attempt": 0
        } for game_id in game_ids]
        
        table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}"
        
        try:
            # Load to BigQuery
            errors = self.bq_client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Failed to store responses: {errors}")
            else:
                logger.info(f"Stored responses for {len(game_ids)} games")
        except Exception as e:
            logger.error(f"Failed to store responses: {e}")
            raise

    def fetch_batch(self) -> bool:
        """Fetch and store a batch of responses.
        
        Returns:
            bool: Whether any responses were fetched
        """
        # Get unfetched IDs
        unfetched = self.get_unfetched_ids()
        if not unfetched:
            logger.info("No unfetched games found")
            return False
            
        # Process in chunks
        for i in range(0, len(unfetched), self.chunk_size):
            chunk = unfetched[i:i + self.chunk_size]
            chunk_ids = [game["game_id"] for game in chunk]
            
            try:
                # Fetch data from API
                logger.info(f"Fetching data for games {chunk_ids}...")
                response = self.api_client.get_thing(chunk_ids)
                if response:
                    # Store raw response
                    self.store_response(chunk_ids, str(response))
                else:
                    logger.warning(f"No data returned for games {chunk_ids}")
                    
            except Exception as e:
                logger.error(f"Failed to fetch chunk {chunk_ids}: {e}")
                
        return True

    def run(self) -> None:
        """Run the fetcher pipeline."""
        logger.info("Starting BGG response fetcher")
        
        try:
            # Only fetch new IDs in production
            if self.environment == "prod":
                temp_dir = Path("temp")
                self.id_fetcher.update_ids(temp_dir)
                try:
                    while True:
                        if not self.fetch_batch():
                            break
                    logger.info("Fetcher completed - all responses fetched")
                finally:
                    # Cleanup
                    if temp_dir.exists():
                        for file in temp_dir.glob("*"):
                            file.unlink()
                        temp_dir.rmdir()
            else:
                # In dev/test, just fetch responses
                while self.fetch_batch():
                    pass
                logger.info("Fetcher completed - all responses fetched")
                    
        except Exception as e:
            logger.error(f"Fetcher failed: {e}")
            raise

def main() -> None:
    """Main entry point for the fetcher."""
    fetcher = BGGResponseFetcher(
        batch_size=1000,
        chunk_size=20,
    )
    fetcher.run()

if __name__ == "__main__":
    main()
