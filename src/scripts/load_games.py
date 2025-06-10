"""Script to load games into the dev data warehouse."""

import logging
import sys
from typing import List, Set

from google.cloud import bigquery

from src.api_client.client import BGGAPIClient
from src.config import get_bigquery_config
from src.data_processor.processor import BGGDataProcessor
from src.pipeline.load_data import DataLoader
from src.utils.logging_config import setup_logging
from src.warehouse.setup_bigquery import BigQuerySetup

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

def mark_games_processed(game_ids: List[int]) -> None:
    """Mark games as processed in BigQuery.
    
    Args:
        game_ids: List of game IDs to mark as processed
    """
    try:
        config = get_bigquery_config()
        client = bigquery.Client()
        
        # Format game IDs for SQL IN clause
        game_ids_str = ", ".join(str(id) for id in game_ids)
        
        # Update processed status
        query = f"""
        UPDATE `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
        SET 
            processed = TRUE,
            process_timestamp = CURRENT_TIMESTAMP()
        WHERE game_id IN ({game_ids_str})
        """
        
        job = client.query(query)
        job.result()  # Wait for job to complete
        logger.info(f"Marked {len(game_ids)} games as processed")
        
    except Exception as e:
        logger.error(f"Failed to mark games as processed: {e}")

def get_unprocessed_game_ids(limit: int = 1000) -> List[dict]:
    """Get IDs of unprocessed games from BigQuery.
    
    Returns:
        List of dictionaries containing game IDs and types
    """
    try:
        config = get_bigquery_config()
        client = bigquery.Client()
        
        # First check total number of records
        count_query = f"""
        SELECT COUNT(*) as total
        FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
        """
        count_df = client.query(count_query).to_dataframe()
        total_records = count_df["total"].iloc[0]
        logger.info(f"Total records in thing_ids table: {total_records}")
        
        # Check number of processed records
        processed_query = f"""
        SELECT COUNT(*) as processed
        FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
        WHERE processed = TRUE
        """
        processed_df = client.query(processed_query).to_dataframe()
        processed_records = processed_df["processed"].iloc[0]
        logger.info(f"Processed records: {processed_records}")
        logger.info(f"Unprocessed records: {total_records - processed_records}")
        
        # Query for unprocessed game IDs and types with limit
        query = f"""
        SELECT game_id, type, processed, process_timestamp
        FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
        WHERE NOT processed
        LIMIT {limit}
        """
        
        df = client.query(query).to_dataframe()
        logger.info(f"Query returned {len(df)} records")
        if len(df) > 0:
            logger.info("Sample of records:")
            logger.info(df.head())
            
        return [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
        
    except Exception as e:
        logger.error(f"Failed to fetch unprocessed game IDs: {e}")
        return set()

def load_games(games: List[dict], batch_size: int = 100) -> None:
    """Load specified games into the dev data warehouse.
    
    Args:
        game_ids: List of BGG game IDs to load
        batch_size: Number of games to process before loading to BigQuery
    """
    try:
        # Initialize components
        client = BGGAPIClient()
        processor = BGGDataProcessor()
        
        # Set up dev environment
        logger.info("Setting up BigQuery dev environment...")
        setup = BigQuerySetup(environment="dev")
        setup.setup_warehouse()
        
        processed_games = []
        games_loaded = 0
        total_games = len(games)
        current_batch = 0
        
        # Process games
        logger.info(f"Fetching and processing {total_games} games...")
        for game in games:
            try:
                game_id = game["game_id"]
                game_type = game["type"]
                # Fetch game data
                logger.info(f"Fetching data for game {game_id}...")
                response = client.get_thing(game_id)
                
                if not response:
                    logger.warning(f"No data returned for game {game_id}")
                    continue
                
                # Process game data
                logger.info(f"Processing game {game_id} (type: {game_type})...")
                processed = processor.process_game(game_id, response, game_type)
                
                if processed:
                    processed_games.append(processed)
                    games_loaded += 1
                    current_batch += 1
                    logger.info(f"Successfully processed game {game_id} ({games_loaded}/{total_games})")
                    
                    # Load batch if we've reached batch_size
                    if current_batch >= batch_size:
                        logger.info(f"Loading batch of {len(processed_games)} games into BigQuery...")
                        loader = DataLoader(environment="dev")
                        loader.load_games(processed_games)
                        
                        # Mark batch as processed
                        successfully_processed = [game["game_id"] for game in processed_games]
                        mark_games_processed(successfully_processed)
                        
                        # Clear batch
                        processed_games = []
                        current_batch = 0
                
            except Exception as e:
                logger.error(f"Failed to process game {game_id}: {e}")
                continue
        
        # Load any remaining games in the final batch
        if processed_games:
            logger.info(f"Loading final batch of {len(processed_games)} games into BigQuery...")
            loader = DataLoader(environment="dev")
            loader.load_games(processed_games)
            
            # Mark final batch as processed
            successfully_processed = [game["game_id"] for game in processed_games]
            mark_games_processed(successfully_processed)
            
            logger.info("All games loaded successfully!")
        elif games_loaded == 0:
            logger.error("No games were successfully processed")
            
    except Exception as e:
        logger.error(f"Failed to load games: {e}")
        raise

def main():
    """Main function."""
    # Check command line arguments
    if len(sys.argv) == 1:
        # No arguments - load unprocessed games in batches
        logger.info("No game IDs provided - loading unprocessed games in batches")
        while True:
            games = get_unprocessed_game_ids(limit=1000)
            if not games:
                logger.info("No more unprocessed games found")
                break
            logger.info(f"Processing batch of {len(games)} games")
            load_games(games)
    else:
        # Load specific games
        try:
            # For command line arguments, we need to query the types
            game_ids = [int(id) for id in sys.argv[1:]]
            config = get_bigquery_config()
            client = bigquery.Client()
            
            # Query for game types
            ids_str = ", ".join(str(id) for id in game_ids)
            query = f"""
            SELECT game_id, type
            FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
            WHERE game_id IN ({ids_str})
            """
            
            df = client.query(query).to_dataframe()
            games = [{"game_id": row["game_id"], "type": row["type"]} for _, row in df.iterrows()]
            
            if not games:
                logger.error("No games found with the provided IDs")
                sys.exit(1)
                
            load_games(games)
        except ValueError:
            print("Error: Game IDs must be integers")
            sys.exit(1)

if __name__ == "__main__":
    main()
