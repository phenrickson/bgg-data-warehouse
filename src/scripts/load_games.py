"""Script to load games into the dev data warehouse."""

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
logger = setup_logging(__name__)

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

def get_unprocessed_game_ids(limit: int = 1000) -> Set[int]:
    """Get IDs of unprocessed games from BigQuery.
    
    Returns:
        Set of unprocessed game IDs
    """
    try:
        config = get_bigquery_config()
        client = bigquery.Client()
        
        # Query for unprocessed game IDs with limit
        query = f"""
        SELECT game_id
        FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['thing_ids']['name']}`
        WHERE NOT processed
        LIMIT {limit}
        """
        
        df = client.query(query).to_dataframe()
        return set(df["game_id"].tolist())
        
    except Exception as e:
        logger.error(f"Failed to fetch unprocessed game IDs: {e}")
        return set()

def load_games(game_ids: List[int], batch_size: int = 100) -> None:
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
        total_games = len(game_ids)
        current_batch = 0
        
        # Process games
        logger.info(f"Fetching and processing {total_games} games...")
        for game_id in game_ids:
            try:
                # Fetch game data
                logger.info(f"Fetching data for game {game_id}...")
                response = client.get_thing(game_id)
                
                if not response:
                    logger.warning(f"No data returned for game {game_id}")
                    continue
                
                # Process game data
                logger.info(f"Processing game {game_id}...")
                processed = processor.process_game(game_id, response)
                
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
            game_ids = list(get_unprocessed_game_ids(limit=1000))
            if not game_ids:
                logger.info("No more unprocessed games found")
                break
            logger.info(f"Processing batch of {len(game_ids)} games")
            load_games(game_ids)
    else:
        # Load specific games
        try:
            game_ids = [int(id) for id in sys.argv[1:]]
        except ValueError:
            print("Error: Game IDs must be integers")
            sys.exit(1)
        load_games(game_ids)

if __name__ == "__main__":
    main()
