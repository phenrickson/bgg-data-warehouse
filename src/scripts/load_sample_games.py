"""Script to load a sample of games into the dev environment."""

import logging
import random
from typing import List

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor
from src.pipeline.load_data import DataLoader
from src.warehouse.setup_bigquery import BigQuerySetup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_random_game_ids(count: int = 200) -> List[int]:
    """Get a list of random game IDs.
    
    For now, just generate random IDs between 1 and 500000.
    In production, we would want to get these from a proper source.
    
    Args:
        count: Number of game IDs to generate
        
    Returns:
        List of game IDs
    """
    # Generate more than we need to account for API failures
    sample_size = count * 2
    return random.sample(range(1, 500000), sample_size)

def main():
    """Main function."""
    try:
        # Initialize components
        client = BGGAPIClient()
        processor = BGGDataProcessor()
        
        # Set up dev environment
        logger.info("Setting up BigQuery dev environment...")
        setup = BigQuerySetup(environment="dev")
        setup.setup_warehouse()
        
        # Get random game IDs
        game_ids = get_random_game_ids()
        processed_games = []
        games_loaded = 0
        
        # Process games until we have enough
        logger.info("Fetching and processing games...")
        for game_id in game_ids:
            if games_loaded >= 200:
                break
                
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
                    logger.info(f"Successfully processed game {game_id} ({games_loaded}/200)")
                
            except Exception as e:
                logger.error(f"Failed to process game {game_id}: {e}")
                continue
        
        # Load all processed games
        if processed_games:
            logger.info(f"Loading {len(processed_games)} games into BigQuery...")
            loader = DataLoader(environment="dev")
            loader.load_games(processed_games)
            logger.info("Sample games loaded successfully!")
        else:
            logger.error("No games were successfully processed")
            
    except Exception as e:
        logger.error(f"Failed to load sample games: {e}")
        raise

if __name__ == "__main__":
    main()
