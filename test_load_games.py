"""Test script to load a sample of board games into the data warehouse."""

import logging
from typing import List

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor
from src.pipeline.load_data import DataLoader
from src.warehouse.setup_bigquery import BigQuerySetup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_sample_games() -> None:
    """Load a sample set of board games into BigQuery."""
    # Sample of diverse games to test with
    game_ids = [
        13,      # Catan - Classic game with lots of expansions
        174430,  # Gloomhaven - Complex modern game with lots of mechanics
        1406,    # Monopoly - Very old game with many versions
        2651,    # Power Grid - Economic game with different implementations
        822,     # Carcassonne - Tile laying game with many expansions
    ]
    
    try:
        # First, ensure BigQuery is set up
        logger.info("Setting up BigQuery tables...")
        setup = BigQuerySetup()
        setup.setup_warehouse()
        
        # Initialize components
        client = BGGAPIClient()
        processor = BGGDataProcessor()
        loader = DataLoader()
        
        # Process each game
        processed_games = []
        for game_id in game_ids:
            try:
                logger.info(f"Fetching data for game {game_id}...")
                response = client.get_thing(game_id)
                
                if response:
                    logger.info(f"Processing game {game_id}...")
                    processed = processor.process_game(game_id, response)
                    if processed:
                        processed_games.append(processed)
                    else:
                        logger.warning(f"Failed to process game {game_id}")
                else:
                    logger.warning(f"Failed to fetch game {game_id}")
                    
            except Exception as e:
                logger.error(f"Error processing game {game_id}: {e}")
                continue
        
        # Load all processed games
        if processed_games:
            logger.info(f"Loading {len(processed_games)} games into BigQuery...")
            loader.load_games(processed_games)
            logger.info("Sample games loaded successfully!")
        else:
            logger.error("No games were processed successfully")
            
    except Exception as e:
        logger.error(f"Failed to load sample games: {e}")
        raise

if __name__ == "__main__":
    load_sample_games()
