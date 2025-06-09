"""Debug script to examine rankings data."""

import logging
from datetime import datetime

from src.api_client.client import BGGAPIClient
from src.data_processor.processor import BGGDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def examine_rankings() -> None:
    """Examine rankings data for a game."""
    game_id = 13  # Catan
    
    client = BGGAPIClient()
    processor = BGGDataProcessor()
    
    response = client.get_thing(game_id)
    if response:
        processed = processor.process_game(game_id, response)
        if processed:
            print("\nRankings for game", game_id)
            print("=" * 50)
            for rank in processed["rankings"]:
                print(f"Type: {rank['type']}")
                print(f"Name: {rank['name']}")
                print(f"Value: {rank['value']}")
                print("-" * 30)

if __name__ == "__main__":
    examine_rankings()
