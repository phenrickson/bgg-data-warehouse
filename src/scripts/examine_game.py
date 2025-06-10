"""Script to examine BGG XML data for a specific game."""

import json
from src.api_client.client import BGGAPIClient

def examine_game(game_id: int) -> None:
    """Fetch and examine XML data for a specific game.
    
    Args:
        game_id: ID of the game to examine
    """
    client = BGGAPIClient()
    endpoint = f"{client.BASE_URL}thing"
    params = {
        "id": game_id,
        "stats": 1,
        "type": "boardgame",
    }
    
    print(f"\nFetching data for game {game_id}...")
    response = client.session.get(endpoint, params=params)
    
    if response.status_code == 200:
        # Print raw XML
        print("\nRaw XML:")
        print(response.text)
        
        # Print parsed dictionary with nice formatting
        data = client.get_thing(game_id)
        if data:
            print("\nParsed Data:")
            print(json.dumps(data, indent=2))
        else:
            print("Failed to parse XML data")
    else:
        print(f"Failed to fetch game {game_id}: {response.status_code}")

if __name__ == "__main__":
    # Examine Catan (game ID 13)
    examine_game(13)
