"""Script to examine BGG XML data for a specific game."""

import json
import logging
from collections import Counter

from src.api_client.client import BGGAPIClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        data = client.get_thing(game_id)
        if data:
            # Extract and analyze implementation links
            items = data.get("items", {}).get("item", [])
            if not isinstance(items, list):
                items = [items]

            for item in items:
                if str(item.get("@id")) == str(game_id):
                    links = item.get("link", [])
                    if not isinstance(links, list):
                        links = [links]

                    # Collect implementation links
                    implementations = []
                    for link in links:
                        if link.get("@type") == "boardgameimplementation":
                            implementations.append(
                                {"id": link.get("@id"), "value": link.get("@value")}
                            )

                    # Analyze for duplicates
                    print(f"\nGame {game_id} Implementation Analysis:")
                    print(f"Total implementation links: {len(implementations)}")

                    # Check for duplicate IDs
                    impl_ids = [impl["id"] for impl in implementations]
                    id_counts = Counter(impl_ids)
                    duplicates = {id: count for id, count in id_counts.items() if count > 1}

                    if duplicates:
                        print("\nDuplicate implementations found:")
                        for impl_id, count in duplicates.items():
                            print(f"Implementation ID {impl_id} appears {count} times")
                            # Show the full entries for duplicates
                            dupes = [impl for impl in implementations if impl["id"] == impl_id]
                            print(json.dumps(dupes, indent=2))
                    else:
                        print("\nNo duplicate implementations found")

                    print("\nAll implementations:")
                    print(json.dumps(implementations, indent=2))

            # Print full parsed data for reference
            print("\nFull Parsed Data:")
            print(json.dumps(data, indent=2))
        else:
            print("Failed to parse XML data")
    else:
        print(f"Failed to fetch game {game_id}: {response.status_code}")


if __name__ == "__main__":
    # Examine a few games known to have implementations
    # Check specific games
    games_to_check = [
        181279,  # Example game
        20963,  # Example game
        936,  # Example game
        1406,  # Monopoly (should have many implementations)
    ]

    for game_id in games_to_check:
        examine_game(game_id)
        print("\n" + "=" * 80 + "\n")
