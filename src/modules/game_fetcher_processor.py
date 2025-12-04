"""Standalone module for fetching and processing BGG game data without database dependencies."""

import logging
from datetime import datetime, UTC
from typing import Dict, Optional, Union, List
import ast

from ..api_client.client import BGGAPIClient
from ..data_processor.processor import BGGDataProcessor
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class GameFetcher:
    """Standalone fetcher and processor for BGG game data.

    This class combines the functionality of ResponseFetcher and ResponseProcessor
    but operates independently without requiring a data warehouse or BigQuery.
    It can fetch game data from the BGG API and process it into the format
    expected by the data warehouse.
    """

    def __init__(self) -> None:
        """Initialize the fetcher and processor."""
        self.api_client = BGGAPIClient()
        self.processor = BGGDataProcessor()
        logger.info("Initialized standalone GameFetcher")

    def fetch_game(self, game_id: int) -> Optional[Dict]:
        """Fetch raw game data from the BGG API.

        Args:
            game_id: The BGG game ID to fetch

        Returns:
            Dictionary containing raw API response data, or None if fetch fails
        """
        try:
            logger.info(f"Fetching game data for game_id {game_id}")

            # Fetch from API (API client handles rate limiting and retries)
            response = self.api_client.get_thing(game_id, stats=True)

            if not response:
                logger.warning(f"No data returned for game_id {game_id}")
                return None

            # Parse the response to extract the specific game item
            try:
                # Convert response to string if needed
                response_str = str(response)
                parsed_response = ast.literal_eval(response_str)

                # Extract items from the response
                items = parsed_response.get("items", {}).get("item", [])

                # Ensure items is a list
                if not isinstance(items, list):
                    items = [items] if items else []

                # Find the specific game in the response
                for item in items:
                    item_id = int(item.get("@id", 0))
                    if item_id == game_id:
                        logger.info(f"Successfully fetched game_id {game_id}")
                        # Return as the expected response structure
                        return {"items": {"item": item}}

                logger.warning(f"Game_id {game_id} not found in API response")
                return None

            except Exception as parse_error:
                logger.error(f"Failed to parse response for game_id {game_id}: {parse_error}")
                return None

        except Exception as e:
            logger.error(f"Failed to fetch game_id {game_id}: {e}")
            return None

    def process_game(
        self,
        game_id: int,
        response_data: Dict,
        game_type: str = "boardgame"
    ) -> Optional[Dict]:
        """Process raw game data into warehouse format.

        Args:
            game_id: The BGG game ID
            response_data: Raw API response data (dict format)
            game_type: Type of game (default: "boardgame")

        Returns:
            Dictionary containing processed game data in warehouse format,
            or None if processing fails
        """
        try:
            logger.info(f"Processing game_id {game_id}")

            # Use the BGGDataProcessor to process the game
            load_timestamp = datetime.now(UTC)
            processed_game = self.processor.process_game(
                game_id=game_id,
                api_response=response_data,
                game_type=game_type,
                load_timestamp=load_timestamp
            )

            if not processed_game:
                logger.warning(f"Processing returned None for game_id {game_id}")
                return None

            logger.info(f"Successfully processed game_id {game_id}")
            return processed_game

        except Exception as e:
            logger.error(f"Failed to process game_id {game_id}: {e}")
            return None

    def fetch_and_process_game(
        self,
        game_id: int,
        game_type: str = "boardgame"
    ) -> Optional[Dict]:
        """Fetch and process a game in one call.

        This is a convenience method that combines fetching and processing.

        Args:
            game_id: The BGG game ID to fetch and process
            game_type: Type of game (default: "boardgame")

        Returns:
            Dictionary containing processed game data in warehouse format,
            or None if either fetch or processing fails
        """
        try:
            logger.info(f"Fetching and processing game_id {game_id}")

            # Fetch the raw data
            response_data = self.fetch_game(game_id)

            if not response_data:
                logger.warning(f"Failed to fetch game_id {game_id}")
                return None

            # Process the data
            processed_game = self.process_game(
                game_id=game_id,
                response_data=response_data,
                game_type=game_type
            )

            if not processed_game:
                logger.warning(f"Failed to process game_id {game_id}")
                return None

            logger.info(f"Successfully fetched and processed game_id {game_id}")
            return processed_game

        except Exception as e:
            logger.error(f"Failed to fetch and process game_id {game_id}: {e}")
            return None

    def fetch_and_process_games(
        self,
        game_ids: List[int],
        game_type: str = "boardgame"
    ) -> Dict[int, Optional[Dict]]:
        """Fetch and process multiple games.

        Args:
            game_ids: List of BGG game IDs to fetch and process
            game_type: Type of games (default: "boardgame")

        Returns:
            Dictionary mapping game_id to processed game data.
            Games that failed to fetch or process will have None as their value.
        """
        results = {}

        logger.info(f"Fetching and processing {len(game_ids)} games")

        for game_id in game_ids:
            try:
                processed_game = self.fetch_and_process_game(game_id, game_type)
                results[game_id] = processed_game
            except Exception as e:
                logger.error(f"Failed to fetch and process game_id {game_id}: {e}")
                results[game_id] = None

        successful = sum(1 for v in results.values() if v is not None)
        logger.info(f"Successfully processed {successful}/{len(game_ids)} games")

        return results

    def to_game_features(self, processed_game: Dict) -> Optional[Dict]:
        """Transform processed game data into game_features_materialized format.

        Args:
            processed_game: Processed game data from process_game()

        Returns:
            Dictionary in game_features_materialized format with all fields,
            or None if transformation fails
        """
        try:
            logger.info(f"Transforming game_id {processed_game.get('game_id')} to game_features format")

            # Extract core game fields
            game_features = {
                "game_id": processed_game.get("game_id"),
                "name": processed_game.get("name"),
                "year_published": processed_game.get("year_published"),
                "bayes_average": processed_game.get("bayes_average"),
                "average_rating": processed_game.get("average_rating"),
                "average_weight": processed_game.get("average_weight"),
                "users_rated": processed_game.get("users_rated"),
                "num_weights": processed_game.get("num_weights"),
                "min_players": processed_game.get("min_players"),
                "max_players": processed_game.get("max_players"),
                "min_playtime": processed_game.get("min_playtime"),
                "max_playtime": processed_game.get("max_playtime"),
                "min_age": processed_game.get("min_age"),
                "image": processed_game.get("image"),
                "thumbnail": processed_game.get("thumbnail"),
                "description": processed_game.get("description"),
            }

            # Extract arrays of names from relationships
            # Categories
            categories = []
            for cat in processed_game.get("categories", []):
                if isinstance(cat, dict) and "name" in cat:
                    categories.append(cat["name"])
                elif isinstance(cat, str):
                    categories.append(cat)
            game_features["categories"] = categories

            # Mechanics
            mechanics = []
            for mech in processed_game.get("mechanics", []):
                if isinstance(mech, dict) and "name" in mech:
                    mechanics.append(mech["name"])
                elif isinstance(mech, str):
                    mechanics.append(mech)
            game_features["mechanics"] = mechanics

            # Publishers
            publishers = []
            for pub in processed_game.get("publishers", []):
                if isinstance(pub, dict) and "name" in pub:
                    publishers.append(pub["name"])
                elif isinstance(pub, str):
                    publishers.append(pub)
            game_features["publishers"] = publishers

            # Designers
            designers = []
            for des in processed_game.get("designers", []):
                if isinstance(des, dict) and "name" in des:
                    designers.append(des["name"])
                elif isinstance(des, str):
                    designers.append(des)
            game_features["designers"] = designers

            # Artists
            artists = []
            for art in processed_game.get("artists", []):
                if isinstance(art, dict) and "name" in art:
                    artists.append(art["name"])
                elif isinstance(art, str):
                    artists.append(art)
            game_features["artists"] = artists

            # Families
            families = []
            for fam in processed_game.get("families", []):
                if isinstance(fam, dict) and "name" in fam:
                    families.append(fam["name"])
                elif isinstance(fam, str):
                    families.append(fam)
            game_features["families"] = families

            # Add timestamp
            game_features["last_updated"] = datetime.now(UTC).isoformat()

            logger.info(f"Successfully transformed game_id {processed_game.get('game_id')} to game_features format")
            return game_features

        except Exception as e:
            logger.error(f"Failed to transform to game_features format: {e}")
            return None

    def fetch_game_features(self, game_id: int, game_type: str = "boardgame") -> Optional[Dict]:
        """Fetch a game and return it in game_features_materialized format.

        This is the main method that combines fetch -> process -> transform.

        Args:
            game_id: The BGG game ID to fetch
            game_type: Type of game (default: "boardgame")

        Returns:
            Dictionary in game_features_materialized format, or None if any step fails
        """
        try:
            logger.info(f"Fetching game features for game_id {game_id}")

            # Step 1: Fetch raw response
            response_data = self.fetch_game(game_id)
            if not response_data:
                return None

            # Step 2: Process response
            processed_game = self.process_game(game_id, response_data, game_type)
            if not processed_game:
                return None

            # Step 3: Transform to game_features format
            game_features = self.to_game_features(processed_game)
            if not game_features:
                return None

            logger.info(f"Successfully fetched game features for game_id {game_id}")
            return game_features

        except Exception as e:
            logger.error(f"Failed to fetch game features for game_id {game_id}: {e}")
            return None

    def fetch_multiple_game_features(
        self,
        game_ids: List[int],
        game_type: str = "boardgame"
    ) -> Dict[int, Optional[Dict]]:
        """Fetch multiple games and return them in game_features_materialized format.

        Args:
            game_ids: List of BGG game IDs to fetch
            game_type: Type of games (default: "boardgame")

        Returns:
            Dictionary mapping game_id to game_features data.
            Games that failed will have None as their value.
        """
        results = {}

        logger.info(f"Fetching game features for {len(game_ids)} games")

        for game_id in game_ids:
            try:
                game_features = self.fetch_game_features(game_id, game_type)
                results[game_id] = game_features
            except Exception as e:
                logger.error(f"Failed to fetch game features for game_id {game_id}: {e}")
                results[game_id] = None

        successful = sum(1 for v in results.values() if v is not None)
        logger.info(f"Successfully fetched game features for {successful}/{len(game_ids)} games")

        return results

    def prepare_for_bigquery(self, processed_games: List[Dict]) -> Dict:
        """Prepare processed games for BigQuery loading.

        This uses the BGGDataProcessor's prepare_for_bigquery method to
        transform the processed game data into the format expected by BigQuery.

        Args:
            processed_games: List of processed game dictionaries

        Returns:
            Dictionary containing data organized by table name
        """
        try:
            logger.info(f"Preparing {len(processed_games)} games for BigQuery format")

            prepared_data = self.processor.prepare_for_bigquery(processed_games)

            logger.info("Successfully prepared data for BigQuery")
            return prepared_data

        except Exception as e:
            logger.error(f"Failed to prepare data for BigQuery: {e}")
            raise
