"""Module for processing BGG API responses into BigQuery-compatible format."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BGGDataProcessor:
    """Processes BGG API responses for BigQuery loading."""

    def _extract_name(self, item: Dict[str, Any]) -> str:
        """Extract the primary name of the game.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Primary name of the game
        """
        names = item.get("name", [])
        if isinstance(names, list):
            primary_name = next(
                (name["@value"] for name in names if name.get("@type") == "primary"),
                names[0]["@value"] if names else "Unknown"
            )
        else:
            primary_name = names.get("@value", "Unknown")
        return primary_name

    def _extract_year(self, item: Dict[str, Any]) -> Optional[int]:
        """Extract the publication year.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Publication year or None if not found
        """
        year = item.get("yearpublished", {}).get("@value")
        return int(year) if year and year.isdigit() else None

    def _extract_list_field(self, item: Dict[str, Any], field: str) -> List[Dict[str, Any]]:
        """Extract a list field (categories, mechanics, etc.).
        
        Args:
            item: Game data dictionary
            field: Field name to extract
            
        Returns:
            List of dictionaries with id and name
        """
        values = item.get(field, [])
        if not values:
            return []
        
        if isinstance(values, dict):
            values = [values]
            
        return [
            {"id": int(v.get("@id", 0)), "name": v.get("@value", "Unknown")}
            for v in values 
            if v.get("@value") and v.get("@id", "").isdigit()
        ]

    def _extract_stats(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract statistics from the game data.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Dictionary of statistics
        """
        stats = item.get("statistics", {}).get("ratings", {})
        return {
            "average": float(stats.get("average", {}).get("@value", 0)),
            "num_ratings": int(stats.get("usersrated", {}).get("@value", 0)),
            "owned": int(stats.get("owned", {}).get("@value", 0)),
            "weight": float(stats.get("averageweight", {}).get("@value", 0)),
        }

    def process_game(
        self, 
        game_id: int, 
        api_response: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Process a game's API response data.
        
        Args:
            game_id: ID of the game
            api_response: Raw API response data
            
        Returns:
            Processed game data ready for BigQuery or None if processing fails
        """
        try:
            items = api_response.get("items", {}).get("item", [])
            if not items:
                logger.warning("No items found in API response for game %d", game_id)
                return None

            # Handle single item response
            if isinstance(items, dict):
                items = [items]

            # Find the matching game
            item = next((i for i in items if i.get("@id") == str(game_id)), None)
            if not item:
                logger.warning("Game %d not found in API response", game_id)
                return None

            # Extract basic information
            processed = {
                "game_id": game_id,
                "name": self._extract_name(item),
                "year_published": self._extract_year(item),
                "min_players": int(item.get("minplayers", {}).get("@value", 0)),
                "max_players": int(item.get("maxplayers", {}).get("@value", 0)),
                "playing_time": int(item.get("playingtime", {}).get("@value", 0)),
                "min_age": int(item.get("minage", {}).get("@value", 0)),
                "description": item.get("description", ""),
                "thumbnail": item.get("thumbnail", ""),
                "image": item.get("image", ""),
                "categories": self._extract_list_field(item, "link"),
                "mechanics": self._extract_list_field(item, "link"),
                "families": self._extract_list_field(item, "link"),
                "raw_data": str(api_response),  # Store original response
                "load_timestamp": datetime.utcnow(),
            }

            # Extract statistics if available
            stats = self._extract_stats(item)
            processed.update(stats)

            return processed

        except Exception as e:
            logger.error("Failed to process game %d: %s", game_id, e)
            return None

    def prepare_for_bigquery(
        self, 
        processed_games: List[Dict[str, Any]]
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Prepare processed game data for BigQuery loading.
        
        Args:
            processed_games: List of processed game dictionaries
            
        Returns:
            Tuple of DataFrames (games, categories, mechanics)
        """
        # Create games DataFrame
        games_df = pl.DataFrame(processed_games)

        # Extract unique categories and mechanics
        categories_dict = {}
        mechanics_dict = {}
        
        for game in processed_games:
            for cat in game.get("categories", []):
                if cat["name"] != "Unknown":
                    categories_dict[cat["id"]] = cat["name"]
            for mech in game.get("mechanics", []):
                if mech["name"] != "Unknown":
                    mechanics_dict[mech["id"]] = mech["name"]

        # Create category mappings
        categories_data = []
        for cat_id, name in sorted(categories_dict.items()):
            if name and name != "Unknown":  # Ensure name is valid
                categories_data.append({
                    "category_id": cat_id,
                    "category_name": name
                })
        categories_df = pl.DataFrame(categories_data).with_columns([
            pl.col("category_name").cast(pl.String).fill_null("Unknown")
        ])

        # Create mechanics mappings
        mechanics_data = []
        for mech_id, name in sorted(mechanics_dict.items()):
            if name and name != "Unknown":  # Ensure name is valid
                mechanics_data.append({
                    "mechanic_id": mech_id,
                    "mechanic_name": name
                })
        mechanics_df = pl.DataFrame(mechanics_data).with_columns([
            pl.col("mechanic_name").cast(pl.String).fill_null("Unknown")
        ])

        return games_df, categories_df, mechanics_df

    def validate_data(self, df: pl.DataFrame, table_name: str) -> bool:
        """Validate processed data before loading.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the target table
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check for required columns
            if table_name == "games":
                required_columns = {"game_id", "name", "load_timestamp"}
                if not all(col in df.columns for col in required_columns):
                    logger.error("Missing required columns in games data")
                    return False

            # Check for data types
            for col in df.columns:
                if df[col].dtype == pl.Null:
                    logger.error("Column %s contains all null values", col)
                    return False

            # Check for duplicates in ID columns
            id_columns = {
                "games": "game_id",
                "categories": "category_id",
                "mechanics": "mechanic_id"
            }
            
            if table_name in id_columns:
                id_col = id_columns[table_name]
                if df[id_col].n_unique() != len(df):
                    logger.error("Duplicate IDs found in %s", table_name)
                    return False

            return True

        except Exception as e:
            logger.error("Data validation failed: %s", e)
            return False
