"""Module for processing BGG API responses into BigQuery-compatible format."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import polars as pl

# Get logger
logger = logging.getLogger(__name__)

class GameStats:
    """Container for game statistics."""
    def __init__(self, stats: Dict[str, Any]):
        ratings = stats.get("statistics", {}).get("ratings", {})
        self.users_rated = int(ratings.get("usersrated", {}).get("@value", 0))
        self.average = float(ratings.get("average", {}).get("@value", 0))
        self.bayes_average = float(ratings.get("bayesaverage", {}).get("@value", 0))
        self.standard_deviation = float(ratings.get("stddev", {}).get("@value", 0))
        self.median = float(ratings.get("median", {}).get("@value", 0))
        self.owned = int(ratings.get("owned", {}).get("@value", 0))
        self.trading = int(ratings.get("trading", {}).get("@value", 0))
        self.wanting = int(ratings.get("wanting", {}).get("@value", 0))
        self.wishing = int(ratings.get("wishing", {}).get("@value", 0))
        self.num_comments = int(ratings.get("numcomments", {}).get("@value", 0))
        self.num_weights = int(ratings.get("numweights", {}).get("@value", 0))
        self.average_weight = float(ratings.get("averageweight", {}).get("@value", 0))

class GameRanks:
    """Container for game ranking information."""
    def __init__(self, stats: Dict[str, Any]):
        self.ranks = []
        ratings = stats.get("statistics", {}).get("ratings", {})
        ranks = ratings.get("ranks", {}).get("rank", [])
        if not isinstance(ranks, list):
            ranks = [ranks]
        
        for rank in ranks:
            if rank.get("@value") != "Not Ranked":
                self.ranks.append({
                    "type": rank.get("@type", ""),
                    "name": rank.get("@name", ""),
                    "friendly_name": rank.get("@friendlyname", ""),
                    "value": int(rank.get("@value", 0)),
                    "bayes_average": float(rank.get("@bayesaverage", 0))
                })

class BGGDataProcessor:
    """Processes BGG API responses for BigQuery loading."""
    
    def _extract_names(self, item: Dict[str, Any]) -> Tuple[str, List[Dict[str, str]]]:
        """Extract primary name and alternate names of the game.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Tuple of (primary_name, list of alternate names)
        """
        names = item.get("name", [])
        if not isinstance(names, list):
            names = [names]
            
        primary_name = "Unknown"
        alternate_names = []
        
        for name in names:
            name_data = {
                "name": name.get("@value", "Unknown"),
                "type": name.get("@type", "alternate"),
                "sort_index": int(name.get("@sortindex", 1))
            }
            
            if name.get("@type") == "primary":
                primary_name = name.get("@value", "Unknown")
            else:
                alternate_names.append(name_data)
                
        return primary_name, alternate_names

    def _extract_year(self, item: Dict[str, Any]) -> Optional[int]:
        """Extract the publication year.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Publication year or None if not found
        """
        year = item.get("yearpublished", {}).get("@value")
        return int(year) if year and year.isdigit() else None

    def _extract_links(self, item: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract all linked entities (categories, mechanics, etc.).
        
        Args:
            item: Game data dictionary
            
        Returns:
            Dictionary of link types to lists of linked entities
        """
        links = item.get("link", [])
        if not links:
            return {}
        
        if isinstance(links, dict):
            links = [links]
            
        result = {
            "categories": [],
            "mechanics": [],
            "families": [],
            "expansions": [],
            "implementations": [],
            "designers": [],
            "artists": [],
            "publishers": []
        }
        
        type_mapping = {
            "boardgamecategory": "categories",
            "boardgamemechanic": "mechanics",
            "boardgamefamily": "families",
            "boardgameexpansion": "expansions",
            "boardgameimplementation": "implementations",
            "boardgamedesigner": "designers",
            "boardgameartist": "artists",
            "boardgamepublisher": "publishers"
        }
        
        for link in links:
            link_type = link.get("@type")
            if link_type in type_mapping:
                result[type_mapping[link_type]].append({
                    "id": int(link.get("@id", 0)),
                    "name": link.get("@value", "Unknown")
                })
                
        return result

    def _extract_poll_results(self, item: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract poll results from the game data.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Dictionary of poll results
        """
        polls = item.get("poll", [])
        if not isinstance(polls, list):
            polls = [polls]
            
        results = {
            "suggested_players": [],
            "language_dependence": [],
            "suggested_age": []
        }
        
        for poll in polls:
            poll_name = poll.get("@name")
            if poll_name == "suggested_numplayers":
                for result in poll.get("results", []):
                    num_players = result.get("@numplayers")
                    votes = result.get("result", [])
                    if not isinstance(votes, list):
                        votes = [votes]
                    
                    results["suggested_players"].append({
                        "player_count": num_players,
                        "best_votes": next((int(v.get("@numvotes", 0)) for v in votes if v.get("@value") == "Best"), 0),
                        "recommended_votes": next((int(v.get("@numvotes", 0)) for v in votes if v.get("@value") == "Recommended"), 0),
                        "not_recommended_votes": next((int(v.get("@numvotes", 0)) for v in votes if v.get("@value") == "Not Recommended"), 0)
                    })
            elif poll_name == "language_dependence":
                votes = poll.get("results", {}).get("result", [])
                if not isinstance(votes, list):
                    votes = [votes]
                
                for vote in votes:
                    results["language_dependence"].append({
                        "level": int(vote.get("@level", 0)),
                        "description": vote.get("@value", ""),
                        "votes": int(vote.get("@numvotes", 0))
                    })
            elif poll_name == "suggested_playerage":
                votes = poll.get("results", {}).get("result", [])
                if not isinstance(votes, list):
                    votes = [votes]
                
                for vote in votes:
                    results["suggested_age"].append({
                        "age": vote.get("@value", ""),
                        "votes": int(vote.get("@numvotes", 0))
                    })
                    
        return results

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

            # Extract names
            primary_name, alternate_names = self._extract_names(item)

            # Extract linked entities
            links = self._extract_links(item)

            # Extract polls
            polls = self._extract_poll_results(item)

            # Extract statistics
            stats = GameStats(item)
            ranks = GameRanks(item)

            # Build processed data
            processed = {
                "game_id": game_id,
                "primary_name": primary_name,
                "alternate_names": alternate_names,
                "year_published": self._extract_year(item),
                "min_players": int(item.get("minplayers", {}).get("@value", 0)),
                "max_players": int(item.get("maxplayers", {}).get("@value", 0)),
                "playing_time": int(item.get("playingtime", {}).get("@value", 0)),
                "min_playtime": int(item.get("minplaytime", {}).get("@value", 0)),
                "max_playtime": int(item.get("maxplaytime", {}).get("@value", 0)),
                "min_age": int(item.get("minage", {}).get("@value", 0)),
                "description": item.get("description", ""),
                "thumbnail": item.get("thumbnail", ""),
                "image": item.get("image", ""),
                
                # Linked entities
                "categories": links.get("categories", []),
                "mechanics": links.get("mechanics", []),
                "families": links.get("families", []),
                "expansions": links.get("expansions", []),
                "implementations": links.get("implementations", []),
                "designers": links.get("designers", []),
                "artists": links.get("artists", []),
                "publishers": links.get("publishers", []),
                
                # Polls
                "suggested_players": polls.get("suggested_players", []),
                "language_dependence": polls.get("language_dependence", []),
                "suggested_age": polls.get("suggested_age", []),
                
                # Statistics
                "users_rated": stats.users_rated,
                "average_rating": stats.average,
                "bayes_average": stats.bayes_average,
                "standard_deviation": stats.standard_deviation,
                "median_rating": stats.median,
                "owned_count": stats.owned,
                "trading_count": stats.trading,
                "wanting_count": stats.wanting,
                "wishing_count": stats.wishing,
                "num_comments": stats.num_comments,
                "num_weights": stats.num_weights,
                "average_weight": stats.average_weight,
                
                # Rankings
                "rankings": ranks.ranks,
                
                # Metadata
                "raw_data": str(api_response),
                "load_timestamp": datetime.utcnow(),
            }

            return processed

        except Exception as e:
            logger.error("Failed to process game %d: %s", game_id, e)
            return None

    def prepare_for_bigquery(
        self, 
        processed_games: List[Dict[str, Any]]
    ) -> Dict[str, pl.DataFrame]:
        """Prepare processed game data for BigQuery loading.
        
        Args:
            processed_games: List of processed game dictionaries
            
        Returns:
            Dictionary of table names to DataFrames
        """
        # Initialize collectors for all entities
        collectors = {
            "games": [],
            "alternate_names": [],
            "categories": set(),
            "mechanics": set(),
            "families": set(),
            "expansions": set(),
            "implementations": set(),
            "designers": set(),
            "artists": set(),
            "publishers": set(),
            "game_categories": [],
            "game_mechanics": [],
            "game_families": [],
            "game_expansions": [],
            "game_implementations": [],
            "game_designers": [],
            "game_artists": [],
            "game_publishers": [],
            "player_counts": [],
            "language_dependence": [],
            "suggested_ages": [],
            "rankings": []
        }
        
        # Process each game
        for game in processed_games:
            game_id = game["game_id"]
            
            # Basic game info
            collectors["games"].append({
                "game_id": game_id,
                "primary_name": game["primary_name"],
                "year_published": game["year_published"],
                "min_players": game["min_players"],
                "max_players": game["max_players"],
                "playing_time": game["playing_time"],
                "min_playtime": game["min_playtime"],
                "max_playtime": game["max_playtime"],
                "min_age": game["min_age"],
                "description": game["description"],
                "thumbnail": game["thumbnail"],
                "image": game["image"],
                "users_rated": game["users_rated"],
                "average_rating": game["average_rating"],
                "bayes_average": game["bayes_average"],
                "standard_deviation": game["standard_deviation"],
                "median_rating": game["median_rating"],
                "owned_count": game["owned_count"],
                "trading_count": game["trading_count"],
                "wanting_count": game["wanting_count"],
                "wishing_count": game["wishing_count"],
                "num_comments": game["num_comments"],
                "num_weights": game["num_weights"],
                "average_weight": game["average_weight"],
                "raw_data": game["raw_data"],
                "load_timestamp": game["load_timestamp"]
            })
            
            # Alternate names
            for name in game["alternate_names"]:
                collectors["alternate_names"].append({
                    "game_id": game_id,
                    "name": name["name"],
                    "sort_index": name["sort_index"]
                })
            
            # Process linked entities and create bridge tables
            for entity_type in ["categories", "mechanics", "families", "expansions", 
                              "implementations", "designers", "artists", "publishers"]:
                for entity in game.get(entity_type, []):
                    # Add to entity set
                    collectors[entity_type].add((entity["id"], entity["name"]))
                    # Add to bridge table with correct column names
                    bridge_record = {"game_id": game_id}
                    
                    # Map entity type to correct ID column name
                    id_mapping = {
                        "categories": "category_id",
                        "mechanics": "mechanic_id",
                        "families": "family_id",
                        "expansions": "expansion_id",
                        "implementations": "implementation_id",
                        "designers": "designer_id",
                        "artists": "artist_id",
                        "publishers": "publisher_id"
                    }
                    
                    bridge_record[id_mapping[entity_type]] = entity["id"]
                    collectors[f"game_{entity_type}"].append(bridge_record)
            
            # Player counts
            for count in game["suggested_players"]:
                collectors["player_counts"].append({
                    "game_id": game_id,
                    "player_count": count["player_count"],
                    "best_votes": count["best_votes"],
                    "recommended_votes": count["recommended_votes"],
                    "not_recommended_votes": count["not_recommended_votes"]
                })
            
            # Language dependence
            for lang in game["language_dependence"]:
                collectors["language_dependence"].append({
                    "game_id": game_id,
                    "level": lang["level"],
                    "description": lang["description"],
                    "votes": lang["votes"]
                })
            
            # Suggested ages
            for age in game["suggested_age"]:
                collectors["suggested_ages"].append({
                    "game_id": game_id,
                    "age": age["age"],
                    "votes": age["votes"]
                })
            
            # Rankings
            for rank in game["rankings"]:
                collectors["rankings"].append({
                    "game_id": game_id,
                    "ranking_type": rank["type"],
                    "ranking_name": rank["name"],
                    "friendly_name": rank["friendly_name"],
                    "value": rank["value"],
                    "bayes_average": rank["bayes_average"],
                    "load_timestamp": game["load_timestamp"]  # Add timestamp to make each ranking unique
                })
        
        # Convert collectors to DataFrames
        dataframes = {}
        
        # Main tables
        dataframes["games"] = pl.DataFrame(collectors["games"])
        dataframes["alternate_names"] = pl.DataFrame(collectors["alternate_names"])
        dataframes["player_counts"] = pl.DataFrame(collectors["player_counts"])
        dataframes["language_dependence"] = pl.DataFrame(collectors["language_dependence"])
        dataframes["suggested_ages"] = pl.DataFrame(collectors["suggested_ages"])
        dataframes["rankings"] = pl.DataFrame(collectors["rankings"])
        
        # Entity and bridge tables
        for entity_type in ["categories", "mechanics", "families", "expansions",
                          "implementations", "designers", "artists", "publishers"]:
            # Entity table
            entity_data = []
            for id, name in collectors[entity_type]:
                # Convert entity_type to proper column name format
                id_col = f"{entity_type[:-1]}_id"
                if entity_type == "categories":
                    id_col = "category_id"
                elif entity_type == "mechanics":
                    id_col = "mechanic_id"
                elif entity_type == "families":
                    id_col = "family_id"
                elif entity_type == "designers":
                    id_col = "designer_id"
                elif entity_type == "artists":
                    id_col = "artist_id"
                elif entity_type == "publishers":
                    id_col = "publisher_id"
                
                entity_data.append({
                    id_col: id,
                    "name": name
                })
            dataframes[entity_type] = pl.DataFrame(entity_data)
            
            # Bridge table
            bridge_data = collectors[f"game_{entity_type}"]
            if bridge_data:
                dataframes[f"game_{entity_type}"] = pl.DataFrame(bridge_data)
        
        return dataframes

    def validate_data(self, df: pl.DataFrame, table_name: str) -> bool:
        """Validate processed data before loading.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the target table
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Define required columns for each table
            required_columns = {
                "games": {"game_id", "primary_name", "load_timestamp"},
                "alternate_names": {"game_id", "name"},
                "categories": {"category_id", "name"},
                "mechanics": {"mechanic_id", "name"},
                "families": {"family_id", "name"},
                "expansions": {"expansion_id", "name"},
                "implementations": {"implementation_id", "name"},
                "designers": {"designer_id", "name"},
                "artists": {"artist_id", "name"},
                "publishers": {"publisher_id", "name"},
                "game_categories": {"game_id", "category_id"},
                "game_mechanics": {"game_id", "mechanic_id"},
                "game_families": {"game_id", "family_id"},
                "game_expansions": {"game_id", "expansion_id"},
                "game_implementations": {"game_id", "implementation_id"},
                "game_designers": {"game_id", "designer_id"},
                "game_artists": {"game_id", "artist_id"},
                "game_publishers": {"game_id", "publisher_id"},
                "player_counts": {"game_id", "player_count"},
                "language_dependence": {"game_id", "level", "description"},
                "suggested_ages": {"game_id", "age", "votes"},
                "rankings": {"game_id", "ranking_type", "value"}
            }

            # Check for required columns
            if table_name in required_columns:
                if not all(col in df.columns for col in required_columns[table_name]):
                    logger.error(f"Missing required columns in {table_name} data")
                    return False

            # Check for data types
            for col in df.columns:
                if df[col].dtype == pl.Null:
                    logger.error(f"Column {col} contains all null values in {table_name}")
                    return False

            # Check for duplicates in primary key columns
            pk_columns = {
                "games": ["game_id"],
                "categories": ["category_id"],
                "mechanics": ["mechanic_id"],
                "families": ["family_id"],
                "expansions": ["expansion_id"],
                "implementations": ["implementation_id"],
                "designers": ["designer_id"],
                "artists": ["artist_id"],
                "publishers": ["publisher_id"],
                "game_categories": ["game_id", "category_id"],
                "game_mechanics": ["game_id", "mechanic_id"],
                "game_families": ["game_id", "family_id"],
                "game_expansions": ["game_id", "expansion_id"],
                "game_implementations": ["game_id", "implementation_id"],
                "game_designers": ["game_id", "designer_id"],
                "game_artists": ["game_id", "artist_id"],
                "game_publishers": ["game_id", "publisher_id"],
                "player_counts": ["game_id", "player_count"],
                "language_dependence": ["game_id", "level"],
                "suggested_ages": ["game_id", "age"],
                "rankings": ["game_id", "ranking_type", "ranking_name"]
            }
            
            if table_name in pk_columns:
                pk_cols = pk_columns[table_name]
                if len(pk_cols) == 1:
                    if df[pk_cols[0]].n_unique() != len(df):
                        logger.error(f"Duplicate primary keys found in {table_name}")
                        return False
                else:
                    # For composite keys, check uniqueness of combined columns
                    unique_combinations = df.select(pk_cols).unique()
                    if len(unique_combinations) != len(df):
                        logger.error(f"Duplicate composite keys found in {table_name}")
                        return False

            return True

        except Exception as e:
            logger.error("Data validation failed: %s", e)
            return False
