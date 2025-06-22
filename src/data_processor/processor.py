"""Module for processing BGG API responses into BigQuery-compatible format."""

import logging
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import polars as pl

# Get logger
logger = logging.getLogger(__name__)

class BGGDataProcessor:
    class GameStats:
        """Container for game statistics."""
        def __init__(self, stats: Dict[str, Any]):
            ratings = stats.get("statistics", {}).get("ratings", {})
            
            def safe_int(value: Any) -> int:
                """Safely convert a value to integer."""
                if isinstance(value, int):
                    return value
                if isinstance(value, str):
                    try:
                        val = int(value)
                        return val if val >= 0 else 0
                    except (ValueError, TypeError):
                        return 0
                if isinstance(value, dict):
                    return safe_int(value.get("@value", 0))
                return 0
                
            def safe_float(value: Any) -> float:
                """Safely convert a value to float."""
                if isinstance(value, (int, float)):
                    return float(value)
                if isinstance(value, str):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return 0.0
                if isinstance(value, dict):
                    return safe_float(value.get("@value", 0))
                return 0.0
                
            self.users_rated = safe_int(ratings.get("usersrated", 0))
            self.average = safe_float(ratings.get("average", 0))
            self.bayes_average = safe_float(ratings.get("bayesaverage", 0))
            self.standard_deviation = safe_float(ratings.get("stddev", 0))
            self.median = safe_float(ratings.get("median", 0))
            self.owned = safe_int(ratings.get("owned", 0))
            self.trading = safe_int(ratings.get("trading", 0))
            self.wanting = safe_int(ratings.get("wanting", 0))
            self.wishing = safe_int(ratings.get("wishing", 0))
            self.num_comments = safe_int(ratings.get("numcomments", 0))
            self.num_weights = safe_int(ratings.get("numweights", 0))
            self.average_weight = safe_float(ratings.get("averageweight", 0))

    class GameRanks:
        """Container for game ranking information."""
        def __init__(self, stats: Dict[str, Any]):
            def safe_int(value: Any) -> int:
                """Safely convert a value to integer."""
                if isinstance(value, int):
                    return value
                if isinstance(value, str):
                    try:
                        val = int(value)
                        return val if val >= 0 else 0
                    except (ValueError, TypeError):
                        return 0
                if isinstance(value, dict):
                    return safe_int(value.get("@value", 0))
                return 0
                
            def safe_float(value: Any) -> float:
                """Safely convert a value to float."""
                if isinstance(value, (int, float)):
                    return float(value)
                if isinstance(value, str):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return 0.0
                if isinstance(value, dict):
                    return safe_float(value.get("@value", 0))
                return 0.0
                
            self.ranks = []
            ratings = stats.get("statistics", {}).get("ratings", {})
            ranks = ratings.get("ranks", {}).get("rank", [])
            if not isinstance(ranks, list):
                ranks = [ranks]
            
            for rank in ranks:
                if isinstance(rank, dict) and rank.get("@value") != "Not Ranked":
                    self.ranks.append({
                        "type": rank.get("@type", ""),
                        "name": rank.get("@name", ""),
                        "friendly_name": rank.get("@friendlyname", ""),
                        "value": safe_int(rank.get("@value", 0)),
                        "bayes_average": safe_float(rank.get("@bayesaverage", 0))
                    })
    def _safe_int(self, value: Any) -> int:
        """Safely convert a value to integer.
        
        Args:
            value: Value to convert
            
        Returns:
            Integer value or 0 if conversion fails
        """
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                val = int(value)
                return val if val >= 0 else 0
            except (ValueError, TypeError):
                return 0
        return 0
    
    def _extract_names(self, item: Dict[str, Any]) -> Tuple[str, List[Dict[str, str]]]:
        """Extract primary name and alternate names of the game.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Tuple of (primary_name, list of alternate names)
        """
        names = item.get("name", [])
        if isinstance(names, dict):
            # Single name entry
            name_data = {
                "name": names.get("@value", "Unknown"),
                "type": names.get("@type", "alternate"),
                "sort_index": int(names.get("@sortindex", 1))
            }
            if names.get("@type") == "primary":
                return names.get("@value", "Unknown"), []
            else:
                return "Unknown", [name_data]
        elif isinstance(names, str):
            return "Unknown", [{
                "name": names,
                "type": "alternate",
                "sort_index": 1
            }]
        elif not isinstance(names, list):
            return "Unknown", []
            
        # Handle list of names
        primary_name = "Unknown"
        alternate_names = []
        
        for name in names:
            if isinstance(name, dict):
                name_data = {
                    "name": name.get("@value", "Unknown"),
                    "type": name.get("@type", "alternate"),
                    "sort_index": int(name.get("@sortindex", 1))
                }
                
                if name.get("@type") == "primary":
                    primary_name = name.get("@value", "Unknown")
                else:
                    alternate_names.append(name_data)
            elif isinstance(name, str):
                alternate_names.append({
                    "name": name,
                    "type": "alternate",
                    "sort_index": 1
                })
                
        return primary_name, alternate_names

    def _extract_year(self, item: Dict[str, Any]) -> Optional[int]:
        """Extract the publication year.
        
        Args:
            item: Game data dictionary
            
        Returns:
            Publication year or None if not found
        """
        year = item.get("yearpublished", {})
        if isinstance(year, str):
            return int(year) if year.isdigit() and int(year) > 0 else None
        year_value = year.get("@value")
        return int(year_value) if year_value and year_value.isdigit() and int(year_value) > 0 else None

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
                entity = {
                    "id": int(link.get("@id", 0)),
                    "name": link.get("@value", "Unknown")
                }
                if link_type == "boardgameimplementation":
                    entity["@inbound"] = link.get("@inbound", "false") == "true"
                result[type_mapping[link_type]].append(entity)
                
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
                poll_results = poll.get("results", [])
                if not isinstance(poll_results, list):
                    poll_results = [poll_results]
                    
                for result in poll_results:
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
                    if isinstance(votes, dict):
                        votes = [votes]
                    else:
                        votes = []
                
                for vote in votes:
                    if isinstance(vote, dict):
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
        api_response: Dict[str, Any],
        game_type: str,
        load_timestamp: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Process a game's API response data.
        
        Args:
            game_id: ID of the game
            api_response: Raw API response data
            game_type: Type of the game (boardgame or boardgameexpansion)
            
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
            stats = self.GameStats(item)
            ranks = self.GameRanks(item)

            # Build processed data
            processed = {
                "game_id": game_id,
                "type": game_type,
                "primary_name": primary_name,
                "alternate_names": alternate_names,
                "year_published": self._extract_year(item),
                "min_players": self._safe_int(item.get("minplayers", {}).get("@value", "0")),
                "max_players": self._safe_int(item.get("maxplayers", {}).get("@value", "0")),
                "playing_time": self._safe_int(item.get("playingtime", {}).get("@value", "0")),
                "min_playtime": self._safe_int(item.get("minplaytime", {}).get("@value", "0")),
                "max_playtime": self._safe_int(item.get("maxplaytime", {}).get("@value", "0")),
                "min_age": self._safe_int(item.get("minage", {}).get("@value", "0")),
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
            "load_timestamp": load_timestamp or datetime.now(UTC),
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
                "type": game["type"],
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
                # Use sets to deduplicate relationships
                relationships = set()
                
                for entity in game.get(entity_type, []):
                    # Add to entity set
                    collectors[entity_type].add((entity["id"], entity["name"]))
                    
                    # Special handling for implementations to avoid duplicates
                    if entity_type == "implementations":
                        # Only create bridge record if this game implements the other game
                        # (not if this game is implemented by the other game)
                        if not entity.get("@inbound"):
                            relationships.add((game_id, entity["id"]))
                    else:
                        # For all other entity types, add to relationships set
                        relationships.add((game_id, entity["id"]))
                
                # Convert relationships to bridge records
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
                
                for game_id, entity_id in relationships:
                    bridge_record = {"game_id": game_id}
                    bridge_record[id_mapping[entity_type]] = entity_id
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
                "games": {"game_id", "type", "primary_name", "load_timestamp"},
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
                "games": ["game_id", "load_timestamp"],  # Games table is time series
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
