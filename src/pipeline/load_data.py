"""Pipeline for loading processed BGG data into BigQuery."""

import logging
from typing import Dict, List, Set

import polars as pl
from google.cloud import bigquery

from src.config import get_bigquery_config
from src.data_processor.processor import BGGDataProcessor

# Get logger
logger = logging.getLogger(__name__)

class DataLoader:
    """Loads processed BGG data into BigQuery."""
    
    def __init__(self, environment: str = None):
        """Initialize BigQuery client and configuration.
        
        Args:
            environment: Optional environment name (dev/prod)
        """
        self.config = get_bigquery_config(environment)
        self.client = bigquery.Client()
        self.processor = BGGDataProcessor()
        
        # Get dataset reference
        project_id = self.config["project"]["id"]
        dataset_id = self.config["project"]["dataset"]
        self.dataset_ref = f"{project_id}.{dataset_id}"

    def _get_table_id(self, table_name: str) -> str:
        """Get fully qualified table ID.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Fully qualified table ID
        """
        return f"{self.dataset_ref}.{table_name}"

    def _delete_existing_game_records(self, table_name: str, game_ids: Set[int]) -> None:
        """Delete existing records for specified games from a table.
        
        Args:
            table_name: Name of the table
            game_ids: Set of game IDs to delete records for
        """
        if not game_ids:
            return
            
        try:
            # Format game IDs for SQL IN clause
            game_ids_str = ", ".join(str(id) for id in game_ids)
            
            # Delete existing records
            query = f"""
            DELETE FROM `{self.dataset_ref}.{table_name}`
            WHERE game_id IN ({game_ids_str})
            """
            
            job = self.client.query(query)
            job.result()  # Wait for job to complete
            
            logger.info(f"Deleted existing records for {len(game_ids)} games from {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete records from {table_name}: {e}")
            raise

    def _load_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        game_ids: Set[int] = None
    ) -> None:
        """Load a DataFrame into BigQuery.
        
        Args:
            df: DataFrame to load
            table_name: Name of the target table
            game_ids: Set of game IDs being loaded (for delete+insert operations)
        """
        if df.height == 0:
            logger.info(f"No data to load for table {table_name}")
            return
            
        try:
            # Determine load type based on table
            time_series_tables = ["games", "rankings"]
            dimension_tables = [
                "categories", "mechanics", "families",
                "designers", "artists", "publishers"
            ]
            
            if table_name in time_series_tables:
                # Append-only for time series data
                write_disposition = "WRITE_APPEND"
            elif table_name in dimension_tables:
                # Full replace for dimension tables
                write_disposition = "WRITE_TRUNCATE"
            else:
                # Delete + Insert for bridge and game-related tables
                if game_ids:
                    self._delete_existing_game_records(table_name, game_ids)
                write_disposition = "WRITE_APPEND"
            
            # Validate data before loading
            if not self.processor.validate_data(df, table_name):
                logger.error(f"Data validation failed for table {table_name}")
                return
                
            # Convert to pandas for BigQuery loading
            pdf = df.to_pandas()
            
            # Load to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition
            )
            
            table_id = self._get_table_id(table_name)
            job = self.client.load_table_from_dataframe(
                pdf, table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            logger.info(f"Loaded {df.height} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise

    def load_games(self, processed_games: List[Dict]) -> None:
        """Load processed game data into BigQuery.
        
        Args:
            processed_games: List of processed game dictionaries
        """
        try:
            # Get set of game IDs being loaded
            game_ids = {game["game_id"] for game in processed_games}
            
            # Prepare data for all tables
            dataframes = self.processor.prepare_for_bigquery(processed_games)
            
            # Load dimension tables first (overwrite existing data)
            dimension_tables = [
                "categories", "mechanics", "families", "designers",
                "artists", "publishers"
            ]
            
            for table_name in dimension_tables:
                if table_name in dataframes:
                    self._load_dataframe(dataframes[table_name], table_name)
            
            # Load bridge tables (delete+insert)
            bridge_tables = [
                "game_categories", "game_mechanics", "game_families",
                "game_designers", "game_artists", "game_publishers"
            ]
            
            for table_name in bridge_tables:
                if table_name in dataframes:
                    self._load_dataframe(dataframes[table_name], table_name, game_ids)
            
            # Load game-related tables (delete+insert)
            game_related_tables = [
                "alternate_names", "player_counts",
                "language_dependence", "suggested_ages"
            ]
            
            for table_name in game_related_tables:
                if table_name in dataframes:
                    self._load_dataframe(dataframes[table_name], table_name, game_ids)
            
            # Load time series tables (append-only)
            time_series_tables = ["games", "rankings"]
            
            for table_name in time_series_tables:
                if table_name in dataframes:
                    self._load_dataframe(dataframes[table_name], table_name)
            
            logger.info("Successfully loaded all game data")
            
        except Exception as e:
            logger.error(f"Failed to load game data: {e}")
            raise

def main():
    """Main entry point for data loading."""
    loader = DataLoader()
    # Example usage:
    # processed_games = [...] # Get processed games from somewhere
    # loader.load_games(processed_games)

if __name__ == "__main__":
    main()
