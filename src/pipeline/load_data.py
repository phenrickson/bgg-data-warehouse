"""Pipeline for loading processed BGG data into BigQuery."""

import logging
from typing import Dict, List

import polars as pl
from google.cloud import bigquery

from src.config import get_bigquery_config
from src.data_processor.processor import BGGDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """Loads processed BGG data into BigQuery."""
    
    def __init__(self):
        """Initialize BigQuery client and configuration."""
        self.config = get_bigquery_config()
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

    def _load_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> None:
        """Load a DataFrame into BigQuery.
        
        Args:
            df: DataFrame to load
            table_name: Name of the target table
            write_disposition: BigQuery write disposition
        """
        if df.height == 0:
            logger.info(f"No data to load for table {table_name}")
            return
            
        try:
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
            # Prepare data for all tables
            dataframes = self.processor.prepare_for_bigquery(processed_games)
            
            # Load dimension tables first (overwrite existing data)
            dimension_tables = [
                "categories", "mechanics", "families", "designers",
                "artists", "publishers"
            ]
            
            for table_name in dimension_tables:
                if table_name in dataframes:
                    self._load_dataframe(
                        dataframes[table_name],
                        table_name,
                        write_disposition="WRITE_TRUNCATE"
                    )
            
            # Load bridge tables (append new relationships)
            bridge_tables = [
                "game_categories", "game_mechanics", "game_families",
                "game_designers", "game_artists", "game_publishers"
            ]
            
            for table_name in bridge_tables:
                if table_name in dataframes:
                    self._load_dataframe(dataframes[table_name], table_name)
            
            # Load game-specific data (append new data)
            game_tables = [
                "games", "alternate_names", "player_counts",
                "language_dependence", "suggested_ages", "rankings"
            ]
            
            for table_name in game_tables:
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
