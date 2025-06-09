"""BigQuery setup for the BGG data warehouse."""

import logging
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQuerySetup:
    """Sets up BigQuery tables for the BGG data warehouse."""
    
    def __init__(self):
        """Initialize BigQuery client and configuration."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client()
        
        # Get dataset reference
        project_id = self.config["project"]["id"]
        dataset_id = self.config["project"]["dataset"]
        self.dataset_ref = f"{project_id}.{dataset_id}"

    def _get_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of BigQuery schema fields
        """
        schemas = {
            "games": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("primary_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("year_published", "INTEGER"),
                bigquery.SchemaField("min_players", "INTEGER"),
                bigquery.SchemaField("max_players", "INTEGER"),
                bigquery.SchemaField("playing_time", "INTEGER"),
                bigquery.SchemaField("min_playtime", "INTEGER"),
                bigquery.SchemaField("max_playtime", "INTEGER"),
                bigquery.SchemaField("min_age", "INTEGER"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("thumbnail", "STRING"),
                bigquery.SchemaField("image", "STRING"),
                bigquery.SchemaField("users_rated", "INTEGER"),
                bigquery.SchemaField("average_rating", "FLOAT64"),
                bigquery.SchemaField("bayes_average", "FLOAT64"),
                bigquery.SchemaField("standard_deviation", "FLOAT64"),
                bigquery.SchemaField("median_rating", "FLOAT64"),
                bigquery.SchemaField("owned_count", "INTEGER"),
                bigquery.SchemaField("trading_count", "INTEGER"),
                bigquery.SchemaField("wanting_count", "INTEGER"),
                bigquery.SchemaField("wishing_count", "INTEGER"),
                bigquery.SchemaField("num_comments", "INTEGER"),
                bigquery.SchemaField("num_weights", "INTEGER"),
                bigquery.SchemaField("average_weight", "FLOAT64"),
                bigquery.SchemaField("raw_data", "STRING"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED")
            ],
            "alternate_names": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sort_index", "INTEGER")
            ],
            "categories": [
                bigquery.SchemaField("category_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "mechanics": [
                bigquery.SchemaField("mechanic_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "families": [
                bigquery.SchemaField("family_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "designers": [
                bigquery.SchemaField("designer_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "artists": [
                bigquery.SchemaField("artist_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "publishers": [
                bigquery.SchemaField("publisher_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED")
            ],
            "game_categories": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("category_id", "INTEGER", mode="REQUIRED")
            ],
            "game_mechanics": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("mechanic_id", "INTEGER", mode="REQUIRED")
            ],
            "game_families": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("family_id", "INTEGER", mode="REQUIRED")
            ],
            "game_designers": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("designer_id", "INTEGER", mode="REQUIRED")
            ],
            "game_artists": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("artist_id", "INTEGER", mode="REQUIRED")
            ],
            "game_publishers": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("publisher_id", "INTEGER", mode="REQUIRED")
            ],
            "player_counts": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("player_count", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("best_votes", "INTEGER"),
                bigquery.SchemaField("recommended_votes", "INTEGER"),
                bigquery.SchemaField("not_recommended_votes", "INTEGER")
            ],
            "language_dependence": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("level", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("votes", "INTEGER")
            ],
            "suggested_ages": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("age", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("votes", "INTEGER")
            ],
            "rankings": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("ranking_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("ranking_name", "STRING", mode="REQUIRED"),  # Now part of composite key
                bigquery.SchemaField("friendly_name", "STRING"),
                bigquery.SchemaField("value", "INTEGER"),
                bigquery.SchemaField("bayes_average", "FLOAT64"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED")
            ]
        }
        
        return schemas.get(table_name, [])

    def create_dataset(self) -> None:
        """Create the dataset if it doesn't exist."""
        try:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = self.config["project"]["location"]
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_ref} is ready")
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            raise

    def create_table(self, table_config: Dict) -> None:
        """Create a BigQuery table with the specified configuration.
        
        Args:
            table_config: Table configuration from bigquery.yaml
        """
        table_id = f"{self.dataset_ref}.{table_config['name']}"
        schema = self._get_schema(table_config['name'])
        
        if not schema:
            logger.error(f"No schema defined for table {table_config['name']}")
            return
            
        try:
            table = bigquery.Table(table_id, schema=schema)
            table.description = table_config.get('description', '')
            
            # Configure partitioning if specified
            if 'time_partitioning' in table_config:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_config['time_partitioning']
                )
            
            # Configure clustering if specified
            if 'clustering_fields' in table_config:
                table.clustering_fields = table_config['clustering_fields']
            
            self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {table_id} is ready")
            
        except Exception as e:
            logger.error(f"Failed to create table {table_id}: {e}")
            raise

    def setup_warehouse(self) -> None:
        """Set up all required BigQuery resources."""
        try:
            # Create dataset
            self.create_dataset()
            
            # Create all tables
            for table_config in self.config["tables"].values():
                self.create_table(table_config)
                
            logger.info("BigQuery setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup warehouse: {e}")
            raise

def main():
    """Main entry point for BigQuery setup."""
    setup = BigQuerySetup()
    setup.setup_warehouse()

if __name__ == "__main__":
    main()
