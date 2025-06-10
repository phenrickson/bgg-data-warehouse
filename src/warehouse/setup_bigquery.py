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
    
    def __init__(self, environment: str = None):
        """Initialize BigQuery client and configuration.
        
        Args:
            environment: Optional environment name (dev/prod)
        """
        self.config = get_bigquery_config(environment)
        self.client = bigquery.Client()
        self.project_id = self.config["project"]["id"]
        
        # Get dataset references
        self.main_dataset = f"{self.project_id}.{self.config['project']['dataset']}"
        self.raw_dataset = f"{self.project_id}.{self.config['datasets']['raw']}"

    def _get_raw_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get schema for a raw table.
        
        Args:
            table_name: Name of the raw table
            
        Returns:
            List of BigQuery schema fields
        """
        schemas = {
            "thing_ids": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("processed", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("process_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED")
            ],
            "request_log": [
                bigquery.SchemaField("request_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("method", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status_code", "INTEGER"),
                bigquery.SchemaField("response_time", "FLOAT64"),
                bigquery.SchemaField("error", "STRING"),
                bigquery.SchemaField("request_timestamp", "TIMESTAMP", mode="REQUIRED")
            ]
        }
        return schemas.get(table_name, [])

    def _get_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get schema for a warehouse table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of BigQuery schema fields
        """
        schemas = {
            "games": [
                bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
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

    def create_dataset(self, dataset_ref: str) -> None:
        """Create a dataset if it doesn't exist.
        
        Args:
            dataset_ref: Full dataset reference (project.dataset)
        """
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.config["project"]["location"]
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_ref} is ready")
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            raise

    def create_table(self, table_config: Dict, dataset_ref: str, is_raw: bool = False) -> None:
        """Create a BigQuery table with the specified configuration.
        
        Args:
            table_config: Table configuration from bigquery.yaml
            dataset_ref: Full dataset reference (project.dataset)
            is_raw: Whether this is a raw table
        """
        table_id = f"{dataset_ref}.{table_config['name']}"
        schema = self._get_raw_schema(table_config['name']) if is_raw else self._get_schema(table_config['name'])
        
        if not schema:
            logger.error(f"No schema defined for table {table_config['name']}")
            return
            
        try:
            # Check if table exists
            try:
                existing_table = self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                
                # Compare schemas to see if we need to add fields
                existing_fields = {field.name: field for field in existing_table.schema}
                new_fields = {field.name: field for field in schema}
                
                logger.info(f"Existing fields in {table_id}: {list(existing_fields.keys())}")
                logger.info(f"Expected fields in {table_id}: {list(new_fields.keys())}")
                
                # Find fields that need to be added
                fields_to_add = []
                for name, field in new_fields.items():
                    if name not in existing_fields:
                        fields_to_add.append(field)
                        logger.info(f"Field {name} missing from {table_id}")
                
                if fields_to_add:
                    # Add new fields using ALTER TABLE
                    for field in fields_to_add:
                        query = f"""
                        ALTER TABLE `{table_id}`
                        ADD COLUMN IF NOT EXISTS {field.name} {field.field_type}
                        """
                        self.client.query(query).result()
                        logger.info(f"Added field {field.name} to {table_id}")
                return
                
            except NotFound:
                # Create new table
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
                
                self.client.create_table(table)
                logger.info(f"Created new table {table_id}")
            
        except Exception as e:
            logger.error(f"Failed to create table {table_id}: {e}")
            raise

    def setup_warehouse(self) -> None:
        """Set up all required BigQuery resources."""
        try:
            # Create main dataset and tables
            self.create_dataset(self.main_dataset)
            for table_config in self.config["tables"].values():
                self.create_table(table_config, self.main_dataset)
            
            # Create raw dataset and tables
            self.create_dataset(self.raw_dataset)
            for table_config in self.config["raw_tables"].values():
                self.create_table(table_config, self.raw_dataset, is_raw=True)
                
            logger.info("BigQuery setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup warehouse: {e}")
            raise

def main():
    """Main entry point for BigQuery setup."""
    import os
    environment = os.environ.get("ENVIRONMENT")
    logger.info(f"Setting up BigQuery warehouse for environment: {environment or 'dev'}")
    setup = BigQuerySetup(environment)
    setup.setup_warehouse()

if __name__ == "__main__":
    main()
