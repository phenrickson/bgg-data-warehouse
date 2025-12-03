"""Migration script to create fetched_responses and processed_responses tracking tables.

These tables track the lifecycle of responses without requiring UPDATE operations
on the streaming buffer in raw_responses.
"""

import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.config import get_bigquery_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_fetched_responses_table(environment: str = None) -> None:
    """Create the fetched_responses tracking table.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_id = f"{project_id}.{raw_dataset}.fetched_responses"

    # Define schema for fetched_responses tracking table
    schema = [
        bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("fetch_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("fetch_status", "STRING", mode="REQUIRED"),  # 'success', 'no_response', 'parse_error'
    ]

    try:
        # Check if table already exists
        try:
            existing_table = client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
            return
        except NotFound:
            # Create the table
            table = bigquery.Table(table_id, schema=schema)
            table.description = "Tracks fetched API responses from BGG"

            # Add clustering on record_id for efficient lookups
            table.clustering_fields = ["record_id", "game_id"]

            # Add partitioning by fetch_timestamp for efficient queries
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="fetch_timestamp"
            )

            client.create_table(table)
            logger.info(f"Created table {table_id}")
            logger.info(f"Schema: {schema}")

    except Exception as e:
        logger.error(f"Failed to create table {table_id}: {e}")
        raise


def create_processed_responses_table(environment: str = None) -> None:
    """Create the processed_responses tracking table.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_id = f"{project_id}.{raw_dataset}.processed_responses"

    # Define schema for processed_responses tracking table
    schema = [
        bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("process_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("process_status", "STRING", mode="REQUIRED"),  # 'success', 'failed', 'error'
        bigquery.SchemaField("process_attempt", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING"),
    ]

    try:
        # Check if table already exists
        try:
            existing_table = client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
            return
        except NotFound:
            # Create the table
            table = bigquery.Table(table_id, schema=schema)
            table.description = "Tracks processed raw responses without updating streaming buffer"

            # Add clustering on record_id for efficient lookups
            table.clustering_fields = ["record_id"]

            # Add partitioning by process_timestamp for efficient queries
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="process_timestamp"
            )

            client.create_table(table)
            logger.info(f"Created table {table_id}")
            logger.info(f"Schema: {schema}")

    except Exception as e:
        logger.error(f"Failed to create table {table_id}: {e}")
        raise


def main():
    """Main entry point for the migration."""
    environment = os.environ.get("ENVIRONMENT")
    logger.info(f"Creating tracking tables for environment: {environment or 'dev'}")

    create_fetched_responses_table(environment)
    create_processed_responses_table(environment)

    logger.info("Migration completed successfully")


if __name__ == "__main__":
    main()
