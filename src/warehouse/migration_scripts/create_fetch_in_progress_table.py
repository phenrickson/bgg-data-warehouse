"""Migration script to create the fetch_in_progress table."""

import logging

from google.cloud import bigquery

from ...config import get_bigquery_config
from ...utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def create_fetch_in_progress_table() -> None:
    """Create the fetch_in_progress table in BigQuery."""
    try:
        # Get configuration
        config = get_bigquery_config()

        # Initialize BigQuery client
        client = bigquery.Client()

        # Construct table reference
        table_id = f"{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['fetch_in_progress']['name']}"

        # Define table schema
        schema = [
            bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("fetch_start_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]

        # Create table
        table = bigquery.Table(table_id, schema=schema)

        # Set clustering fields
        table.clustering_fields = ["game_id"]

        # Create the table
        table = client.create_table(table, exists_ok=True)
        logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    except Exception as e:
        logger.error(f"Failed to create fetch_in_progress table: {e}")
        raise


def main() -> None:
    """Main entry point for the migration script."""
    create_fetch_in_progress_table()


if __name__ == "__main__":
    main()
