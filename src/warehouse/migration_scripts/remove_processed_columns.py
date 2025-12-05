"""Migration script to remove processed-related columns from raw_responses.

These columns are now tracked in the processed_responses table instead.
"""

import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import get_bigquery_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_processed_columns(environment: str = None) -> None:
    """Remove processed, process_timestamp, process_status, and process_attempt columns.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_id = f"{project_id}.{raw_dataset}.raw_responses"

    columns_to_remove = [
        "processed",
        "process_timestamp",
        "process_status",
        "process_attempt"
    ]

    try:
        # Get current table
        table = client.get_table(table_id)
        logger.info(f"Current columns: {[field.name for field in table.schema]}")

        # Remove each column
        for column in columns_to_remove:
            # Check if column exists
            if any(field.name == column for field in table.schema):
                drop_query = f"""
                ALTER TABLE `{table_id}`
                DROP COLUMN IF EXISTS {column}
                """
                logger.info(f"Dropping column: {column}")
                client.query(drop_query).result()
                logger.info(f"Successfully dropped column: {column}")
            else:
                logger.info(f"Column {column} does not exist, skipping")

        # Verify the changes
        table = client.get_table(table_id)
        remaining_columns = [field.name for field in table.schema]
        logger.info(f"Remaining columns: {remaining_columns}")

        # Check that processed columns are gone
        for column in columns_to_remove:
            if column in remaining_columns:
                logger.error(f"Column {column} still exists!")
                return False

        logger.info("✓ All processed-related columns removed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to remove columns from {table_id}: {e}")
        raise


def main():
    """Main entry point for the migration."""
    environment = os.environ.get("ENVIRONMENT")
    logger.info(f"Removing processed columns for environment: {environment or 'dev'}")

    remove_processed_columns(environment)

    logger.info("Migration completed successfully")


if __name__ == "__main__":
    main()
