"""Migration script to add refresh tracking columns to raw_responses table."""

import logging
import os
import argparse
from dotenv import load_dotenv
from google.cloud import bigquery
from src.config import get_bigquery_config

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_refresh_columns(environment: str = None):
    """Add refresh tracking columns to raw_responses table."""

    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_name = config["raw_tables"]["raw_responses"]["name"]
    table_id = f"{project_id}.{raw_dataset}.{table_name}"

    logger.info(f"Adding refresh columns to {table_id}")

    # Add the new columns
    alter_queries = [
        f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS last_refresh_timestamp TIMESTAMP
        """,
        f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS refresh_count INTEGER
        """,
        f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS next_refresh_due TIMESTAMP
        """,
    ]

    for query in alter_queries:
        try:
            client.query(query).result()
            logger.info("Successfully added column")
        except Exception as e:
            logger.error(f"Failed to add column: {e}")
            raise

    # Backfill existing data
    logger.info("Backfilling existing data...")

    backfill_query = f"""
    UPDATE `{table_id}`
    SET 
        last_refresh_timestamp = fetch_timestamp,
        refresh_count = 0,
        next_refresh_due = TIMESTAMP_ADD(fetch_timestamp, INTERVAL 90 DAY)
    WHERE last_refresh_timestamp IS NULL
    """

    try:
        job = client.query(backfill_query)
        result = job.result()
        logger.info(f"Backfilled {job.num_dml_affected_rows} rows")
    except Exception as e:
        logger.error(f"Failed to backfill data: {e}")
        raise

    logger.info("Migration completed successfully")


def main():
    """Main entry point for the migration."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Add refresh columns to raw_responses table")
    parser.add_argument(
        "--environment", "-e", default="dev", help="Specify the environment (default: dev)"
    )

    # Parse arguments
    args = parser.parse_args()

    # Log and run migration
    logger.info(f"Running migration for environment: {args.environment}")
    add_refresh_columns(args.environment)


if __name__ == "__main__":
    main()
