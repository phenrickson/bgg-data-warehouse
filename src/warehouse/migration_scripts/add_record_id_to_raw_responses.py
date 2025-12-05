"""Migration script to add record_id column to raw_responses table."""

import logging
import uuid
from google.cloud import bigquery
from src.config import get_bigquery_config

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def add_record_id_column(environment=None):
    """Add record_id column to raw_responses table and populate existing records."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        # Get the raw dataset configuration
        raw_dataset = config["datasets"]["raw"]
        project_id = config["project"]["id"]
        table_id = f"{project_id}.{raw_dataset}.raw_responses"

        logger.info(f"Adding record_id column to table: {table_id}")

        # Step 1: Add the record_id column
        add_column_sql = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS record_id STRING
        """

        logger.info("Adding record_id column...")
        query_job = client.query(add_column_sql)
        query_job.result()
        logger.info("Successfully added record_id column")

        # Step 1.5: Set default value for future inserts
        set_default_sql = f"""
        ALTER TABLE `{table_id}`
        ALTER COLUMN record_id SET DEFAULT GENERATE_UUID()
        """

        logger.info("Setting default value for record_id column...")
        query_job = client.query(set_default_sql)
        query_job.result()
        logger.info("Successfully set default value for record_id")

        # Step 2: Check if we need to populate existing records
        check_null_sql = f"""
        SELECT COUNT(*) as null_count
        FROM `{table_id}`
        WHERE record_id IS NULL
        """

        query_job = client.query(check_null_sql)
        result = next(query_job.result())
        null_count = result.null_count

        if null_count > 0:
            logger.info(f"Found {null_count} records with NULL record_id, populating...")

            # Step 3: Populate existing records with UUIDs
            # We'll use a combination of game_id and fetch_timestamp to create deterministic UUIDs
            populate_sql = f"""
            UPDATE `{table_id}`
            SET record_id = GENERATE_UUID()
            WHERE record_id IS NULL
            """

            logger.info("Populating record_id for existing records...")
            query_job = client.query(populate_sql)
            query_job.result()
            logger.info(f"Successfully populated record_id for {null_count} existing records")

        # Step 4: Verify the update
        verify_sql = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(record_id) as records_with_id,
            COUNT(*) - COUNT(record_id) as records_without_id
        FROM `{table_id}`
        """

        query_job = client.query(verify_sql)
        result = next(query_job.result())

        logger.info(f"Verification results:")
        logger.info(f"  Total records: {result.total_records}")
        logger.info(f"  Records with record_id: {result.records_with_id}")
        logger.info(f"  Records without record_id: {result.records_without_id}")

        if result.records_without_id > 0:
            logger.error(
                f"Migration incomplete: {result.records_without_id} records still missing record_id"
            )
            return False

        logger.info("Migration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def verify_clustering_update(environment=None):
    """Verify that the table clustering has been updated."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        raw_dataset = config["datasets"]["raw"]
        project_id = config["project"]["id"]
        table_id = f"{project_id}.{raw_dataset}.raw_responses"

        # Get table metadata
        table = client.get_table(table_id)

        logger.info(f"Current clustering fields: {table.clustering_fields}")

        # Check if record_id is in clustering fields
        if table.clustering_fields and "record_id" in table.clustering_fields:
            logger.info("✓ Table is properly clustered with record_id")
        else:
            logger.warning("⚠ Table clustering may need to be updated")
            logger.warning("Note: You may need to recreate the table to update clustering")
            logger.warning(
                "Current clustering will still work, but record_id clustering would be optimal"
            )

    except Exception as e:
        logger.error(f"Error checking clustering: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add record_id column to raw_responses table")
    parser.add_argument(
        "--environment",
        choices=["dev", "test", "prod"],
        help="Environment to migrate (default: from config)",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify clustering, don't run migration",
    )

    args = parser.parse_args()

    if args.verify_only:
        verify_clustering_update(args.environment)
    else:
        success = add_record_id_column(args.environment)
        if success:
            verify_clustering_update(args.environment)
