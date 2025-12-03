"""Migration script to backfill fetched_responses and processed_responses from raw_responses.

This populates the new tracking tables with historical data from the existing
raw_responses table.
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


def backfill_fetched_responses(environment: str = None) -> None:
    """Backfill fetched_responses from existing raw_responses data.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]

    source_table = f"{project_id}.{raw_dataset}.raw_responses"
    target_table = f"{project_id}.{raw_dataset}.fetched_responses"

    logger.info(f"Backfilling {target_table} from {source_table}")

    # Insert all existing records from raw_responses into fetched_responses
    backfill_query = f"""
    INSERT INTO `{target_table}` (record_id, game_id, fetch_timestamp, fetch_status)
    SELECT
        record_id,
        game_id,
        fetch_timestamp,
        CASE
            WHEN process_status = 'no_response' THEN 'no_response'
            WHEN process_status = 'parse_error' THEN 'parse_error'
            ELSE 'success'
        END as fetch_status
    FROM `{source_table}`
    WHERE record_id IS NOT NULL
        -- Only insert if not already in target table
        AND NOT EXISTS (
            SELECT 1 FROM `{target_table}` f
            WHERE f.record_id = `{source_table}`.record_id
        )
    """

    try:
        query_job = client.query(backfill_query)
        result = query_job.result()

        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM `{target_table}`"
        count_result = client.query(count_query).result()
        row_count = next(count_result).count

        logger.info(f"Backfill complete. {target_table} now has {row_count} records")

    except Exception as e:
        logger.error(f"Failed to backfill {target_table}: {e}")
        raise


def backfill_processed_responses(environment: str = None) -> None:
    """Backfill processed_responses from existing raw_responses data.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]

    source_table = f"{project_id}.{raw_dataset}.raw_responses"
    target_table = f"{project_id}.{raw_dataset}.processed_responses"

    logger.info(f"Backfilling {target_table} from {source_table}")

    # Insert records that have been processed (processed = TRUE)
    backfill_query = f"""
    INSERT INTO `{target_table}` (record_id, process_timestamp, process_status, process_attempt, error_message)
    SELECT
        record_id,
        COALESCE(process_timestamp, fetch_timestamp) as process_timestamp,
        COALESCE(process_status, 'unknown') as process_status,
        COALESCE(process_attempt, 0) as process_attempt,
        NULL as error_message
    FROM `{source_table}`
    WHERE record_id IS NOT NULL
        AND processed = TRUE
        -- Only insert if not already in target table
        AND NOT EXISTS (
            SELECT 1 FROM `{target_table}` p
            WHERE p.record_id = `{source_table}`.record_id
        )
    """

    try:
        query_job = client.query(backfill_query)
        result = query_job.result()

        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM `{target_table}`"
        count_result = client.query(count_query).result()
        row_count = next(count_result).count

        logger.info(f"Backfill complete. {target_table} now has {row_count} records")

    except Exception as e:
        logger.error(f"Failed to backfill {target_table}: {e}")
        raise


def verify_backfill(environment: str = None) -> None:
    """Verify that backfill was successful.

    Args:
        environment: Optional environment name (dev/prod/test)
    """
    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]

    raw_responses = f"{project_id}.{raw_dataset}.raw_responses"
    fetched_responses = f"{project_id}.{raw_dataset}.fetched_responses"
    processed_responses = f"{project_id}.{raw_dataset}.processed_responses"

    logger.info("Verifying backfill...")

    # Verify fetched_responses
    verify_fetched_query = f"""
    SELECT
        (SELECT COUNT(*) FROM `{raw_responses}` WHERE record_id IS NOT NULL) as raw_count,
        (SELECT COUNT(*) FROM `{fetched_responses}`) as fetched_count
    """
    result = client.query(verify_fetched_query).result()
    row = next(result)
    logger.info(f"Raw responses: {row.raw_count}, Fetched responses: {row.fetched_count}")

    if row.raw_count != row.fetched_count:
        logger.warning(f"Mismatch in fetched_responses count!")
    else:
        logger.info("✓ fetched_responses backfill verified")

    # Verify processed_responses
    verify_processed_query = f"""
    SELECT
        (SELECT COUNT(*) FROM `{raw_responses}` WHERE record_id IS NOT NULL AND processed = TRUE) as processed_raw_count,
        (SELECT COUNT(*) FROM `{processed_responses}`) as processed_count
    """
    result = client.query(verify_processed_query).result()
    row = next(result)
    logger.info(f"Processed in raw: {row.processed_raw_count}, Processed responses: {row.processed_count}")

    if row.processed_raw_count != row.processed_count:
        logger.warning(f"Mismatch in processed_responses count!")
    else:
        logger.info("✓ processed_responses backfill verified")


def main():
    """Main entry point for the backfill."""
    environment = os.environ.get("ENVIRONMENT")
    logger.info(f"Backfilling tracking tables for environment: {environment or 'dev'}")

    backfill_fetched_responses(environment)
    backfill_processed_responses(environment)
    verify_backfill(environment)

    logger.info("Backfill completed successfully")


if __name__ == "__main__":
    main()
