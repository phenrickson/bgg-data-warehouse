"""Check the status of tracking tables and raw_responses schema in prod."""

import logging
from google.cloud import bigquery
from src.config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_prod_status():
    """Check the current state of prod tables."""
    config = get_bigquery_config("prod")
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]

    raw_responses_table = f"{project_id}.{raw_dataset}.raw_responses"
    fetched_responses_table = f"{project_id}.{raw_dataset}.fetched_responses"
    processed_responses_table = f"{project_id}.{raw_dataset}.processed_responses"

    logger.info("=" * 80)
    logger.info("CHECKING PROD ENVIRONMENT STATUS")
    logger.info("=" * 80)

    # Check raw_responses schema
    logger.info("\n1. RAW_RESPONSES TABLE SCHEMA:")
    logger.info("-" * 80)
    try:
        table = client.get_table(raw_responses_table)
        logger.info(f"Table: {raw_responses_table}")
        logger.info(f"Columns: {[field.name for field in table.schema]}")

        # Check for the columns that should have been removed
        column_names = [field.name for field in table.schema]
        has_processed = "processed" in column_names
        has_process_status = "process_status" in column_names
        has_process_timestamp = "process_timestamp" in column_names
        has_process_attempt = "process_attempt" in column_names

        logger.info(f"\nColumn existence:")
        logger.info(f"  - processed: {has_processed}")
        logger.info(f"  - process_status: {has_process_status}")
        logger.info(f"  - process_timestamp: {has_process_timestamp}")
        logger.info(f"  - process_attempt: {has_process_attempt}")

        if not any([has_processed, has_process_status, has_process_timestamp, has_process_attempt]):
            logger.info("\n✓ Migration Step 5 completed: Old columns have been removed")
        else:
            logger.info("\n✗ Old columns still exist - migration incomplete")

    except Exception as e:
        logger.error(f"Failed to check raw_responses: {e}")

    # Check fetched_responses
    logger.info("\n2. FETCHED_RESPONSES TABLE:")
    logger.info("-" * 80)
    try:
        table = client.get_table(fetched_responses_table)
        logger.info(f"Table: {fetched_responses_table}")
        logger.info(f"✓ Table exists")

        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM `{fetched_responses_table}`"
        result = client.query(count_query).result()
        row_count = next(result).count
        logger.info(f"Row count: {row_count:,}")

        if row_count > 0:
            logger.info("✓ Table has been backfilled")
        else:
            logger.info("✗ Table is empty - needs backfill")

    except Exception as e:
        logger.error(f"✗ Table does not exist or error: {e}")

    # Check processed_responses
    logger.info("\n3. PROCESSED_RESPONSES TABLE:")
    logger.info("-" * 80)
    try:
        table = client.get_table(processed_responses_table)
        logger.info(f"Table: {processed_responses_table}")
        logger.info(f"✓ Table exists")

        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM `{processed_responses_table}`"
        result = client.query(count_query).result()
        row_count = next(result).count
        logger.info(f"Row count: {row_count:,}")

        if row_count > 0:
            logger.info("✓ Table has been backfilled")
        else:
            logger.info("✗ Table is empty - needs backfill")

    except Exception as e:
        logger.error(f"✗ Table does not exist or error: {e}")

    # Compare counts
    logger.info("\n4. ROW COUNT COMPARISON:")
    logger.info("-" * 80)
    try:
        comparison_query = f"""
        SELECT
            (SELECT COUNT(*) FROM `{raw_responses_table}`) as raw_count,
            (SELECT COUNT(*) FROM `{fetched_responses_table}`) as fetched_count,
            (SELECT COUNT(*) FROM `{processed_responses_table}`) as processed_count
        """
        result = client.query(comparison_query).result()
        row = next(result)

        logger.info(f"raw_responses:       {row.raw_count:,} records")
        logger.info(f"fetched_responses:   {row.fetched_count:,} records")
        logger.info(f"processed_responses: {row.processed_count:,} records")

        if row.raw_count == row.fetched_count:
            logger.info("\n✓ fetched_responses fully backfilled")
        else:
            logger.info(f"\n✗ Missing {row.raw_count - row.fetched_count:,} records in fetched_responses")

    except Exception as e:
        logger.error(f"Failed to compare counts: {e}")

    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY:")
    logger.info("=" * 80)
    logger.info("If old columns are removed but tracking tables are empty,")
    logger.info("the backfill script needs to be updated to work without those columns.")
    logger.info("=" * 80)


if __name__ == "__main__":
    check_prod_status()
