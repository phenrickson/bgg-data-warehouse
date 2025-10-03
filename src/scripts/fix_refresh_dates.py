"""Script to fix incorrect refresh dates in raw_responses table."""

import logging
import os
import argparse
from dotenv import load_dotenv
from google.cloud import bigquery
from src.config import get_bigquery_config, get_refresh_config

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_refresh_dates(environment: str = None):
    """Fix incorrect refresh dates in raw_responses table."""

    config = get_bigquery_config(environment)
    refresh_config = get_refresh_config()
    client = bigquery.Client()

    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_name = config["raw_tables"]["raw_responses"]["name"]
    table_id = f"{project_id}.{raw_dataset}.{table_name}"

    logger.info(f"Fixing refresh dates in {table_id}")

    # First, let's see how many records have incorrect dates
    check_query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN next_refresh_due < last_refresh_timestamp THEN 1 END) as invalid_dates,
        COUNT(CASE WHEN next_refresh_due > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY) THEN 1 END) as far_future_dates,
        COUNT(CASE WHEN last_refresh_timestamp IS NULL THEN 1 END) as null_refresh_timestamps
    FROM `{table_id}`
    """

    try:
        result = client.query(check_query).result()
        for row in result:
            logger.info(f"Found {row.total_records} total records")
            logger.info(f"Found {row.invalid_dates} records with next_refresh_due < last_refresh_timestamp")
            logger.info(f"Found {row.far_future_dates} records with dates more than 1 year in future")
            logger.info(f"Found {row.null_refresh_timestamps} records with null refresh timestamps")
    except Exception as e:
        logger.error(f"Failed to check existing data: {e}")
        raise

    # Fix records by recalculating refresh dates properly
    fix_query = f"""
    UPDATE `{table_id}` AS r
    SET 
        last_refresh_timestamp = CURRENT_TIMESTAMP(),
        next_refresh_due = TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(),
            INTERVAL LEAST(
                {refresh_config['max_interval_days']},  -- max_interval_days
                CAST({refresh_config['base_interval_days']} * POW({refresh_config['decay_factor']}, 
                    GREATEST(0, EXTRACT(YEAR FROM CURRENT_DATE()) - COALESCE(g.year_published, EXTRACT(YEAR FROM CURRENT_DATE())))
                ) AS INT64)
            ) DAY)
    FROM `{config['project']['id']}.{config['project']['dataset']}.games` AS g
    WHERE r.game_id = g.game_id 
      AND (
