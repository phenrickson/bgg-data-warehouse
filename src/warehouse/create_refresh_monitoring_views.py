"""Create monitoring views for the refresh strategy."""

import logging
import os
from google.cloud import bigquery
from src.config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_refresh_monitoring_views(environment: str = None):
    """Create BigQuery views for monitoring the refresh strategy."""

    config = get_bigquery_config(environment)
    client = bigquery.Client()

    project_id = config["project"]["id"]
    main_dataset = config["project"]["dataset"]
    raw_dataset = config["datasets"]["raw"]

    # Create monitoring dataset if it doesn't exist
    monitoring_dataset = f"{project_id}.monitoring"
    try:
        dataset = bigquery.Dataset(monitoring_dataset)
        dataset.location = config["project"]["location"]
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Monitoring dataset {monitoring_dataset} is ready")
    except Exception as e:
        logger.error(f"Failed to create monitoring dataset: {e}")
        raise

    # Refresh queue view
    refresh_queue_view = f"""
    CREATE OR REPLACE VIEW `{monitoring_dataset}.refresh_queue` AS
    WITH game_years AS (
      SELECT 
        r.game_id,
        g.year_published,
        g.primary_name,
        r.last_refresh_timestamp,
        r.refresh_count,
        EXTRACT(YEAR FROM CURRENT_DATE()) as current_year
      FROM `{project_id}.{raw_dataset}.raw_responses` r
      JOIN `{project_id}.{main_dataset}.games` g ON r.game_id = g.game_id
      WHERE r.processed = TRUE
        AND r.last_refresh_timestamp IS NOT NULL
    ),
    refresh_intervals AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        last_refresh_timestamp,
        refresh_count,
        CASE 
          WHEN year_published > current_year THEN 3  -- Upcoming games
          WHEN year_published = current_year THEN 7  -- Current year
          ELSE LEAST(90, 7 * POW(2, current_year - year_published))  -- Exponential decay
        END as refresh_interval_days
      FROM game_years
    ),
    refresh_status AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        last_refresh_timestamp,
        refresh_count,
        refresh_interval_days,
        TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) as next_due,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 
                      TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY), 
                      HOUR) as hours_overdue
      FROM refresh_intervals
    )
    SELECT 
      year_published,
      COUNT(*) as total_games,
      COUNTIF(next_due <= CURRENT_TIMESTAMP()) as games_due_for_refresh,
      AVG(refresh_interval_days) as avg_refresh_interval_days,
      AVG(CASE WHEN next_due <= CURRENT_TIMESTAMP() THEN hours_overdue END) as avg_hours_overdue
    FROM refresh_status
    GROUP BY year_published
    ORDER BY year_published DESC
    """

    # Refresh activity view
    refresh_activity_view = f"""
    CREATE OR REPLACE VIEW `{monitoring_dataset}.refresh_activity` AS
    WITH daily_activity AS (
      SELECT 
        DATE(r.fetch_timestamp) as fetch_date,
        g.year_published,
        COUNT(*) as responses_fetched,
        COUNT(DISTINCT r.game_id) as unique_games_fetched,
        COUNTIF(r.refresh_count > 0) as refresh_responses,
        COUNTIF(r.refresh_count = 0 OR r.refresh_count IS NULL) as initial_responses
      FROM `{project_id}.{raw_dataset}.raw_responses` r
      JOIN `{project_id}.{main_dataset}.games` g ON r.game_id = g.game_id
      WHERE r.fetch_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      GROUP BY fetch_date, year_published
    )
    SELECT 
      fetch_date,
      year_published,
      responses_fetched,
      unique_games_fetched,
      refresh_responses,
      initial_responses,
      SAFE_DIVIDE(refresh_responses, responses_fetched) as refresh_ratio
    FROM daily_activity
    ORDER BY fetch_date DESC, year_published DESC
    """

    # Games overdue for refresh view
    games_overdue_view = f"""
    CREATE OR REPLACE VIEW `{monitoring_dataset}.games_overdue_for_refresh` AS
    WITH game_years AS (
      SELECT 
        r.game_id,
        g.year_published,
        g.primary_name,
        r.last_refresh_timestamp,
        r.refresh_count,
        EXTRACT(YEAR FROM CURRENT_DATE()) as current_year
      FROM `{project_id}.{raw_dataset}.raw_responses` r
      JOIN `{project_id}.{main_dataset}.games` g ON r.game_id = g.game_id
      WHERE r.processed = TRUE
        AND r.last_refresh_timestamp IS NOT NULL
    ),
    refresh_intervals AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        last_refresh_timestamp,
        refresh_count,
        CASE 
          WHEN year_published > current_year THEN 3  -- Upcoming games
          WHEN year_published = current_year THEN 7  -- Current year
          ELSE LEAST(90, 7 * POW(2, current_year - year_published))  -- Exponential decay
        END as refresh_interval_days
      FROM game_years
    ),
    overdue_games AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        last_refresh_timestamp,
        refresh_count,
        refresh_interval_days,
        TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) as next_due,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 
                      TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY), 
                      HOUR) as hours_overdue
      FROM refresh_intervals
      WHERE TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) <= CURRENT_TIMESTAMP()
    )
    SELECT 
      game_id,
      primary_name,
      year_published,
      last_refresh_timestamp,
      refresh_count,
      refresh_interval_days,
      next_due,
      hours_overdue,
      CASE 
        WHEN hours_overdue < 24 THEN 'Recently Due'
        WHEN hours_overdue < 168 THEN 'Overdue (< 1 week)'
        WHEN hours_overdue < 720 THEN 'Very Overdue (< 1 month)'
        ELSE 'Critically Overdue (> 1 month)'
      END as overdue_category
    FROM overdue_games
    ORDER BY year_published DESC, hours_overdue DESC
    LIMIT 1000
    """

    views = [
        ("refresh_queue", refresh_queue_view),
        ("refresh_activity", refresh_activity_view),
        ("games_overdue_for_refresh", games_overdue_view),
    ]

    for view_name, view_sql in views:
        try:
            client.query(view_sql).result()
            logger.info(f"Created view: {monitoring_dataset}.{view_name}")
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            raise

    logger.info("All refresh monitoring views created successfully")


def main():
    """Main entry point for creating monitoring views."""
    environment = os.environ.get("ENVIRONMENT", "dev")
    logger.info(f"Creating refresh monitoring views for environment: {environment}")
    create_refresh_monitoring_views(environment)


if __name__ == "__main__":
    main()
