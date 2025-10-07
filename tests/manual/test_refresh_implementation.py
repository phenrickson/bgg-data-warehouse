"""Test script for validating the refresh strategy implementation."""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import get_bigquery_config, get_refresh_config
from src.pipeline.fetch_responses import BGGResponseFetcher

# Load environment variables
load_dotenv()

# Explicitly set the credentials path
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_test_data(client, config):
    """Set up test data with controlled refresh timestamps."""
    # Get table references
    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_name = config["raw_tables"]["raw_responses"]["name"]
    table_id = f"{project_id}.{raw_dataset}.{table_name}"

    # Update a subset of games with controlled refresh timestamps
    current_year = datetime.now().year

    # Set up different refresh patterns:
    # 1. Current year games - refreshed recently
    # 2. Last year games - due for refresh
    # 3. Older games - some due, some not

    update_queries = [
        # Current year games - refreshed recently
        f"""
        UPDATE `{table_id}`
        SET 
            last_refresh_timestamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
            refresh_count = 1
        WHERE game_id IN (
            SELECT r.game_id 
            FROM `{table_id}` r
            JOIN `{project_id}.{config["project"]["dataset"]}.{config["tables"]["games"]["name"]}` g
            ON r.game_id = g.game_id
            WHERE g.year_published = {current_year}
            LIMIT 5
        )
        """,
        # Last year games - due for refresh
        f"""
        UPDATE `{table_id}`
        SET 
            last_refresh_timestamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY),
            refresh_count = 1
        WHERE game_id IN (
            SELECT r.game_id 
            FROM `{table_id}` r
            JOIN `{project_id}.{config["project"]["dataset"]}.{config["tables"]["games"]["name"]}` g
            ON r.game_id = g.game_id
            WHERE g.year_published = {current_year - 1}
            LIMIT 5
        )
        """,
        # Older games - mixed refresh status
        f"""
        UPDATE `{table_id}`
        SET 
            last_refresh_timestamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 DAY),
            refresh_count = 1
        WHERE game_id IN (
            SELECT r.game_id 
            FROM `{table_id}` r
            JOIN `{project_id}.{config["project"]["dataset"]}.{config["tables"]["games"]["name"]}` g
            ON r.game_id = g.game_id
            WHERE g.year_published < {current_year - 1}
            LIMIT 5
        )
        """,
    ]

    for query in update_queries:
        try:
            client.query(query).result()
            logger.info("Successfully executed update query")
        except Exception as e:
            logger.error(f"Failed to execute update query: {e}")

    logger.info("Test data setup complete")


def verify_refresh_candidates(client, config, decay_factor=2.0):
    """Verify that refresh candidates are selected correctly."""
    # Query to check refresh candidates
    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_name = config["raw_tables"]["raw_responses"]["name"]

    query = f"""
    WITH game_years AS (
      SELECT 
        r.game_id,
        g.year_published,
        g.primary_name,
        r.last_refresh_timestamp,
        r.refresh_count,
        EXTRACT(YEAR FROM CURRENT_DATE()) as current_year
      FROM `{project_id}.{raw_dataset}.{table_name}` r
      JOIN `{project_id}.{config["project"]["dataset"]}.{config["tables"]["games"]["name"]}` g
        ON r.game_id = g.game_id
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
          WHEN year_published > current_year THEN {get_refresh_config()["upcoming_interval_days"]}  -- Upcoming games
          WHEN year_published = current_year THEN {get_refresh_config()["base_interval_days"]}  -- Current year
          ELSE LEAST(180, 
                    {get_refresh_config()["base_interval_days"]} * 
                    POWER(2, LEAST(10, LOG(2, {decay_factor}) * (current_year - year_published))))  -- Safe exponential decay
        END as refresh_interval_days
      FROM game_years
    ),
    due_for_refresh AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        refresh_interval_days,
        TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) as next_due,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 
                      TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY), 
                      HOUR) as hours_overdue
      FROM refresh_intervals
    )
    SELECT game_id, primary_name, year_published, next_due, hours_overdue
    FROM due_for_refresh 
    WHERE next_due <= CURRENT_TIMESTAMP()
    ORDER BY 
      year_published DESC,  -- Prioritize newer games
      hours_overdue DESC    -- Then most overdue
    LIMIT 20
    """

    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Found {len(df)} games due for refresh:")
        for _, row in df.iterrows():
            logger.info(
                f"Game {row['game_id']} ({row['primary_name']}): {row['year_published']}, {row['hours_overdue']} hours overdue"
            )
        return df
    except Exception as e:
        logger.error(f"Failed to query refresh candidates: {e}")
        return None


def check_data_consistency(client, config, game_ids):
    """Check data consistency before and after refresh."""
    if not game_ids or len(game_ids) == 0:
        logger.warning("No game IDs provided for consistency check")
        return

    game_ids_str = ",".join(str(id) for id in game_ids)

    # Query to get key game data
    project_id = config["project"]["id"]
    dataset = config["project"]["dataset"]
    games_table = config["tables"]["games"]["name"]

    query = f"""
    SELECT 
        game_id, 
        primary_name, 
        year_published, 
        min_players, 
        max_players, 
        min_playtime, 
        max_playtime, 
        min_age,
        users_rated,
        average_rating, 
        bayes_average, 
        standard_deviation, 
        median_rating,
        num_weights, 
        average_weight,
        load_timestamp
    FROM `{project_id}.{dataset}.{games_table}`
    WHERE game_id IN ({game_ids_str})
    """

    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Retrieved data for {len(df)} games")
        return df
    except Exception as e:
        logger.error(f"Failed to query game data: {e}")
        return None


def run_limited_refresh(environment="dev", batch_size=5):
    """Run a limited refresh operation with a small batch size."""
    # Create a fetcher with a small batch size
    fetcher = BGGResponseFetcher(
        batch_size=batch_size,
        chunk_size=2,  # Small chunk size to minimize API impact
        environment=environment,
    )

    # Run the fetcher
    logger.info(f"Running limited refresh with batch_size={batch_size}")
    result = fetcher.fetch_batch()

    return result


def verify_refresh_tracking(client, config, game_ids):
    """Verify that refresh tracking was updated correctly."""
    if not game_ids or len(game_ids) == 0:
        logger.warning("No game IDs provided for refresh tracking verification")
        return

    game_ids_str = ",".join(str(id) for id in game_ids)

    # Query to check refresh tracking
    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    table_name = config["raw_tables"]["raw_responses"]["name"]

    query = f"""
    SELECT 
        game_id, 
        last_refresh_timestamp, 
        refresh_count,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_refresh_timestamp, MINUTE) as minutes_since_refresh
    FROM `{project_id}.{raw_dataset}.{table_name}`
    WHERE game_id IN ({game_ids_str})
    """

    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Refresh tracking for {len(df)} games:")
        for _, row in df.iterrows():
            logger.info(
                f"Game {row['game_id']}: refreshed {row['minutes_since_refresh']} minutes ago, count={row['refresh_count']}"
            )
        return df
    except Exception as e:
        logger.error(f"Failed to query refresh tracking: {e}")
        return None


def check_monitoring_views(client, config):
    """Check that monitoring views are working correctly."""
    project_id = config["project"]["id"]

    # Check each monitoring view
    views = ["refresh_activity", "games_overdue_for_refresh"]

    for view in views:
        query = f"SELECT * FROM `{project_id}.monitoring.{view}` LIMIT 10"
        try:
            df = client.query(query).to_dataframe()
            logger.info(f"View {view} returned {len(df)} rows")
            logger.info(f"Columns: {', '.join(df.columns)}")
        except Exception as e:
            logger.error(f"Failed to query view {view}: {e}")


def run_test_refresh(environment="dev", decay_factor=2.0):
    """Run a test refresh cycle."""
    logger.info(f"Starting test refresh in {environment} environment")

    config = get_bigquery_config(environment)
    client = bigquery.Client()

    # 1. Set up test data
    logger.info("Setting up test data...")
    setup_test_data(client, config)

    # 2. Verify refresh candidates
    logger.info("Verifying refresh candidates...")
    candidates = verify_refresh_candidates(client, config, decay_factor)
    if candidates is None or len(candidates) == 0:
        logger.warning("No refresh candidates found. Test setup may be incorrect.")
        return

    # 3. Check data consistency before refresh
    candidate_ids = candidates["game_id"].tolist()
    logger.info(f"Checking data consistency for {len(candidate_ids)} games before refresh...")
    before_data = check_data_consistency(client, config, candidate_ids)

    # 4. Run a small refresh batch
    logger.info("Running limited refresh...")
    result = run_limited_refresh(environment, batch_size=5)

    # 5. Verify refresh tracking was updated
    if result:
        logger.info("Refresh batch completed successfully")

        # 6. Check refresh tracking
        logger.info("Verifying refresh tracking...")
        tracking = verify_refresh_tracking(client, config, candidate_ids)

        # 7. Check data consistency after refresh
        logger.info("Checking data consistency after refresh...")
        after_data = check_data_consistency(client, config, candidate_ids)

        # 8. Compare before and after data
        if before_data is not None and after_data is not None:
            logger.info("Comparing data before and after refresh...")
            # Check for any changes in key fields
            for game_id in candidate_ids:
                before_row = before_data[before_data["game_id"] == game_id]
                after_row = after_data[after_data["game_id"] == game_id]

                if not before_row.empty and not after_row.empty:
                    # Check if any fields changed
                    changed_fields = []
                    for col in before_row.columns:
                        if (
                            col != "last_updated"
                            and before_row[col].values[0] != after_row[col].values[0]
                        ):
                            changed_fields.append(col)

                    if changed_fields:
                        logger.info(
                            f"Game {game_id} had changes in fields: {', '.join(changed_fields)}"
                        )
                    else:
                        logger.info(f"Game {game_id} had no changes in key fields")
    else:
        logger.warning("No games were refreshed")

    # 9. Check monitoring views
    logger.info("Checking monitoring views...")
    check_monitoring_views(client, config)

    logger.info("Test refresh completed")


def create_test_environment():
    """Create a test environment by cloning a subset of production data."""
    # This function would create test tables with a subset of production data
    # Implementation depends on your specific BigQuery setup
    logger.info("Creating test environment is not implemented in this script")
    logger.info("Please create a test environment manually before running this script")


def main():
    """Main entry point for the test script."""
    import argparse

    parser = argparse.ArgumentParser(description="Test the BGG refresh strategy implementation")
    parser.add_argument(
        "--environment", type=str, default="dev", help="Environment to use (dev/test)"
    )
    parser.add_argument(
        "--create-env", action="store_true", help="Create a test environment (not implemented)"
    )
    parser.add_argument(
        "--decay-factor",
        type=float,
        default=2.0,
        help="Exponential decay factor for refresh intervals",
    )

    args = parser.parse_args()

    if args.create_env:
        create_test_environment()
    else:
        run_test_refresh(args.environment, args.decay_factor)


if __name__ == "__main__":
    main()
