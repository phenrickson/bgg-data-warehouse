"""Dry run script for the refresh strategy implementation.

This script identifies which games would be refreshed based on the
exponential decay refresh strategy without actually making any changes
to the database.
"""

import logging
import os
import sys
import traceback
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

from src.config import get_bigquery_config, get_refresh_config

# Load environment variables
load_dotenv()

# Explicitly set the credentials path
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed logging
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Output to console
        logging.FileHandler("refresh_strategy_debug.log"),  # Output to file
    ],
)
logger = logging.getLogger(__name__)


def get_refresh_candidates(client, config, limit=25000, decay_factor=2.0):
    """Get games that would be selected for refresh based on the strategy.

    This is a read-only operation that doesn't modify any data.
    """
    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    raw_responses_table = config["raw_tables"]["raw_responses"]["name"]
    games_table = config["tables"]["games"]["name"]
    main_dataset = config["project"]["dataset"]

    # Get refresh configuration
    refresh_config = get_refresh_config()

    # Log detailed configuration
    logger.info(f"Refresh Configuration: {refresh_config}")
    logger.info(f"Limit: {limit}")
    logger.info(f"Decay Factor: {decay_factor}")
    logger.info(f"Current Date: {datetime.now().date()}")

    # Query to identify refresh candidates based on the exponential decay formula
    query = f"""
    WITH game_years AS (
      SELECT 
        r.game_id,
        g.year_published,
        g.primary_name,
        r.last_refresh_timestamp,
        r.refresh_count,
        EXTRACT(YEAR FROM CURRENT_DATE()) as current_year,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_refresh_timestamp, DAY) as days_since_last_refresh
      FROM `{project_id}.{raw_dataset}.{raw_responses_table}` r
      JOIN `{project_id}.{main_dataset}.{games_table}` g
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
        current_year,
        days_since_last_refresh,
        CASE 
          WHEN year_published > current_year THEN {refresh_config["upcoming_interval_days"]}  -- Upcoming games
          WHEN year_published = current_year THEN {refresh_config["base_interval_days"]}  -- Current year
      ELSE LEAST(180, 
                    {refresh_config["base_interval_days"]} * 
                    POW({decay_factor}, LEAST(50, current_year - year_published)))  -- Exponential decay with year cap
        END as refresh_interval_days
      FROM game_years
    ),
    due_for_refresh AS (
      SELECT 
        game_id,
        primary_name,
        year_published,
        current_year,
        last_refresh_timestamp,
        refresh_count,
        refresh_interval_days,
        days_since_last_refresh,
        TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) as next_due,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 
                      TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY), 
                      HOUR) as hours_overdue
      FROM refresh_intervals
    )
    SELECT 
        game_id,
        primary_name,
        year_published,
        current_year,
        last_refresh_timestamp,
        refresh_count,
        refresh_interval_days,
        days_since_last_refresh,
        next_due,
        hours_overdue,
        CASE 
          WHEN year_published > current_year THEN 'Upcoming'
          WHEN year_published = current_year THEN 'Current Year'
          WHEN year_published = current_year - 1 THEN 'Last Year'
          WHEN year_published <= current_year - 2 THEN 'Older'
        END as age_category
    FROM due_for_refresh 
    WHERE next_due <= CURRENT_TIMESTAMP()
    ORDER BY 
      year_published DESC,  -- Prioritize newer games
      hours_overdue DESC    -- Then most overdue
    LIMIT {limit}
    """

    try:
        logger.debug("Executing refresh candidates query")
        logger.debug(f"Full query: {query}")

        # Capture detailed query execution information
        job_config = bigquery.QueryJobConfig()
        query_job = client.query(query, job_config=job_config)

        # Log query job details
        logger.debug(f"Query Job ID: {query_job.job_id}")
        logger.debug(f"Query Job State: {query_job.state}")

        # Fetch results
        df = query_job.to_dataframe()

        logger.info(f"Query successful. Found {len(df)} refresh candidates")

        # Detailed logging of extreme cases
        if not df.empty:
            max_year_diff = df["current_year"] - df["year_published"]
            logger.info(f"Max year difference: {max_year_diff.max()}")
            logger.info(f"Min year difference: {max_year_diff.min()}")

            # Log extreme cases for debugging
            extreme_cases = df[max_year_diff > 50]
            if not extreme_cases.empty:
                logger.warning("Extreme year differences found:")
                logger.warning(
                    extreme_cases[["game_id", "primary_name", "year_published", "current_year"]]
                )

        return df
    except Exception as e:
        logger.error(f"Failed to query refresh candidates: {e}")
        logger.error(f"Refresh configuration: {refresh_config}")

        # Detailed error logging
        logger.error(traceback.format_exc())

        return pd.DataFrame()


def get_unfetched_games(client, config, limit=100):
    """Get games that have never been fetched.

    This is a read-only operation that doesn't modify any data.
    """
    project_id = config["project"]["id"]
    raw_dataset = config["datasets"]["raw"]
    thing_ids_table = config["raw_tables"]["thing_ids"]["name"]
    raw_responses_table = config["raw_tables"]["raw_responses"]["name"]

    query = f"""
    SELECT t.game_id, t.type
    FROM `{project_id}.{raw_dataset}.{thing_ids_table}` t
    WHERE NOT EXISTS (
        SELECT 1 
        FROM `{project_id}.{raw_dataset}.{raw_responses_table}` r
        WHERE t.game_id = r.game_id
    )
    AND t.type = 'boardgame'
    ORDER BY t.game_id
    LIMIT {limit}
    """

    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"Failed to query unfetched games: {e}")
        return pd.DataFrame()


def analyze_refresh_distribution(refresh_candidates):
    """Analyze the distribution of refresh candidates by year and category."""
    if refresh_candidates.empty:
        logger.warning("No refresh candidates found")
        return

    # Distribution by year
    year_counts = refresh_candidates.groupby("year_published").size()
    logger.info("\n=== Refresh Distribution by Year ===")
    for year, count in year_counts.items():
        logger.info(f"Year {year}: {count} games")

    # Distribution by age category
    category_counts = refresh_candidates.groupby("age_category").size()
    logger.info("\n=== Refresh Distribution by Category ===")
    for category, count in category_counts.items():
        logger.info(f"{category}: {count} games")

    # Average refresh interval by year
    avg_intervals = refresh_candidates.groupby("year_published")["refresh_interval_days"].mean()
    logger.info("\n=== Average Refresh Interval by Year (days) ===")
    for year, interval in avg_intervals.items():
        logger.info(f"Year {year}: {interval:.1f} days")

    # Average hours overdue by year
    avg_overdue = refresh_candidates.groupby("year_published")["hours_overdue"].mean()
    logger.info("\n=== Average Hours Overdue by Year ===")
    for year, hours in avg_overdue.items():
        logger.info(f"Year {year}: {hours:.1f} hours")

    # Detailed analysis of 2023 games
    logger.info("\n=== Detailed Analysis of 2023 Games ===")
    if 2023 in year_counts.index:
        year_2023_games = refresh_candidates[refresh_candidates["year_published"] == 2023]
        logger.info(f"Total 2023 games due for refresh: {len(year_2023_games)}")

        # Days since last refresh
        avg_days_since_refresh = year_2023_games["days_since_last_refresh"].mean()
        logger.info(f"Average days since last refresh: {avg_days_since_refresh:.1f}")

        # Refresh interval details
        avg_refresh_interval = year_2023_games["refresh_interval_days"].mean()
        logger.info(f"Average refresh interval: {avg_refresh_interval:.1f} days")

        # Sample of 2023 games
        logger.info("\nSample of 2023 games due for refresh:")
        sample_columns = [
            "game_id",
            "primary_name",
            "last_refresh_timestamp",
            "days_since_last_refresh",
            "refresh_interval_days",
            "hours_overdue",
        ]
        logger.info(year_2023_games[sample_columns].head(10).to_string())


def simulate_batch_selection(
    unfetched_games, refresh_candidates, batch_size=1000, refresh_batch_size=200
):
    """Simulate which games would be selected in a batch operation."""
    logger.info("\n=== Batch Selection Simulation ===")

    # Calculate how many unfetched games would be included
    unfetched_count = min(len(unfetched_games), batch_size)

    # Calculate how many refresh games would be included
    remaining_slots = batch_size - unfetched_count
    refresh_count = min(len(refresh_candidates), min(remaining_slots, refresh_batch_size))

    logger.info(f"Batch composition:")
    logger.info(f"  - Unfetched games: {unfetched_count}")
    logger.info(f"  - Refresh games: {refresh_count}")
    logger.info(f"  - Total batch size: {unfetched_count + refresh_count}")

    # Show sample of unfetched games
    if not unfetched_games.empty:
        sample_size = min(5, len(unfetched_games))
        logger.info(f"\nSample of {sample_size} unfetched games that would be included:")
        for _, row in unfetched_games.head(sample_size).iterrows():
            logger.info(f"  - Game ID: {row['game_id']}")

    # Show sample of refresh games
    if not refresh_candidates.empty:
        sample_size = min(5, len(refresh_candidates))
        logger.info(f"\nSample of {sample_size} refresh games that would be included:")
        for _, row in refresh_candidates.head(sample_size).iterrows():
            logger.info(
                f"  - Game ID: {row['game_id']}, Name: {row['primary_name']}, Year: {row['year_published']}"
            )


def export_results_to_csv(unfetched_games, refresh_candidates):
    """Export the results to CSV files for further analysis."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not unfetched_games.empty:
        unfetched_file = f"unfetched_games_{timestamp}.csv"
        unfetched_games.to_csv(unfetched_file, index=False)
        logger.info(f"Exported {len(unfetched_games)} unfetched games to {unfetched_file}")

    if not refresh_candidates.empty:
        refresh_file = f"refresh_candidates_{timestamp}.csv"
        refresh_candidates.to_csv(refresh_file, index=False)
        logger.info(f"Exported {len(refresh_candidates)} refresh candidates to {refresh_file}")


def run_dry_run(environment="dev", export_csv=False, limit=1000, decay_factor=2.0):
    """Run a dry run of the refresh strategy."""
    logger.info(f"Starting refresh strategy dry run in {environment} environment")

    # Get configuration
    config = get_bigquery_config(environment)
    refresh_config = get_refresh_config()

    # Log refresh configuration
    logger.info("\n=== Refresh Configuration ===")
    logger.info(f"Base interval (current year): {refresh_config['base_interval_days']} days")
    logger.info(f"Upcoming interval: {refresh_config['upcoming_interval_days']} days")
    logger.info(f"Decay factor: {decay_factor}")
    logger.info(f"Maximum interval: {refresh_config['max_interval_days']} days")
    logger.info(f"Refresh batch size: {refresh_config['refresh_batch_size']}")

    # Create BigQuery client
    client = bigquery.Client()

    # Get unfetched games
    logger.info("\nIdentifying unfetched games...")
    unfetched_games = get_unfetched_games(client, config)
    logger.info(f"Found {len(unfetched_games)} unfetched games")

    # Get refresh candidates
    logger.info("\nIdentifying refresh candidates...")
    refresh_candidates = get_refresh_candidates(client, config, limit, decay_factor)
    logger.info(f"Found {len(refresh_candidates)} games due for refresh")

    # Analyze refresh distribution
    if not refresh_candidates.empty:
        analyze_refresh_distribution(refresh_candidates)

    # Simulate batch selection
    simulate_batch_selection(
        unfetched_games,
        refresh_candidates,
        batch_size=1000,
        refresh_batch_size=refresh_config["refresh_batch_size"],
    )

    # Export results to CSV if requested
    if export_csv:
        export_results_to_csv(unfetched_games, refresh_candidates)

    logger.info("\nDry run completed successfully")


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(description="Run a dry run of the BGG refresh strategy")
    parser.add_argument(
        "--environment", type=str, default="dev", help="Environment to use (dev/prod)"
    )
    parser.add_argument("--export-csv", action="store_true", help="Export results to CSV files")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of games to analyze")
    parser.add_argument(
        "--decay-factor",
        type=float,
        default=2,
        help="Exponential decay factor for refresh intervals",
    )

    args = parser.parse_args()

    # Set environment variable if provided
    if args.environment:
        os.environ["ENVIRONMENT"] = args.environment

    run_dry_run(args.environment, args.export_csv, args.limit, args.decay_factor)


if __name__ == "__main__":
    main()
