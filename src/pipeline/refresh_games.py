"""Pipeline module for refreshing previously loaded games based on publication year."""

import logging
import os
from datetime import datetime, UTC, timedelta
from typing import List, Dict, Optional

from google.cloud import bigquery

from ..api_client.client import BGGAPIClient
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging
from .process_responses import BGGResponseProcessor

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class BGGGameRefresher:
    """Refreshes previously loaded games with priority for recently published games."""

    def __init__(
        self,
        chunk_size: int = 20,
        environment: str = "test",
        dry_run: bool = False,
    ) -> None:
        """Initialize the refresher.

        Args:
            chunk_size: Number of games to request in each API call
            environment: Environment to use (test/dev/prod). Defaults to test for safety.
            dry_run: If True, only query and log what would be done without making changes
        """
        self.config = get_bigquery_config(environment)
        self.chunk_size = chunk_size
        self.environment = environment
        self.dry_run = dry_run
        self.api_client = BGGAPIClient()
        self.bq_client = bigquery.Client()

        # Load refresh policy from config
        refresh_config = self.config.get("refresh_policy", {})
        self.batch_size = refresh_config.get("batch_size", 1000)
        self.refresh_intervals = refresh_config.get("intervals", [])

        if self.dry_run:
            logger.info("*** DRY RUN MODE - No data will be fetched or written ***")
        logger.info(f"Initialized refresher with batch size {self.batch_size}")
        logger.info("Refresh intervals:")
        for interval in self.refresh_intervals:
            age_range = f"{interval.get('min_age_years', 0)}-{interval.get('max_age_years', '∞')} years"
            if interval.get('max_age_years') is None:
                age_range = f"{interval.get('min_age_years', 0)}+ years"
            elif interval.get('min_age_years', 0) == 0:
                age_range = f"0-{interval.get('max_age_years')} years"
            logger.info(f"  {interval['name']:12} | Age: {age_range:12} | Refresh every {interval['refresh_days']:3} days")

    def count_games_needing_refresh(self) -> Dict[str, int]:
        """Count how many games need refresh by category without fetching data.

        This is a lightweight query that only counts, minimizing BigQuery costs.

        Returns:
            Dictionary with counts by refresh category and total
        """
        try:
            current_year = datetime.now(UTC).year
            counts = {"total": 0}

            for interval in self.refresh_intervals:
                name = interval.get("name")
                max_age = interval.get("max_age_years")
                min_age = interval.get("min_age_years", 0)
                refresh_days = interval.get("refresh_days")

                # Calculate year thresholds
                if max_age:
                    min_year = current_year - max_age
                    year_filter = f"year_published BETWEEN {min_year} AND {current_year - min_age}"
                else:
                    year_filter = f"year_published <= {current_year - min_age}"

                # Count query - very cheap, only scans necessary partitions
                count_query = f"""
                SELECT COUNT(DISTINCT game_data.game_id) as count
                FROM (
                    SELECT game_id, year_published
                    FROM `{self.config['project']['id']}.{self.config['project']['dataset']}.{self.config['tables']['games']['name']}`
                    WHERE {year_filter}
                    GROUP BY game_id, year_published
                ) game_data
                LEFT JOIN (
                    SELECT
                        game_id,
                        MAX(load_timestamp) as last_load_timestamp
                    FROM `{self.config['project']['id']}.{self.config['project']['dataset']}.{self.config['tables']['games']['name']}`
                    GROUP BY game_id
                ) last_refresh ON game_data.game_id = last_refresh.game_id
                WHERE
                    (last_refresh.last_load_timestamp IS NULL
                     OR last_refresh.last_load_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {refresh_days} DAY))
                """

                result = self.bq_client.query(count_query).result()
                count = next(result).count
                counts[name] = count
                counts["total"] += count

            return counts

        except Exception as e:
            logger.error(f"Failed to count games needing refresh: {e}")
            raise

    def get_games_to_refresh(self) -> List[Dict]:
        """Get games that need to be refreshed based on publication year and last refresh time.

        Returns:
            List of dictionaries containing game IDs and metadata for games to refresh
        """
        try:
            # Clean up old in-progress entries first
            cleanup_query = f"""
            DELETE FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
            WHERE fetch_start_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
            """
            if not self.dry_run:
                self.bq_client.query(cleanup_query).result()
            else:
                logger.info(f"[DRY RUN] Would clean up old in-progress entries")

            # Build a query that finds games due for refresh
            # For each refresh interval, create a UNION query
            current_year = datetime.now(UTC).year

            interval_queries = []
            for interval in self.refresh_intervals:
                name = interval.get("name")
                max_age = interval.get("max_age_years")
                min_age = interval.get("min_age_years", 0)
                refresh_days = interval.get("refresh_days")

                # Calculate year thresholds
                if max_age:
                    min_year = current_year - max_age
                else:
                    min_year = 0  # No minimum for vintage games

                max_year = current_year - min_age

                # Build year filter
                if max_age:
                    year_filter = f"year_published BETWEEN {min_year} AND {max_year}"
                else:
                    year_filter = f"year_published <= {max_year}"

                # Create query for this interval
                interval_query = f"""
                SELECT
                    game_data.game_id,
                    game_data.year_published,
                    last_refresh.last_load_timestamp,
                    '{name}' as refresh_category,
                    {refresh_days} as refresh_days
                FROM (
                    SELECT game_id, year_published
                    FROM `{self.config['project']['id']}.{self.config['project']['dataset']}.{self.config['tables']['games']['name']}`
                    WHERE {year_filter}
                    GROUP BY game_id, year_published
                ) game_data
                LEFT JOIN (
                    SELECT
                        game_id,
                        MAX(load_timestamp) as last_load_timestamp
                    FROM `{self.config['project']['id']}.{self.config['project']['dataset']}.{self.config['tables']['games']['name']}`
                    GROUP BY game_id
                ) last_refresh ON game_data.game_id = last_refresh.game_id
                WHERE
                    -- Include games never loaded or loaded more than refresh_days ago
                    (last_refresh.last_load_timestamp IS NULL
                     OR last_refresh.last_load_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {refresh_days} DAY))
                    -- Exclude games currently being fetched
                    AND NOT EXISTS (
                        SELECT 1
                        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}` f
                        WHERE game_data.game_id = f.game_id
                    )
                """
                interval_queries.append(interval_query)

            # Check if we have any intervals configured
            if not interval_queries:
                logger.warning("No refresh intervals configured in refresh_policy")
                return []

            # Combine all interval queries with UNION ALL
            combined_query = " UNION ALL ".join(interval_queries)

            # Final query that prioritizes recent games
            final_query = f"""
            WITH all_candidates AS (
                {combined_query}
            )
            SELECT
                game_id,
                year_published,
                last_load_timestamp,
                refresh_category,
                refresh_days
            FROM all_candidates
            ORDER BY
                -- Prioritize by publication year (newer games first)
                year_published DESC,
                -- Then by staleness (older refresh times first)
                COALESCE(last_load_timestamp, TIMESTAMP('1970-01-01')) ASC
            LIMIT {self.batch_size}
            """

            logger.debug(f"Refresh query: {final_query}")

            # Execute query
            candidates_df = self.bq_client.query(final_query).to_dataframe()

            if len(candidates_df) > 0:
                # Mark them as in progress
                if not self.dry_run:
                    game_ids_str = ", ".join(str(id) for id in candidates_df["game_id"])
                    mark_query = f"""
                    INSERT INTO `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
                        (game_id, fetch_start_timestamp)
                    SELECT game_id, CURRENT_TIMESTAMP()
                    FROM UNNEST([{game_ids_str}]) AS game_id
                    """
                    self.bq_client.query(mark_query).result()

                logger.info(f"Found {len(candidates_df)} games to refresh")

                # Log breakdown by category
                category_counts = candidates_df['refresh_category'].value_counts()
                for category, count in category_counts.items():
                    logger.info(f"  - {category}: {count} games")

                if self.dry_run:
                    logger.info("[DRY RUN] Would mark these games as in-progress:")
                    # Show sample of games that would be refreshed
                    sample_size = min(10, len(candidates_df))
                    logger.info(f"Sample of first {sample_size} games:")
                    for idx, row in candidates_df.head(sample_size).iterrows():
                        logger.info(f"  Game ID {row['game_id']}: {row['year_published']} ({row['refresh_category']}) - Last refresh: {row['last_load_timestamp']}")
            else:
                logger.info("No games found that need refreshing")

            # Convert pandas DataFrame to list of dicts
            return [
                {
                    "game_id": row["game_id"],
                    "year_published": row["year_published"],
                    "last_load_timestamp": row["last_load_timestamp"],
                    "refresh_category": row["refresh_category"],
                }
                for _, row in candidates_df.iterrows()
            ]

        except Exception as e:
            logger.error(f"Failed to get games to refresh: {e}")
            raise

    def store_response(
        self, game_ids: List[int], response_data: str, no_response_ids: Optional[List[int]] = None
    ) -> None:
        """Store raw API response in BigQuery.

        Args:
            game_ids: List of game IDs in the response
            response_data: Raw API response data
            no_response_ids: List of game IDs with no response
        """
        import ast

        base_time = datetime.now(UTC)
        rows = []

        logger.info(
            f"store_response called with: game_ids={game_ids}, no_response_ids={no_response_ids}"
        )

        # Handle game IDs with no response
        if no_response_ids:
            logger.info(f"Processing {len(no_response_ids)} no-response game IDs")

            # Check existing attempts for these game IDs
            existing_attempts = {}
            if no_response_ids:
                game_ids_str = ",".join(str(gid) for gid in no_response_ids)
                attempt_query = f"""
                SELECT game_id, process_attempt
                FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}`
                WHERE game_id IN ({game_ids_str})
                """
                try:
                    attempt_results = self.bq_client.query(attempt_query).to_dataframe()
                    existing_attempts = dict(
                        zip(attempt_results["game_id"], attempt_results["process_attempt"])
                    )
                except Exception as e:
                    logger.warning(f"Could not fetch existing attempts: {e}")

            for game_id in no_response_ids:
                current_attempt = existing_attempts.get(game_id, 0) + 1
                row = {
                    "game_id": game_id,
                    "response_data": "",
                    "fetch_timestamp": base_time.isoformat(),
                    "processed": True,
                    "process_timestamp": base_time.isoformat(),
                    "process_status": "no_response",
                    "process_attempt": int(current_attempt),  # Convert numpy int64 to Python int
                }
                rows.append(row)

        # If response data is provided, process it
        if response_data:
            logger.info(f"Processing response data for {len(game_ids)} game IDs")

            try:
                # Parse the response data
                parsed_response = ast.literal_eval(response_data)

                # Extract items from the response
                items = parsed_response.get("items", {}).get("item", [])

                # Ensure items is a list
                if not isinstance(items, list):
                    items = [items] if items else []

                logger.info(f"Found {len(items)} items in the response")

                # Create a mapping of game IDs to their specific response
                game_responses = {}
                for item in items:
                    game_id = int(item.get("@id", 0))
                    if game_id in game_ids:
                        game_responses[game_id] = str({"items": {"item": item}})

                # Create rows for each game with its specific response
                for game_id in game_ids:
                    if game_id in game_responses:
                        row = {
                            "game_id": game_id,
                            "response_data": game_responses[game_id],
                            "fetch_timestamp": base_time.isoformat(),
                            "processed": False,
                            "process_timestamp": None,
                            "process_status": None,
                            "process_attempt": 0,
                        }
                        rows.append(row)

            except Exception as parse_error:
                logger.error(f"Failed to parse response data: {parse_error}")
                # Mark all game IDs as processed with error status
                existing_attempts = {}
                if game_ids:
                    game_ids_str = ",".join(str(gid) for gid in game_ids)
                    attempt_query = f"""
                    SELECT game_id, process_attempt
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}`
                    WHERE game_id IN ({game_ids_str})
                    """
                    try:
                        attempt_results = self.bq_client.query(attempt_query).to_dataframe()
                        existing_attempts = dict(
                            zip(attempt_results["game_id"], attempt_results["process_attempt"])
                        )
                    except Exception as e:
                        logger.warning(f"Could not fetch existing attempts for parse error: {e}")

                for game_id in game_ids:
                    current_attempt = existing_attempts.get(game_id, 0) + 1
                    row = {
                        "game_id": game_id,
                        "response_data": "",
                        "fetch_timestamp": base_time.isoformat(),
                        "processed": True,
                        "process_timestamp": base_time.isoformat(),
                        "process_status": "parse_error",
                        "process_attempt": int(current_attempt),  # Convert numpy int64 to Python int
                    }
                    rows.append(row)

        logger.info(f"Total rows to insert: {len(rows)}")

        table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}"

        try:
            # Use insert_rows_json for simplicity
            errors = self.bq_client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Failed to insert rows: {errors}")
                raise Exception(f"BigQuery insert errors: {errors}")

            logger.info(f"Successfully stored responses for {len(rows)} games")

            # Clean up fetch_in_progress entries for these games
            try:
                game_ids_str = ",".join(str(row["game_id"]) for row in rows)
                cleanup_query = f"""
                DELETE FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
                WHERE game_id IN ({game_ids_str})
                """
                self.bq_client.query(cleanup_query).result()
                logger.info(f"Cleaned up fetch_in_progress entries for {len(rows)} games")
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up fetch_in_progress entries: {cleanup_error}")

        except Exception as e:
            logger.error(f"Failed to store responses: {e}")
            for row in rows:
                logger.error(f"Problematic row: {row}")
            raise

    def fetch_batch(self, games_to_refresh: List[Dict]) -> bool:
        """Fetch and store responses for a batch of games.

        Args:
            games_to_refresh: List of game dictionaries to refresh

        Returns:
            bool: Whether any responses were fetched
        """
        if not games_to_refresh:
            logger.info("No games to refresh")
            return False

        if self.dry_run:
            logger.info(f"[DRY RUN] Would fetch {len(games_to_refresh)} games in chunks of {self.chunk_size}")
            logger.info(f"[DRY RUN] This would result in {(len(games_to_refresh) + self.chunk_size - 1) // self.chunk_size} API calls")
            return True

        # Process in chunks
        for i in range(0, len(games_to_refresh), self.chunk_size):
            chunk = games_to_refresh[i : i + self.chunk_size]
            chunk_ids = [game["game_id"] for game in chunk]

            try:
                logger.info(f"Refreshing data for games {chunk_ids}...")

                # Fetch data from API
                try:
                    response = self.api_client.get_thing(chunk_ids)

                    if response:
                        logger.debug(f"Raw response: {response}")

                        # Parse and validate the response
                        try:
                            import ast

                            parsed_response = ast.literal_eval(str(response))
                            items = parsed_response.get("items", {}).get("item", [])

                            if not isinstance(items, list):
                                items = [items] if items else []

                            # Extract game IDs from the response
                            response_game_ids = [
                                int(item.get("@id", 0))
                                for item in items
                                if int(item.get("@id", 0)) in chunk_ids
                            ]

                            logger.info(f"Found game IDs in response: {response_game_ids}")

                            # Store the response for found games
                            if response_game_ids:
                                self.store_response(response_game_ids, str(response))

                            # Mark games not found in the response
                            not_found_ids = [
                                game_id for game_id in chunk_ids if game_id not in response_game_ids
                            ]
                            if not_found_ids:
                                logger.warning(f"No data found for games: {not_found_ids}")
                                self.store_response([], None, not_found_ids)

                        except Exception as parse_error:
                            logger.error(f"Failed to parse response for {chunk_ids}: {parse_error}")
                            self.store_response([], None, chunk_ids)
                    else:
                        logger.warning(f"No data returned for games {chunk_ids}")
                        self.store_response([], None, chunk_ids)

                except Exception as e:
                    logger.error(f"Failed to fetch chunk {chunk_ids}: {e}")
                    raise

            except Exception as e:
                logger.error(f"Unhandled error in fetch_batch: {e}")
                continue

        return True

    def run(self) -> bool:
        """Run the refresh pipeline.

        Returns:
            bool: True if any games were refreshed, False otherwise
        """
        logger.info("Starting BGG game refresher")

        try:
            # First, check how many games need refreshing (cheap count query)
            counts = self.count_games_needing_refresh()
            logger.info(f"Games needing refresh: {counts['total']} total")
            for category, count in counts.items():
                if category != "total":
                    logger.info(f"  - {category}: {count} games")

            if counts["total"] == 0:
                logger.info("No games need refreshing")
                return False

            # Get games that need refreshing (this fetches actual data up to batch_size)
            logger.info(f"Fetching batch of up to {self.batch_size} games to refresh...")
            games_to_refresh = self.get_games_to_refresh()

            if not games_to_refresh:
                logger.info("No games to refresh")
                return False

            # Fetch and store responses
            games_refreshed = self.fetch_batch(games_to_refresh)

            if games_refreshed:
                logger.info(f"Refresher completed - {len(games_to_refresh)} games refreshed")
            else:
                logger.info("No games refreshed")

            return games_refreshed

        except Exception as e:
            logger.error(f"Refresher failed: {e}")
            raise


def main() -> None:
    """Main entry point for the refresher."""
    import argparse

    parser = argparse.ArgumentParser(description="Refresh BGG game data")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no data will be fetched or written)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default=None,
        help="Environment to use (test/dev/prod). Defaults to ENVIRONMENT env var or 'test'",
    )
    args = parser.parse_args()

    environment = args.environment or os.getenv("ENVIRONMENT", "test")
    logger.info(f"Starting refresher in {environment} environment")

    refresher = BGGGameRefresher(
        chunk_size=20,
        environment=environment,
        dry_run=args.dry_run,
    )
    games_refreshed = refresher.run()

    if games_refreshed:
        logger.info("Games were refreshed - now processing responses")
        processor = BGGResponseProcessor(environment=environment)
        processor.run()
        logger.info("Refresh cycle complete")
    else:
        logger.info("No games refreshed - no further action needed")


if __name__ == "__main__":
    main()
