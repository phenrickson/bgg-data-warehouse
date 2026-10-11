"""Module for refreshing previously loaded games based on publication year."""

import logging
import os
from datetime import datetime, UTC, timedelta
from typing import List, Dict, Optional

from google.cloud import bigquery

from ..api_client.client import BGGAPIClient
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging
from .response_fetcher import ResponseFetcher

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

# Hardcoded table names (managed by Terraform)
RAW_DATASET = "raw"
CORE_DATASET = "core"
GAMES_TABLE = "games"
FETCHED_RESPONSES_TABLE = "fetched_responses"
FETCH_IN_PROGRESS_TABLE = "fetch_in_progress"


def _year_bounds(interval: Dict, current_year: int) -> tuple:
    """Inclusive (first, last) year_published for an age interval, None if unbounded.

    Age is current_year - year_published, matched half-open:
    min_age_years <= age < max_age_years.
    """
    max_age = interval.get("max_age_years")
    min_age = interval.get("min_age_years")
    first = current_year - max_age + 1 if max_age is not None else None
    last = current_year - min_age if min_age is not None else None
    return first, last


def _year_condition(interval: Dict, current_year: int) -> str:
    """SQL predicate on year_published for one refresh interval."""
    if interval.get("year_published_null"):
        return "year_published IS NULL"
    first, last = _year_bounds(interval, current_year)
    if first is not None and last is not None:
        return f"year_published BETWEEN {first} AND {last}"
    if first is not None:
        return f"year_published >= {first}"
    if last is not None:
        return f"year_published <= {last}"
    return "year_published IS NOT NULL"


def _bucket_case_sql(intervals: List[Dict], current_year: int) -> str:
    """refresh_category and refresh_days columns; each game takes its first matching interval."""
    conditions = [(_year_condition(i, current_year), i) for i in intervals]
    category = "\n".join(f"  WHEN {cond} THEN '{i['name']}'" for cond, i in conditions)
    days = "\n".join(f"  WHEN {cond} THEN {int(i['refresh_days'])}" for cond, i in conditions)
    return f"CASE\n{category}\nEND AS refresh_category,\nCASE\n{days}\nEND AS refresh_days"


class ResponseRefresher:
    """Refreshes previously loaded games with priority for recently published games."""

    def __init__(
        self,
        chunk_size: int = 20,
        dry_run: bool = False,
    ) -> None:
        """Initialize the refresher.

        Args:
            chunk_size: Number of games to request in each API call
            dry_run: If True, only query and log what would be done without making changes
        """
        self.config = get_bigquery_config()
        self.project_id = self.config["project"]["id"]
        self.chunk_size = chunk_size
        self.dry_run = dry_run
        self.api_client = BGGAPIClient()
        self.bq_client = bigquery.Client()
        self.response_fetcher = ResponseFetcher(
            chunk_size=chunk_size
        )

        # Load refresh policy from config
        refresh_config = self.config.get("refresh_policy", {})
        self.batch_size = refresh_config.get("batch_size", 1000)
        self.refresh_intervals = refresh_config.get("intervals", [])

        if self.dry_run:
            logger.info("*** DRY RUN MODE - No data will be fetched or written ***")
        logger.info(f"Initialized refresher with batch size {self.batch_size}")
        logger.info("Refresh intervals:")
        current_year = datetime.now(UTC).year
        for interval in self.refresh_intervals:
            logger.info(
                f"  {interval['name']:12} | {_year_condition(interval, current_year):40} "
                f"| refresh every {interval['refresh_days']:3} days"
            )

    def _due_games_sql(self) -> str:
        """Games past their bucket's refresh interval, with bucket and last fetch."""
        current_year = datetime.now(UTC).year
        return f"""
        WITH bucketed AS (
            SELECT
                game_id,
                year_published,
                {_bucket_case_sql(self.refresh_intervals, current_year)}
            FROM (
                SELECT game_id, ANY_VALUE(year_published) AS year_published
                FROM `{self.project_id}.{CORE_DATASET}.{GAMES_TABLE}`
                GROUP BY game_id
            )
        ),
        last_fetch AS (
            SELECT game_id, MAX(fetch_timestamp) AS last_fetch_timestamp
            FROM `{self.project_id}.{RAW_DATASET}.{FETCHED_RESPONSES_TABLE}`
            GROUP BY game_id
        )
        SELECT b.*, last_fetch.last_fetch_timestamp
        FROM bucketed b
        LEFT JOIN last_fetch USING (game_id)
        WHERE b.refresh_category IS NOT NULL
            AND (last_fetch.last_fetch_timestamp IS NULL
                 OR last_fetch.last_fetch_timestamp
                    < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL b.refresh_days DAY))
        """

    def count_games_needing_refresh(self) -> Dict[str, int]:
        """Count how many games need refresh by category without fetching data.

        Returns:
            Dictionary with counts by refresh category and total
        """
        try:
            count_query = f"""
            SELECT refresh_category, COUNT(*) AS games
            FROM ({self._due_games_sql()})
            GROUP BY refresh_category
            """
            counts = {row.refresh_category: row.games for row in self.bq_client.query(count_query).result()}
            counts["total"] = sum(counts.values())
            return counts

        except Exception as e:
            logger.error(f"Failed to count games needing refresh: {e}")
            raise

    def get_games_to_refresh(self) -> List[Dict]:
        """Get the most overdue games, relative to their bucket's refresh interval.

        Returns:
            List of dictionaries containing game IDs and metadata for games to refresh
        """
        try:
            # Clean up old in-progress entries first
            cleanup_query = f"""
            DELETE FROM `{self.project_id}.{RAW_DATASET}.{FETCH_IN_PROGRESS_TABLE}`
            WHERE fetch_start_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
            """
            if not self.dry_run:
                self.bq_client.query(cleanup_query).result()
            else:
                logger.info(f"[DRY RUN] Would clean up old in-progress entries")

            if not self.refresh_intervals:
                logger.warning("No refresh intervals configured in refresh_policy")
                return []

            # Never-fetched games first, then by how many intervals overdue, so a
            # short budget delays every bucket evenly instead of starving the oldest.
            final_query = f"""
            SELECT
                game_id,
                year_published,
                last_fetch_timestamp,
                refresh_category,
                refresh_days,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_fetch_timestamp, HOUR)
                    / (refresh_days * 24) AS overdue_ratio
            FROM ({self._due_games_sql()}) due
            WHERE NOT EXISTS (
                SELECT 1
                FROM `{self.project_id}.{RAW_DATASET}.{FETCH_IN_PROGRESS_TABLE}` f
                WHERE due.game_id = f.game_id
            )
            ORDER BY
                last_fetch_timestamp IS NULL DESC,
                overdue_ratio DESC
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
                    INSERT INTO `{self.project_id}.{RAW_DATASET}.{FETCH_IN_PROGRESS_TABLE}`
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
                        logger.info(f"  Game ID {row['game_id']}: {row['year_published']} ({row['refresh_category']}) - Last fetch: {row['last_fetch_timestamp']}")
            else:
                logger.info("No games found that need refreshing")

            # Convert pandas DataFrame to list of dicts
            return [
                {
                    "game_id": row["game_id"],
                    "year_published": row["year_published"],
                    "last_fetch_timestamp": row["last_fetch_timestamp"],
                    "refresh_category": row["refresh_category"],
                }
                for _, row in candidates_df.iterrows()
            ]

        except Exception as e:
            logger.error(f"Failed to get games to refresh: {e}")
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
                                self.response_fetcher.store_response(response_game_ids, str(response))

                            # Mark games not found in the response
                            not_found_ids = [
                                game_id for game_id in chunk_ids if game_id not in response_game_ids
                            ]
                            if not_found_ids:
                                logger.warning(f"No data found for games: {not_found_ids}")
                                self.response_fetcher.store_response([], None, not_found_ids)

                        except Exception as parse_error:
                            logger.error(f"Failed to parse response for {chunk_ids}: {parse_error}")
                            self.response_fetcher.store_response([], None, chunk_ids)
                    else:
                        logger.warning(f"No data returned for games {chunk_ids}")
                        self.response_fetcher.store_response([], None, chunk_ids)

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
        logger.info("Starting response refresher")

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
