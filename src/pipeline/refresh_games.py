"""Pipeline for refreshing existing game data."""

import argparse
import logging
import os
from datetime import datetime
from typing import List, Dict, Any
import math

from src.pipeline.base import BaseBGGPipeline
from src.pipeline.fetch_responses import BGGResponseFetcher
from src.pipeline.process_responses import BGGResponseProcessor
from src.config import get_refresh_config
from src.utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class RefreshPipeline(BaseBGGPipeline):
    """Pipeline for refreshing existing game data based on configured intervals."""

    def __init__(self, environment: str = None, max_games: int = None, **kwargs):
        # Get environment from env var if not provided
        if not environment:
            environment = os.getenv("ENVIRONMENT")
        super().__init__(environment=environment, **kwargs)
        self.refresh_config = get_refresh_config()
        self.fetcher = BGGResponseFetcher(environment=environment)
        self.processor = BGGResponseProcessor(environment=environment)
        self.max_games = max_games

    def get_refresh_batch(self, batch_size: int = 1000) -> List[int]:
        """Get a single batch of games that are due for refresh.

        Args:
            batch_size: Maximum number of games to return in this batch.

        Returns:
            List of game IDs that should be refreshed.
        """
        # If max_games is set, calculate how many more games we can process
        if self.max_games:
            # Count games already processed in current hour
            processed_query = f"""
            SELECT COUNT(*) as count
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses`
            WHERE last_refresh_timestamp >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
            """

            processed_result = self.execute_query(processed_query)
            processed_count = processed_result[0].count if processed_result else 0

            # Calculate remaining capacity
            remaining_capacity = self.max_games - processed_count
            if remaining_capacity <= 0:
                return []

            # Use the smaller of batch_size or remaining capacity
            effective_batch_size = min(batch_size, remaining_capacity)
        else:
            effective_batch_size = batch_size

        query = f"""
        SELECT game_id 
        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses`
        WHERE next_refresh_due < CURRENT_TIMESTAMP()
        ORDER BY next_refresh_due ASC
        LIMIT @batch_size
        """

        results = self.execute_query(query, params={"batch_size": effective_batch_size})

        return [row.game_id for row in results]

    def update_refresh_tracking(self, game_ids: List[int]) -> None:
        """Update refresh tracking columns for processed games.

        Args:
            game_ids: List of game IDs that were refreshed.
        """
        if not game_ids:
            return

        # Convert game_ids to comma-separated string for IN clause
        game_ids_str = ",".join(str(id) for id in game_ids)

        query = f"""
        UPDATE `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses` AS r
        SET last_refresh_timestamp = CURRENT_TIMESTAMP(),
            refresh_count = refresh_count + 1,
            next_refresh_due = TIMESTAMP_ADD(
                CURRENT_TIMESTAMP(),
                INTERVAL LEAST(
                    @max_interval,
                    CAST(@base_interval * POW(@decay_factor, 
                        GREATEST(0, EXTRACT(YEAR FROM CURRENT_DATE()) - COALESCE(g.year_published, EXTRACT(YEAR FROM CURRENT_DATE()))))
                    AS INT64)
                ) DAY)
        FROM `{self.config['project']['id']}.{self.config['project']['dataset']}.games` AS g
        WHERE r.game_id = g.game_id AND r.game_id IN ({game_ids_str})
        """

        self.execute_query(
            query,
            params={
                "base_interval": self.refresh_config["base_interval_days"],
                "decay_factor": self.refresh_config["decay_factor"],
                "max_interval": self.refresh_config["max_interval_days"],
            },
        )

    def execute(self, batch_size: int = 20) -> Dict[str, Any]:
        """Execute one batch of the refresh pipeline.

        Args:
            batch_size: Number of games to refresh in this batch.

        Returns:
            Dict containing execution statistics:
                - status: Completion status ("complete")
                - games_refreshed: Number of games refreshed
                - message: Human-readable status message
                - duration_seconds: Total execution time in seconds
        """
        start_time = datetime.now()

        # Get batch of games to refresh
        game_ids = self.get_refresh_batch(batch_size)
        if not game_ids:
            return {
                "status": "complete",
                "games_refreshed": 0,
                "message": "No games need refresh",
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Use fetcher to get and store responses
        self.fetcher.fetch_batch(game_ids)

        # Use processor to process the responses
        self.processor.run()

        # Update refresh tracking info
        self.update_refresh_tracking(game_ids)

        return {
            "status": "complete",
            "games_refreshed": len(game_ids),
            "message": f"Successfully refreshed {len(game_ids)} games",
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
        }

    def count_games_needing_refresh(self) -> Dict[str, Any]:
        """Count total games that need refresh and calculate batch statistics.

        Returns:
            Dict containing refresh statistics:
                - total_games_needing_refresh: Total number of games due for refresh
                - unfetched_games: Number of games never fetched
                - refresh_games: Number of games due for refresh
                - batch_size: Current batch size from fetcher
                - estimated_batches: Number of batches needed
                - estimated_api_calls: Estimated API calls needed
        """
        # Get unfetched games count
        unfetched_query = f"""
        SELECT COUNT(*) as count
        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.thing_ids` t
        WHERE NOT EXISTS (
            SELECT 1 
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses` r
            WHERE t.game_id = r.game_id
        )
        AND t.type = 'boardgame'
        """

        unfetched_result = self.execute_query(unfetched_query)
        unfetched_count = unfetched_result[0].count if unfetched_result else 0

        # Get refresh candidates using the same logic as BGGResponseFetcher
        refresh_query = f"""
        WITH game_years AS (
          SELECT 
            r.game_id,
            g.year_published,
            r.last_refresh_timestamp,
            r.refresh_count,
            EXTRACT(YEAR FROM CURRENT_DATE()) as current_year
          FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses` r
          JOIN `{self.config['project']['id']}.{self.config['project']['dataset']}.games` g
            ON r.game_id = g.game_id
          WHERE r.processed = TRUE
            AND r.last_refresh_timestamp IS NOT NULL
        ),
        refresh_intervals AS (
          SELECT 
            game_id,
            year_published,
            last_refresh_timestamp,
            refresh_count,
            CASE 
              WHEN year_published > current_year THEN {self.refresh_config.get('upcoming_interval_days', 7)}
              WHEN year_published = current_year THEN {self.refresh_config['base_interval_days']}
              ELSE LEAST({self.refresh_config['max_interval_days']}, 
                         {self.refresh_config['base_interval_days']} * LEAST(POW({self.refresh_config['decay_factor']}, LEAST(current_year - year_published, 10)), 
                               {self.refresh_config['max_interval_days']}))
            END as refresh_interval_days
          FROM game_years
        ),
        due_for_refresh AS (
          SELECT 
            game_id,
            year_published,
            refresh_interval_days,
            TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY) as next_due,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 
                          TIMESTAMP_ADD(last_refresh_timestamp, INTERVAL CAST(refresh_interval_days AS INT64) DAY), 
                          HOUR) as hours_overdue
          FROM refresh_intervals
        )
        SELECT COUNT(*) as count
        FROM due_for_refresh 
        WHERE next_due <= CURRENT_TIMESTAMP()
        """

        refresh_result = self.execute_query(refresh_query)
        refresh_count = refresh_result[0].count if refresh_result else 0

        total_games = unfetched_count + refresh_count
        batch_size = self.fetcher.batch_size  # Get batch size from fetcher (1000)
        chunk_size = self.fetcher.chunk_size  # Get chunk size from fetcher (20)

        estimated_batches = math.ceil(total_games / batch_size) if total_games > 0 else 0
        estimated_api_calls = math.ceil(total_games / chunk_size) if total_games > 0 else 0

        return {
            "total_games_needing_refresh": total_games,
            "unfetched_games": unfetched_count,
            "refresh_games": refresh_count,
            "batch_size": batch_size,
            "estimated_batches": estimated_batches,
            "estimated_api_calls": estimated_api_calls,
            "chunk_size": chunk_size,
        }

    def preview_refresh(self) -> None:
        """Display preview information about the refresh operation."""
        logger.info("REFRESH PREVIEW")
        logger.info("=" * 50)

        try:
            stats = self.count_games_needing_refresh()

            logger.info("Games Summary:")
            logger.info(f"  * Unfetched games: {stats['unfetched_games']:,}")
            logger.info(f"  * Games due for refresh: {stats['refresh_games']:,}")
            logger.info(f"  * Total games to process: {stats['total_games_needing_refresh']:,}")

            if stats["total_games_needing_refresh"] > 0:
                logger.info("Processing Configuration:")
                logger.info(f"  * Batch size: {stats['batch_size']:,} games per batch")
                logger.info(f"  * Chunk size: {stats['chunk_size']} games per API call")

                logger.info("Estimated Workload:")
                logger.info(f"  * Number of batches: {stats['estimated_batches']:,}")
                logger.info(f"  * API calls needed: {stats['estimated_api_calls']:,}")

                # Estimate time (rough calculation)
                # Assume ~2 seconds per API call on average
                estimated_minutes = (stats["estimated_api_calls"] * 2) / 60
                if estimated_minutes < 60:
                    logger.info(f"  * Estimated time: ~{estimated_minutes:.1f} minutes")
                else:
                    hours = estimated_minutes / 60
                    logger.info(f"  * Estimated time: ~{hours:.1f} hours")
            else:
                logger.info("No games currently need refresh!")

        except Exception as e:
            logger.error(f"Failed to generate preview: {e}")

    def run_full_refresh(self) -> None:
        """Run the full refresh process until all games are processed or max_games limit reached."""
        if self.max_games:
            logger.info("STARTING LIMITED REFRESH PROCESS")
            logger.info(f"Maximum games to process: {self.max_games:,}")
        else:
            logger.info("STARTING FULL REFRESH PROCESS")
        logger.info("=" * 50)

        total_processed = 0
        batch_number = 1

        try:
            while True:
                logger.info(f"Processing batch {batch_number}...")

                # Get games that need refresh using our limited batch logic
                game_ids = self.get_refresh_batch()

                if not game_ids:
                    if self.max_games and total_processed >= self.max_games:
                        logger.info(f"Reached max games limit ({self.max_games:,})")
                    else:
                        logger.info("No more games need refresh!")
                    break

                logger.info(f"  Found {len(game_ids)} games to process")

                # Use fetcher to get and store responses
                self.fetcher.fetch_batch(game_ids)

                # Process the responses
                self.processor.run()

                # Update refresh tracking info
                self.update_refresh_tracking(game_ids)

                total_processed += len(game_ids)
                logger.info(f"  Batch {batch_number} complete ({len(game_ids)} games)")
                logger.info(f"  Total processed so far: {total_processed:,}")

                batch_number += 1

        except KeyboardInterrupt:
            logger.warning("Process interrupted by user")
            logger.info(f"Total games processed: {total_processed}")
        except Exception as e:
            logger.error(f"Error during refresh: {e}")
            logger.info(f"Total games processed before error: {total_processed}")

        logger.info("Refresh process finished")
        logger.info(f"Total games processed: {total_processed}")


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(description="BGG Data Refresh Pipeline")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Show preview of refresh statistics without running the actual refresh",
    )
    parser.add_argument(
        "--environment",
        default=os.getenv("ENVIRONMENT", "dev"),
        help="Environment to run in (dev/test/prod)",
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Maximum number of games to process in this run",
    )

    args = parser.parse_args()

    # Get max_games from command line arg or environment variable
    max_games = args.max_games or (int(os.getenv("MAX_GAMES")) if os.getenv("MAX_GAMES") else None)

    # Set up the pipeline
    pipeline = RefreshPipeline(environment=args.environment, max_games=max_games)

    if args.preview:
        # Show preview without running
        pipeline.preview_refresh()
    else:
        # Run the full refresh process
        pipeline.run_full_refresh()


if __name__ == "__main__":
    main()
