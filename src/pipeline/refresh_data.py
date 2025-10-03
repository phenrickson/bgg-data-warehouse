"""Pipeline for refreshing existing game data."""

import logging
import os
from datetime import datetime
from typing import List, Dict, Any

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

    def __init__(self, environment: str = None, **kwargs):
        # Get environment from env var if not provided
        if not environment:
            environment = os.getenv("ENVIRONMENT")
        super().__init__(environment=environment, **kwargs)
        self.refresh_config = get_refresh_config()
        self.fetcher = BGGResponseFetcher(environment=environment)
        self.processor = BGGResponseProcessor(environment=environment)

    def get_refresh_batch(self, batch_size: int = 100) -> List[int]:
        """Get a single batch of games that are due for refresh.

        Args:
            batch_size: Maximum number of games to return in this batch.

        Returns:
            List of game IDs that should be refreshed.
        """
        query = f"""
        SELECT game_id 
        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.raw_responses`
        WHERE next_refresh_due < CURRENT_TIMESTAMP()
        ORDER BY next_refresh_due ASC
        LIMIT @batch_size
        """

        results = self.execute_query(query, params={"batch_size": batch_size})

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


if __name__ == "__main__":
    import os

    pipeline = RefreshPipeline(environment=os.getenv("ENVIRONMENT"))
    result = pipeline.execute()
    print(f"Refresh pipeline result: {result}")
