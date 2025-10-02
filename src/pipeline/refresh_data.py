"""Pipeline for refreshing existing game data."""

from datetime import datetime
from typing import List, Dict, Any

from src.pipeline.base import BaseBGGPipeline
from src.config import get_refresh_config


class RefreshPipeline(BaseBGGPipeline):
    """Pipeline for refreshing existing game data based on configured intervals."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.refresh_config = get_refresh_config()

    def get_refresh_batch(self, batch_size: int = 100) -> List[int]:
        """Get a single batch of games that are due for refresh.

        Args:
            batch_size: Maximum number of games to return in this batch.

        Returns:
            List of game IDs that should be refreshed.
        """
        query = """
        SELECT game_id 
        FROM raw.raw_responses
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

        query = """
        UPDATE raw.raw_responses
        SET last_refresh_timestamp = CURRENT_TIMESTAMP(),
            refresh_count = refresh_count + 1,
            next_refresh_due = TIMESTAMP_ADD(
                CURRENT_TIMESTAMP(),
                INTERVAL LEAST(
                    @max_interval,
                    @base_interval * POW(@decay_factor, 
                        GREATEST(0, EXTRACT(YEAR FROM CURRENT_DATE()) - year_published))
                ) DAY)
        FROM (
            SELECT game_id, year_published 
            FROM bgg_data.games 
            WHERE game_id IN UNNEST(@game_ids)
        ) games
        WHERE raw_responses.game_id = games.game_id
        """

        self.execute_query(
            query,
            params={
                "game_ids": game_ids,
                "base_interval": self.refresh_config["base_interval_days"],
                "decay_factor": self.refresh_config["decay_factor"],
                "max_interval": self.refresh_config["max_interval_days"],
            },
        )

    def execute(self, batch_size: int = 100) -> Dict[str, Any]:
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

        # Fetch fresh data for these games
        responses = self.api_client.fetch_items(game_ids)

        # Store the refreshed responses
        self.loader.load_games(responses)

        # Update tracking info
        self.update_refresh_tracking(game_ids)

        return {
            "status": "complete",
            "games_refreshed": len(game_ids),
            "message": f"Successfully refreshed {len(game_ids)} games",
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
        }


if __name__ == "__main__":
    pipeline = RefreshPipeline()
    result = pipeline.execute()
    print(f"Refresh pipeline result: {result}")
