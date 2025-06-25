"""Script to test parallel fetching with the locking mechanism."""

import logging
import asyncio
import os
from typing import List

from ..pipeline.fetch_responses import BGGResponseFetcher
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

async def run_fetcher(task_id: int, game_ids: List[int]) -> None:
    """Run a fetcher task.
    
    Args:
        task_id: ID of this task for logging
        game_ids: List of game IDs to fetch
    """
    logger.info(f"Task {task_id} starting")
    
    try:
        fetcher = BGGResponseFetcher(
            batch_size=len(game_ids),
            chunk_size=5,
            environment="dev"
        )
        
        # Run the fetcher
        fetcher.run(game_ids)
        logger.info(f"Task {task_id} completed")
        
    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")

async def main() -> None:
    """Run multiple fetcher tasks in parallel."""
    # Use a small set of game IDs for testing
    test_game_ids = [13, 9209, 325, 1406, 2651, 822, 9217, 13823, 68448, 9216]
    
    # Create multiple tasks that will try to fetch the same game IDs
    tasks = []
    for i in range(5):  # Simulate 5 parallel tasks
        task = asyncio.create_task(run_fetcher(i, test_game_ids))
        tasks.append(task)
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    logger.info("All tasks completed")
    
    # Query BigQuery to check for duplicates
    from google.cloud import bigquery
    from ..config import get_bigquery_config
    
    config = get_bigquery_config()
    client = bigquery.Client()
    
    # Check for duplicate responses
    query = f"""
    SELECT game_id, COUNT(*) as response_count
    FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['raw_responses']['name']}`
    WHERE game_id IN ({','.join(str(id) for id in test_game_ids)})
    GROUP BY game_id
    HAVING COUNT(*) > 1
    """
    
    df = client.query(query).to_dataframe()
    
    if df.empty:
        logger.info("No duplicate responses found - locking mechanism working correctly!")
    else:
        logger.error(f"Found duplicate responses: {df.to_string()}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
