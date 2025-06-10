"""Script to check for duplicate records in bridge tables."""

import logging
from google.cloud import bigquery

from src.config import get_bigquery_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_bridge_table_duplicates(client: bigquery.Client, project: str, dataset: str, table: str) -> None:
    """Check for duplicate records in a bridge table."""
    query = f"""
    WITH DuplicateCheck AS (
        SELECT 
            game_id,
            COUNT(*) as total_records,
            COUNT(DISTINCT CONCAT(
                -- Concatenate all non-game_id columns to check for exact duplicates
                {' || "_" || '.join(f"CAST({col.name} AS STRING)" for col in client.get_table(f"{project}.{dataset}.{table}").schema if col.name != 'game_id')}
            )) as unique_combinations
        FROM `{project}.{dataset}.{table}`
        GROUP BY game_id
    )
    SELECT 
        game_id,
        total_records,
        unique_combinations,
        total_records - unique_combinations as duplicate_count
    FROM DuplicateCheck
    WHERE total_records > unique_combinations
    ORDER BY duplicate_count DESC
    """
    
    try:
        results = client.query(query).result()
        logger.info(f"\nChecking duplicates in {table}:")
        logger.info("=" * 50)
        for row in results:
            logger.info(f"Game {row.game_id}: {row.total_records} total records, {row.unique_combinations} unique, {row.duplicate_count} duplicates")
            
            # Get example of duplicate records
            detail_query = f"""
            SELECT *
            FROM `{project}.{dataset}.{table}`
            WHERE game_id = {row.game_id}
            """
            details = client.query(detail_query).result()
            logger.info("\nExample records:")
            for detail in details:
                logger.info(detail)
            logger.info("-" * 50)
            
    except Exception as e:
        logger.error(f"Failed to check {table}: {e}")

def main() -> None:
    """Main function."""
    config = get_bigquery_config()
    client = bigquery.Client(project=config["project"]["id"])
    
    bridge_tables = [
        "game_categories",
        "game_mechanics",
        "game_families",
        "game_designers",
        "game_artists",
        "game_publishers"
    ]
    
    for table in bridge_tables:
        check_bridge_table_duplicates(
            client,
            config["project"]["id"],
            config["project"]["dataset"],
            table
        )

if __name__ == "__main__":
    main()
