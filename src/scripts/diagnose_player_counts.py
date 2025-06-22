"""Diagnostic script to investigate missing player count records."""

import logging
import json
from typing import List, Dict, Any

from google.cloud import bigquery
from ..config import get_bigquery_config
from ..data_processor.processor import BGGDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def diagnose_player_count_issues():
    """Diagnose potential issues with player count data extraction."""
    # Get BigQuery configuration
    config = get_bigquery_config()
    
    # Initialize BigQuery client with project and location
    bq_client = bigquery.Client(
        project=config['project']['id'], 
        location=config['project']['location']
    )

    # Query to get comprehensive player count coverage
    query = f"""
    WITH 
    total_games AS (
        SELECT COUNT(DISTINCT game_id) as total_game_count
        FROM `{config['project']['id']}.{config['project']['dataset']}.games`
    ),
    games_with_player_counts AS (
        SELECT COUNT(DISTINCT game_id) as player_count_game_count
        FROM `{config['project']['id']}.{config['project']['dataset']}.player_counts`
    ),
    missing_player_counts AS (
        SELECT g.game_id, g.primary_name
        FROM `{config['project']['id']}.{config['project']['dataset']}.games` g
        LEFT JOIN `{config['project']['id']}.{config['project']['dataset']}.player_counts` pc 
        ON g.game_id = pc.game_id
        WHERE pc.game_id IS NULL
        LIMIT 1000  # Limit to first 1000 missing entries for initial investigation
    )

    SELECT 
        total_game_count, 
        player_count_game_count,
        total_game_count - player_count_game_count as missing_player_count_games,
        ROUND((player_count_game_count / total_game_count) * 100, 2) as coverage_percentage,
        (SELECT ARRAY_AGG(STRUCT(game_id, primary_name)) FROM missing_player_counts) as missing_games
    FROM total_games, games_with_player_counts
    """

    # Execute query
    query_job = bq_client.query(query)
    results = query_job.result()
    result = next(results)

    # Prepare analysis results
    analysis_results = {
        'total_games': result.total_game_count,
        'games_with_player_counts': result.player_count_game_count,
        'missing_player_count_games': result.missing_player_count_games,
        'coverage_percentage': result.coverage_percentage,
        'missing_games_sample': result.missing_games
    }

    # Output results
    _output_analysis_results(analysis_results)

def _output_analysis_results(results: Dict[str, Any]):
    """Output analysis results to a file and console."""
    import json
    
    # Write to file
    with open('player_count_diagnosis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    logger.info("Player Count Diagnosis Summary:")
    logger.info(f"Total Games: {results['total_games']}")
    logger.info(f"Games with Player Counts: {results['games_with_player_counts']}")
    logger.info(f"Missing Player Count Games: {results['missing_player_count_games']}")
    logger.info(f"Coverage Percentage: {results['coverage_percentage']}%")
    
    # Log sample of missing games
    if results.get('missing_games_sample'):
        logger.info("\nSample of Games Missing Player Counts:")
        for game in results['missing_games_sample'][:10]:  # Show first 10
            logger.info(f"Game ID: {game['game_id']}, Name: {game['primary_name']}")
    
    logger.info("Full diagnosis saved to player_count_diagnosis.json")

    # Removed unnecessary code block
    pass

def main():
    diagnose_player_count_issues()

if __name__ == "__main__":
    main()
