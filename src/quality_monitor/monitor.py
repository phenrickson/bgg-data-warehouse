"""Module for monitoring data quality in the BGG Data Warehouse."""

import logging
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List

from google.cloud import bigquery

from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

class DataQualityMonitor:
    """Monitors data quality for the BGG Data Warehouse."""
    
    def __init__(self, config: Optional[Dict] = None) -> None:
        """Initialize the data quality monitor.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or get_bigquery_config()
        self.bq_client = bigquery.Client()
        
        # Configure tables from config or use defaults
        self.raw_responses_table = self.config.get(
            'raw_responses_table', 
            f"{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}"
        )
        self.processed_games_table = self.config.get(
            'processed_games_table', 
            f"{self.config['project']['id']}.{self.config['datasets']['transformed']}.games"
        )

    def _check_completeness(self, table_id: str) -> Dict[str, Any]:
        """
        Check data completeness for a given table.
        
        Args:
            table_id: BigQuery table to check
        
        Returns:
            Completeness metrics dictionary
        """
        try:
            # Count total rows and rows with non-null values for key columns
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNTIF(game_id IS NOT NULL) as non_null_game_ids,
                COUNTIF(name IS NOT NULL) as non_null_names,
                COUNTIF(year_published IS NOT NULL) as non_null_years
            FROM `{table_id}`
            """
            
            query_job = self.bq_client.query(query)
            results = list(query_job)[0]
            
            return {
                'total_rows': results['total_rows'],
                'game_id_completeness': results['non_null_game_ids'] / results['total_rows'] if results['total_rows'] > 0 else 0,
                'name_completeness': results['non_null_names'] / results['total_rows'] if results['total_rows'] > 0 else 0,
                'year_published_completeness': results['non_null_years'] / results['total_rows'] if results['total_rows'] > 0 else 0
            }
        except Exception as e:
            logger.error(f"Completeness check failed: {e}")
            return {}

    def _check_consistency(self, table_id: str) -> Dict[str, Any]:
        """
        Check data consistency for a given table.
        
        Args:
            table_id: BigQuery table to check
        
        Returns:
            Consistency metrics dictionary
        """
        try:
            # Check for duplicate game IDs and invalid values
            query = f"""
            WITH game_stats AS (
                SELECT 
                    COUNT(*) as total_rows,
                    COUNTIF(min_players > max_players) as invalid_player_count,
                    COUNTIF(year_published < 1800 OR year_published > EXTRACT(YEAR FROM CURRENT_DATE())) as invalid_year_count,
                    (SELECT COUNT(DISTINCT game_id) FROM `{table_id}`) as unique_game_ids
            )
            SELECT 
                total_rows,
                unique_game_ids,
                invalid_player_count,
                invalid_year_count
            FROM game_stats
            """
            
            query_job = self.bq_client.query(query)
            results = list(query_job)[0]
            
            return {
                'total_rows': results['total_rows'],
                'unique_game_ids': results['unique_game_ids'],
                'duplicate_game_ids_ratio': 1 - (results['unique_game_ids'] / results['total_rows']) if results['total_rows'] > 0 else 0,
                'invalid_player_range_ratio': results['invalid_player_count'] / results['total_rows'] if results['total_rows'] > 0 else 0,
                'invalid_year_ratio': results['invalid_year_count'] / results['total_rows'] if results['total_rows'] > 0 else 0
            }
        except Exception as e:
            logger.error(f"Consistency check failed: {e}")
            return {}

    def _check_timeliness(self, table_id: str) -> Dict[str, Any]:
        """
        Check data timeliness for a given table.
        
        Args:
            table_id: BigQuery table to check
        
        Returns:
            Timeliness metrics dictionary
        """
        try:
            # Check data age and update frequency
            query = f"""
            SELECT 
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(processing_timestamp), HOUR) as hours_since_last_update,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(processing_timestamp), DAY) as days_of_data_span
            FROM `{table_id}`
            """
            
            query_job = self.bq_client.query(query)
            results = list(query_job)[0]
            
            return {
                'hours_since_last_update': results['hours_since_last_update'],
                'days_of_data_span': results['days_of_data_span']
            }
        except Exception as e:
            logger.error(f"Timeliness check failed: {e}")
            return {}

    def run_quality_checks(self, 
                            tables: Optional[List[str]] = None, 
                            check_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run comprehensive data quality checks.
        
        Args:
            tables: Optional list of tables to check (defaults to configured tables)
            check_types: Optional list of check types to run
        
        Returns:
            Comprehensive data quality report
        """
        start_time = datetime.now(UTC)
        
        # Default to configured tables if not specified
        if not tables:
            tables = [
                self.raw_responses_table,
                self.processed_games_table
            ]
        
        # Default check types
        if not check_types:
            check_types = ['completeness', 'consistency', 'timeliness']
        
        quality_results = {
            'tables': {},
            'overall_quality_score': 1.0,
            'critical_issues': [],
            'timestamp': start_time.isoformat()
        }
        
        for table in tables:
            table_results = {}
            
            if 'completeness' in check_types:
                table_results['completeness'] = self._check_completeness(table)
            
            if 'consistency' in check_types:
                table_results['consistency'] = self._check_consistency(table)
            
            if 'timeliness' in check_types:
                table_results['timeliness'] = self._check_timeliness(table)
            
            # Calculate table-level quality score
            table_score = self._calculate_quality_score(table_results)
            table_results['quality_score'] = table_score
            
            # Track critical issues
            if table_score < 0.8:
                quality_results['critical_issues'].append({
                    'table': table,
                    'quality_score': table_score
                })
            
            quality_results['tables'][table] = table_results
        
        # Calculate overall quality score
        if quality_results['tables']:
            quality_results['overall_quality_score'] = sum(
                table_result.get('quality_score', 0) 
                for table_result in quality_results['tables'].values()
            ) / len(quality_results['tables'])
        
        return quality_results

    def _calculate_quality_score(self, table_results: Dict[str, Any]) -> float:
        """
        Calculate a quality score based on various metrics.
        
        Args:
            table_results: Dictionary of quality check results
        
        Returns:
            Calculated quality score (0-1)
        """
        try:
            # Default weights for different quality dimensions
            weights = {
                'completeness': {
                    'game_id_completeness': 0.4,
                    'name_completeness': 0.3,
                    'year_published_completeness': 0.3
                },
                'consistency': {
                    'duplicate_game_ids_ratio': -0.3,
                    'invalid_player_range_ratio': -0.3,
                    'invalid_year_ratio': -0.4
                }
            }
            
            score = 1.0
            
            # Completeness score
            if 'completeness' in table_results:
                completeness = table_results['completeness']
                score *= (
                    weights['completeness']['game_id_completeness'] * completeness.get('game_id_completeness', 0) +
                    weights['completeness']['name_completeness'] * completeness.get('name_completeness', 0) +
                    weights['completeness']['year_published_completeness'] * completeness.get('year_published_completeness', 0)
                )
            
            # Consistency score
            if 'consistency' in table_results:
                consistency = table_results['consistency']
                score += (
                    weights['consistency']['duplicate_game_ids_ratio'] * consistency.get('duplicate_game_ids_ratio', 0) +
                    weights['consistency']['invalid_player_range_ratio'] * consistency.get('invalid_player_range_ratio', 0) +
                    weights['consistency']['invalid_year_ratio'] * consistency.get('invalid_year_ratio', 0)
                )
            
            # Clip score between 0 and 1
            return max(0, min(1, score))
        
        except Exception as e:
            logger.error(f"Quality score calculation failed: {e}")
            return 0.5  # Default neutral score

def main() -> None:
    """Main entry point for data quality monitoring."""
    monitor = DataQualityMonitor()
    quality_results = monitor.run_quality_checks()
    
    # Log quality results
    logger.info("Data Quality Results:")
    logger.info(f"Overall Quality Score: {quality_results['overall_quality_score']}")
    logger.info(f"Critical Issues: {quality_results['critical_issues']}")

if __name__ == "__main__":
    main()
