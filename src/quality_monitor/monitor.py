"""Module for monitoring data quality in the BGG Data Warehouse."""

import logging
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List, Union

from google.cloud import bigquery
import polars as pl

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
        self.client = bigquery.Client()
        
        # Configure monitoring dataset
        self.monitoring_dataset = f"{self.config['project']['id']}.bgg_monitoring_dev"
        
        # Configure tables from config
        self.raw_responses_table = f"{self.config['project']['id']}.bgg_raw_dev.raw_responses"
        self.games_table = f"{self.config['project']['id']}.bgg_data_dev.games"

    def _log_check_result(
        self,
        check_name: str,
        table_name: str,
        passed: bool,
        records_checked: int,
        failed_records: int,
        details: str
    ) -> None:
        """Log check results to monitoring table.
        
        Args:
            check_name: Name of the quality check
            table_name: Name of the table checked
            passed: Whether the check passed
            records_checked: Number of records checked
            failed_records: Number of records that failed the check
            details: Additional details about the check
        """
        row = {
            "timestamp": datetime.now(UTC).isoformat(),
            "check_name": check_name,
            "table_name": table_name,
            "check_status": "PASSED" if passed else "FAILED",
            "records_checked": int(records_checked) if records_checked is not None else 0,
            "failed_records": int(failed_records) if failed_records is not None else 0,
            "details": details
        }
        
        table_id = f"{self.monitoring_dataset}.quality_check_results"
        errors = self.client.insert_rows_json(table_id, [row])
        
        if errors:
            logger.error(f"Error logging check result: {errors}")

    def check_completeness(
        self,
        table_name: str,
        required_columns: List[str],
        threshold: float = 0.99
    ) -> bool:
        """Check for completeness of required columns.
        
        Args:
            table_name: Name of the table to check
            required_columns: List of columns that should not be null
            threshold: Minimum acceptable completeness ratio (0-1)
            
        Returns:
            True if completeness check passes, False otherwise
        """
        columns_str = ", ".join(required_columns)
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNTIF({" IS NULL OR ".join(required_columns)} IS NULL) as null_records
        FROM `{table_name}`
        """
        
        query_job = self.client.query(query)
        df = pl.from_pandas(query_job.to_dataframe())
        
        total_records = df["total_records"][0]
        null_records = df["null_records"][0]
        completeness_ratio = 1 - (null_records / total_records) if total_records > 0 else 0
        
        passed = completeness_ratio >= threshold
        
        self._log_check_result(
            check_name="completeness",
            table_name=table_name,
            passed=passed,
            records_checked=total_records,
            failed_records=null_records,
            details=f"Completeness ratio: {completeness_ratio:.2%} (threshold: {threshold:.2%})"
        )
        
        return passed

    def check_freshness(
        self,
        table_name: str,
        hours: int = 24
    ) -> bool:
        """Check data freshness based on last update time.
        
        Args:
            table_name: Name of the table to check
            hours: Maximum acceptable hours since last update
            
        Returns:
            True if freshness check passes, False otherwise
        """
        # Use appropriate timestamp field based on table
        timestamp_field = "load_timestamp" if "games" in table_name else "fetch_timestamp"
        query = f"""
        SELECT
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({timestamp_field}), HOUR) as hours_since_update,
            COUNT(*) as total_records
        FROM `{table_name}`
        """
        
        query_job = self.client.query(query)
        df = pl.from_pandas(query_job.to_dataframe())
        
        hours_since_update = df["hours_since_update"][0]
        total_records = df["total_records"][0]
        
        passed = hours_since_update <= hours
        
        self._log_check_result(
            check_name="freshness",
            table_name=table_name,
            passed=passed,
            records_checked=total_records,
            failed_records=0,
            details=f"Hours since last update: {hours_since_update} (threshold: {hours})"
        )
        
        return passed

    def check_validity(
        self,
        table_name: str,
        conditions: Optional[List[str]] = None
    ) -> bool:
        """Check data validity based on business rules.
        
        Args:
            table_name: Name of the table to check
            conditions: Optional list of SQL conditions for valid records
            
        Returns:
            True if validity check passes, False otherwise
        """
        if not conditions:
            conditions = [
                "year_published BETWEEN 1800 AND EXTRACT(YEAR FROM CURRENT_DATE())",
                "min_players <= max_players",
                "average_rating BETWEEN 1 AND 10"
            ]
        
        conditions_str = " AND ".join(conditions)
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNTIF(NOT ({conditions_str})) as invalid_records
        FROM `{table_name}`
        """
        
        query_job = self.client.query(query)
        df = pl.from_pandas(query_job.to_dataframe())
        
        total_records = df["total_records"][0]
        invalid_records = df["invalid_records"][0]
        
        passed = invalid_records == 0
        
        self._log_check_result(
            check_name="validity",
            table_name=table_name,
            passed=passed,
            records_checked=total_records,
            failed_records=invalid_records,
            details=f"Found {invalid_records} invalid records"
        )
        
        return passed

    def run_all_checks(self) -> Dict[str, bool]:
        """Run all quality checks.
        
        Returns:
            Dictionary mapping check names to their results
        """
        results = {}
        
        # Check completeness
        results["games_completeness"] = self.check_completeness(
            self.games_table,
            ["game_id", "primary_name", "year_published"]
        )
        
        results["responses_completeness"] = self.check_completeness(
            self.raw_responses_table,
            ["game_id", "response_data", "fetch_timestamp"]
        )
        
        # Check freshness
        results["games_freshness"] = self.check_freshness(self.games_table)
        results["responses_freshness"] = self.check_freshness(self.raw_responses_table)
        
        # Check validity
        results["games_validity"] = self.check_validity(self.games_table)
        
        return results

def main() -> None:
    """Main entry point for data quality monitoring."""
    monitor = DataQualityMonitor()
    results = monitor.run_all_checks()
    
    # Log overall results
    passed_checks = sum(1 for result in results.values() if result)
    total_checks = len(results)
    
    logger.info(f"Data Quality Check Results: {passed_checks}/{total_checks} checks passed")
    
    for check_name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"{check_name}: {status}")

if __name__ == "__main__":
    main()
