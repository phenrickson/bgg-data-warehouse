"""Module for monitoring data quality in the BGG data warehouse."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import polars as pl
from google.cloud import bigquery

from ..config import get_bigquery_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class DataQualityMonitor:
    """Monitors data quality in the BGG data warehouse."""

    def __init__(self) -> None:
        """Initialize the monitor with configuration."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client(project=self.config["project"]["id"])
        self.dataset_id = self.config["datasets"]["monitoring"]
        self.raw_dataset = self.config["datasets"]["raw"]

    def _log_check_result(
        self,
        check_name: str,
        table_name: str,
        passed: bool,
        records_checked: int,
        failed_records: int,
        details: str
    ) -> None:
        """Log a quality check result to BigQuery.
        
        Args:
            check_name: Name of the quality check
            table_name: Name of the table checked
            passed: Whether the check passed
            records_checked: Number of records checked
            failed_records: Number of records that failed
            details: Additional details about the check
        """
        row = {
            "check_timestamp": datetime.utcnow(),
            "check_name": check_name,
            "table_name": table_name,
            "check_status": "PASSED" if passed else "FAILED",
            "records_checked": records_checked,
            "failed_records": failed_records,
            "details": details,
        }

        table_ref = f"{self.config['project']['id']}.{self.dataset_id}.data_quality"
        errors = self.client.insert_rows_json(table_ref, [row])
        
        if errors:
            logger.error("Failed to log quality check result: %s", errors)

    def check_completeness(self, table_name: str, required_columns: List[str]) -> bool:
        """Check for null values in required columns.
        
        Args:
            table_name: Name of the table to check
            required_columns: List of columns that should not be null
            
        Returns:
            True if check passes, False otherwise
        """
        columns_str = ", ".join(required_columns)
        nulls_str = " OR ".join(f"{col} IS NULL" for col in required_columns)
        
        query = f"""
        WITH null_checks AS (
            SELECT COUNT(*) as total_records,
                   COUNTIF({nulls_str}) as null_records
            FROM `{self.config['project']['id']}.{self.raw_dataset}.{table_name}`
            WHERE DATE(load_timestamp) = CURRENT_DATE()
        )
        SELECT *
        FROM null_checks
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            total_records = int(df["total_records"].iloc[0])
            null_records = int(df["null_records"].iloc[0])
            
            passed = null_records == 0
            details = (
                f"Found {null_records} records with null values "
                f"in required columns: {columns_str}"
            )
            
            self._log_check_result(
                check_name="completeness",
                table_name=table_name,
                passed=passed,
                records_checked=total_records,
                failed_records=null_records,
                details=details,
            )
            
            return passed

        except Exception as e:
            logger.error("Completeness check failed: %s", e)
            return False

    def check_freshness(self, table_name: str, hours: int = 24) -> bool:
        """Check if data is being regularly updated.
        
        Args:
            table_name: Name of the table to check
            hours: Maximum age of data in hours
            
        Returns:
            True if check passes, False otherwise
        """
        query = f"""
        SELECT
            TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(),
                MAX(load_timestamp),
                HOUR
            ) as hours_since_update,
            COUNT(*) as total_records
        FROM `{self.config['project']['id']}.{self.raw_dataset}.{table_name}`
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            hours_since_update = int(df["hours_since_update"].iloc[0])
            total_records = int(df["total_records"].iloc[0])
            
            passed = hours_since_update <= hours
            details = f"Last update was {hours_since_update} hours ago"
            
            self._log_check_result(
                check_name="freshness",
                table_name=table_name,
                passed=passed,
                records_checked=total_records,
                failed_records=0,
                details=details,
            )
            
            return passed

        except Exception as e:
            logger.error("Freshness check failed: %s", e)
            return False

    def check_validity(self, table_name: str) -> bool:
        """Check for invalid values in key fields.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if check passes, False otherwise
        """
        validity_checks = {
            "games": """
                game_id <= 0 OR
                min_players < 0 OR
                max_players < min_players OR
                playing_time < 0 OR
                min_age < 0
            """,
            "request_log": """
                response_timestamp < request_timestamp OR
                retry_count < 0
            """,
            "thing_ids": """
                game_id <= 0
            """
        }
        
        if table_name not in validity_checks:
            logger.warning("No validity checks defined for table %s", table_name)
            return True
        
        query = f"""
        WITH validity_check AS (
            SELECT COUNT(*) as total_records,
                   COUNTIF({validity_checks[table_name]}) as invalid_records
            FROM `{self.config['project']['id']}.{self.raw_dataset}.{table_name}`
            WHERE DATE(load_timestamp) = CURRENT_DATE()
        )
        SELECT *
        FROM validity_check
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            total_records = int(df["total_records"].iloc[0])
            invalid_records = int(df["invalid_records"].iloc[0])
            
            passed = invalid_records == 0
            details = f"Found {invalid_records} records with invalid values"
            
            self._log_check_result(
                check_name="validity",
                table_name=table_name,
                passed=passed,
                records_checked=total_records,
                failed_records=invalid_records,
                details=details,
            )
            
            return passed

        except Exception as e:
            logger.error("Validity check failed: %s", e)
            return False

    def check_api_performance(self, hours: int = 24) -> bool:
        """Check API request performance and success rate.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            True if check passes, False otherwise
        """
        query = f"""
        WITH api_stats AS (
            SELECT
                COUNT(*) as total_requests,
                COUNTIF(success) as successful_requests,
                AVG(TIMESTAMP_DIFF(response_timestamp, request_timestamp, SECOND)) as avg_response_time,
                AVG(retry_count) as avg_retries
            FROM `{self.config['project']['id']}.{self.raw_dataset}.request_log`
            WHERE request_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        )
        SELECT *
        FROM api_stats
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            total_requests = int(df["total_requests"].iloc[0])
            successful_requests = int(df["successful_requests"].iloc[0])
            avg_response_time = float(df["avg_response_time"].iloc[0])
            avg_retries = float(df["avg_retries"].iloc[0])
            
            success_rate = (successful_requests / total_requests) if total_requests > 0 else 0
            passed = success_rate >= 0.95 and avg_response_time <= 5.0
            
            details = (
                f"Success rate: {success_rate:.2%}, "
                f"Avg response time: {avg_response_time:.2f}s, "
                f"Avg retries: {avg_retries:.2f}"
            )
            
            self._log_check_result(
                check_name="api_performance",
                table_name="request_log",
                passed=passed,
                records_checked=total_requests,
                failed_records=total_requests - successful_requests,
                details=details,
            )
            
            return passed

        except Exception as e:
            logger.error("API performance check failed: %s", e)
            return False

    def run_all_checks(self) -> Dict[str, bool]:
        """Run all quality checks.
        
        Returns:
            Dictionary mapping check names to their results
        """
        results = {}
        
        # Completeness checks
        results["games_completeness"] = self.check_completeness(
            "games", ["game_id", "name"]
        )
        results["request_log_completeness"] = self.check_completeness(
            "request_log", ["request_id", "request_timestamp"]
        )
        results["thing_ids_completeness"] = self.check_completeness(
            "thing_ids", ["game_id"]
        )
        
        # Freshness checks
        for table in ["games", "request_log", "thing_ids"]:
            results[f"{table}_freshness"] = self.check_freshness(table)
        
        # Validity checks
        for table in ["games", "request_log", "thing_ids"]:
            results[f"{table}_validity"] = self.check_validity(table)
        
        # API performance check
        results["api_performance"] = self.check_api_performance()
        
        return results

def main() -> None:
    """Main function to run quality checks."""
    monitor = DataQualityMonitor()
    results = monitor.run_all_checks()
    
    # Log overall results
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    logger.info(
        "Quality checks completed: %d/%d checks passed (%.2f%%)",
        passed,
        total,
        (passed/total)*100 if total > 0 else 0
    )
    
    # Log failed checks
    failed = [name for name, passed in results.items() if not passed]
    if failed:
        logger.warning("Failed checks: %s", ", ".join(failed))

if __name__ == "__main__":
    main()
