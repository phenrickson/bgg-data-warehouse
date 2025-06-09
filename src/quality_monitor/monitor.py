"""Module for monitoring data quality in the BGG data warehouse."""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
        self.dataset_id = self.config["project"]["dataset"]

    def _log_check_result(
        self,
        check_name: str,
        table_name: str,
        passed: bool,
        records_checked: int,
        failed_records: int,
        details: str
    ) -> None:
        """Log a quality check result.
        
        Args:
            check_name: Name of the quality check
            table_name: Name of the table checked
            passed: Whether the check passed
            records_checked: Number of records checked
            failed_records: Number of records that failed
            details: Additional details about the check
        """
        logger.info(
            "Quality check '%s' on table '%s': %s (%d/%d records passed) - %s",
            check_name,
            table_name,
            "PASSED" if passed else "FAILED",
            records_checked - failed_records,
            records_checked,
            details
        )

    def check_referential_integrity(self) -> bool:
        """Check referential integrity between games and related tables.
        
        Returns:
            True if check passes, False otherwise
        """
        bridge_tables = [
            ("game_categories", "category_id", "categories"),
            ("game_mechanics", "mechanic_id", "mechanics"),
            ("game_families", "family_id", "families"),
            ("game_designers", "designer_id", "designers"),
            ("game_artists", "artist_id", "artists"),
            ("game_publishers", "publisher_id", "publishers")
        ]
        
        all_passed = True
        for bridge_table, fk_col, ref_table in bridge_tables:
            query = f"""
            WITH integrity_check AS (
                SELECT 
                    COUNT(*) as total_refs,
                    COUNTIF(ref.{fk_col} IS NULL) as broken_refs
                FROM `{self.config['project']['id']}.{self.dataset_id}.{bridge_table}` bridge
                LEFT JOIN `{self.config['project']['id']}.{self.dataset_id}.{ref_table}` ref
                ON bridge.{fk_col} = ref.{fk_col}
            )
            SELECT *
            FROM integrity_check
            """
            
            try:
                df = self.client.query(query).to_dataframe()
                total_refs = int(df["total_refs"].iloc[0])
                broken_refs = int(df["broken_refs"].iloc[0])
                
                passed = broken_refs == 0
                all_passed = all_passed and passed
                
                details = f"Found {broken_refs} broken references to {ref_table}"
                
                self._log_check_result(
                    check_name="referential_integrity",
                    table_name=bridge_table,
                    passed=passed,
                    records_checked=total_refs,
                    failed_records=broken_refs,
                    details=details
                )
                
            except Exception as e:
                logger.error("Referential integrity check failed for %s: %s", bridge_table, e)
                all_passed = False
        
        return all_passed

    def check_data_freshness(self) -> bool:
        """Check if data is being regularly updated.
        
        Returns:
            True if check passes, False otherwise
        """
        query = """
        SELECT
            COUNT(*) as total_games,
            TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(),
                MAX(load_timestamp),
                HOUR
            ) as hours_since_update
        FROM `{}.{}.games`
        """.format(self.config['project']['id'], self.dataset_id)
        
        try:
            df = self.client.query(query).to_dataframe()
            total_games = int(df["total_games"].iloc[0])
            hours_since_update = int(df["hours_since_update"].iloc[0])
            
            passed = hours_since_update <= 24  # Consider data stale after 24 hours
            details = f"Last update was {hours_since_update} hours ago"
            
            self._log_check_result(
                check_name="freshness",
                table_name="games",
                passed=passed,
                records_checked=total_games,
                failed_records=0,
                details=details
            )
            
            return passed
            
        except Exception as e:
            logger.error("Freshness check failed: %s", e)
            return False

    def check_data_completeness(self) -> bool:
        """Check for completeness of game data.
        
        Returns:
            True if check passes, False otherwise
        """
        checks = [
            ("primary_name", "games with missing names"),
            ("year_published", "games with missing year"),
            ("min_players", "games with missing player count"),
            ("description", "games with missing description")
        ]
        
        all_passed = True
        for column, description in checks:
            query = f"""
            SELECT
                COUNT(*) as total_games,
                COUNTIF({column} IS NULL) as null_count
            FROM `{self.config['project']['id']}.{self.dataset_id}.games`
            """
            
            try:
                df = self.client.query(query).to_dataframe()
                total_games = int(df["total_games"].iloc[0])
                null_count = int(df["null_count"].iloc[0])
                
                passed = null_count == 0
                all_passed = all_passed and passed
                
                details = f"Found {null_count} {description}"
                
                self._log_check_result(
                    check_name=f"completeness_{column}",
                    table_name="games",
                    passed=passed,
                    records_checked=total_games,
                    failed_records=null_count,
                    details=details
                )
                
            except Exception as e:
                logger.error("Completeness check failed for %s: %s", column, e)
                all_passed = False
        
        return all_passed

    def check_data_consistency(self) -> bool:
        """Check for data consistency across tables.
        
        Returns:
            True if check passes, False otherwise
        """
        query = """
        WITH consistency_check AS (
            SELECT
                COUNT(DISTINCT g.game_id) as total_games,
                COUNT(DISTINCT an.game_id) as games_with_alt_names,
                COUNT(DISTINCT pc.game_id) as games_with_player_counts,
                COUNT(DISTINCT ld.game_id) as games_with_language_dep,
                COUNT(DISTINCT sa.game_id) as games_with_age_suggestions,
                COUNT(DISTINCT r.game_id) as games_with_rankings
            FROM `{}.{}.games` g
            LEFT JOIN `{}.{}.alternate_names` an ON g.game_id = an.game_id
            LEFT JOIN `{}.{}.player_counts` pc ON g.game_id = pc.game_id
            LEFT JOIN `{}.{}.language_dependence` ld ON g.game_id = ld.game_id
            LEFT JOIN `{}.{}.suggested_ages` sa ON g.game_id = sa.game_id
            LEFT JOIN `{}.{}.rankings` r ON g.game_id = r.game_id
        )
        SELECT *
        FROM consistency_check
        """.format(*(self.config['project']['id'], self.dataset_id) * 6)
        
        try:
            df = self.client.query(query).to_dataframe()
            total_games = int(df["total_games"].iloc[0])
            
            # Check each related table
            checks = {
                "alternate_names": df["games_with_alt_names"].iloc[0],
                "player_counts": df["games_with_player_counts"].iloc[0],
                "language_dependence": df["games_with_language_dep"].iloc[0],
                "suggested_ages": df["games_with_age_suggestions"].iloc[0],
                "rankings": df["games_with_rankings"].iloc[0]
            }
            
            all_passed = True
            for table_name, count in checks.items():
                # Consider it passed if at least 90% of games have related data
                passed = (count / total_games) >= 0.9 if total_games > 0 else True
                all_passed = all_passed and passed
                
                details = f"{count}/{total_games} games have {table_name} data"
                
                self._log_check_result(
                    check_name=f"consistency_{table_name}",
                    table_name="games",
                    passed=passed,
                    records_checked=total_games,
                    failed_records=total_games - int(count),
                    details=details
                )
            
            return all_passed
            
        except Exception as e:
            logger.error("Consistency check failed: %s", e)
            return False

    def get_data_summary(self) -> Dict[str, Any]:
        """Get a summary of the data in the warehouse.
        
        Returns:
            Dictionary containing summary statistics
        """
        query = """
        SELECT
            -- Basic counts
            (SELECT COUNT(*) FROM `{}.{}.games`) as total_games,
            (SELECT COUNT(*) FROM `{}.{}.categories`) as total_categories,
            (SELECT COUNT(*) FROM `{}.{}.mechanics`) as total_mechanics,
            (SELECT COUNT(*) FROM `{}.{}.families`) as total_families,
            (SELECT COUNT(*) FROM `{}.{}.designers`) as total_designers,
            (SELECT COUNT(*) FROM `{}.{}.artists`) as total_artists,
            (SELECT COUNT(*) FROM `{}.{}.publishers`) as total_publishers,
            
            -- Game statistics
            (SELECT AVG(average_rating) FROM `{}.{}.games`) as avg_rating,
            (SELECT AVG(owned_count) FROM `{}.{}.games`) as avg_owned,
            (SELECT AVG(year_published) FROM `{}.{}.games` WHERE year_published IS NOT NULL) as avg_year,
            
            -- Latest update
            (SELECT MAX(load_timestamp) FROM `{}.{}.games`) as last_update
        """.format(*(self.config['project']['id'], self.dataset_id) * 11)
        
        try:
            df = self.client.query(query).to_dataframe()
            return df.iloc[0].to_dict()
        except Exception as e:
            logger.error("Failed to get data summary: %s", e)
            return {}

    def run_all_checks(self) -> Dict[str, bool]:
        """Run all quality checks.
        
        Returns:
            Dictionary mapping check names to their results
        """
        results = {
            "referential_integrity": self.check_referential_integrity(),
            "data_freshness": self.check_data_freshness(),
            "data_completeness": self.check_data_completeness(),
            "data_consistency": self.check_data_consistency()
        }
        
        # Log overall results
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        logger.info(
            "Quality checks completed: %d/%d checks passed (%.2f%%)",
            passed,
            total,
            (passed/total)*100 if total > 0 else 0
        )
        
        # Get and log data summary
        summary = self.get_data_summary()
        if summary:
            logger.info("\nData Warehouse Summary:")
            logger.info("=====================")
            logger.info(f"Total Games: {summary['total_games']}")
            logger.info(f"Categories: {summary['total_categories']}")
            logger.info(f"Mechanics: {summary['total_mechanics']}")
            logger.info(f"Families: {summary['total_families']}")
            logger.info(f"Designers: {summary['total_designers']}")
            logger.info(f"Artists: {summary['total_artists']}")
            logger.info(f"Publishers: {summary['total_publishers']}")
            logger.info(f"Average Rating: {summary['avg_rating']:.2f}")
            logger.info(f"Average Owned: {summary['avg_owned']:.0f}")
            logger.info(f"Average Year: {summary['avg_year']:.0f}")
            logger.info(f"Last Update: {summary['last_update']}")
        
        return results

def main() -> None:
    """Main function to run quality checks."""
    monitor = DataQualityMonitor()
    monitor.run_all_checks()

if __name__ == "__main__":
    main()
