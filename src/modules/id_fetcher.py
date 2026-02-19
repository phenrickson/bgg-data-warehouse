"""Module for fetching and managing BoardGameGeek IDs."""

import datetime
import logging
import os
from pathlib import Path
from typing import List, Set

import polars as pl
from dotenv import load_dotenv
from google.cloud import bigquery

from ..config import get_bigquery_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hardcoded table names (managed by Terraform)
RAW_DATASET = "raw"
THING_IDS_TABLE = "thing_ids"


class IDFetcher:
    """Fetches and manages BoardGameGeek IDs."""

    def __init__(self) -> None:
        """Initialize the fetcher with BigQuery configuration."""
        self.config = get_bigquery_config()
        self.project_id = self.config["project"]["id"]
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = RAW_DATASET
        self.table_id = THING_IDS_TABLE

    def get_existing_ids(self) -> Set[tuple]:
        """Get existing game IDs and types from BigQuery.

        Returns:
            Set of tuples containing (game_id, type)
        """
        query = f"""
        SELECT DISTINCT game_id, type
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """

        try:
            df = self.client.query(query).to_dataframe()
            existing_ids = {(row["game_id"], row["type"]) for _, row in df.iterrows()}
            logger.info("Found %d existing game IDs in BigQuery", len(existing_ids))
            return existing_ids
        except Exception as e:
            logger.error("Failed to fetch existing IDs: %s", e)
            return set()

    def upload_new_ids(self, new_games: List[dict]) -> None:
        """Upload new game IDs to BigQuery.

        Args:
            new_games: List of dictionaries containing game IDs and types to upload
        """
        if not new_games:
            logger.info("No new IDs to upload")
            return

        # Create temp table for new data
        temp_table = f"{self.project_id}.{self.dataset_id}.temp_thing_ids"

        # Create DataFrame with new IDs
        now = datetime.datetime.now(datetime.UTC)
        df = pl.DataFrame(
            {
                "game_id": [game["game_id"] for game in new_games],
                "type": [game["type"] for game in new_games],
                "processed": [False] * len(new_games),
                "process_timestamp": [None] * len(new_games),
                "source": ["bgg_sitemap"] * len(new_games),
                "load_timestamp": [now] * len(new_games),
            }
        )

        # Log DataFrame info before upload
        logger.info("DataFrame preview:")
        try:
            preview = str(df.head())
            preview = preview.encode("cp1252", errors="replace").decode("cp1252")
            logger.info(preview)
        except Exception as e:
            logger.info("Could not display DataFrame preview: %s", e)

        try:
            # Load data to temp table with schema
            pandas_df = df.to_pandas()
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("processed", "BOOLEAN", mode="REQUIRED"),
                    bigquery.SchemaField("process_timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
                ],
            )
            job = self.client.load_table_from_dataframe(
                pandas_df, temp_table, job_config=job_config
            )
            job.result()

            # Merge into main table
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{self.table_id}` T
            USING `{temp_table}` S
            ON T.game_id = S.game_id AND T.type = S.type
            WHEN NOT MATCHED THEN
              INSERT (game_id, type, processed, process_timestamp, source, load_timestamp)
              VALUES (game_id, type, processed, process_timestamp, source, load_timestamp)
            """

            self.client.query(merge_query).result()
            logger.info("Merged %d new IDs into BigQuery", len(new_games))

        except Exception as e:
            logger.error("Failed to upload new IDs: %s", e)
            raise

        finally:
            # Clean up temp table
            self.client.delete_table(temp_table, not_found_ok=True)

    def run(self, use_browser: bool = True) -> bool:
        """Run the ID fetcher pipeline.

        Args:
            use_browser: Use browser-based fetching (default True, legacy param for compatibility)

        Returns:
            bool: True if new IDs were found and added, False otherwise
        """
        logger.info("Starting ID fetcher")

        try:
            # Fetch IDs from BGG sitemaps
            all_games = self._fetch_via_browser()

            if not all_games:
                logger.warning("No games fetched from BGG")
                return False

            # Get existing IDs and find new ones
            existing_ids = self.get_existing_ids()
            new_games = [
                game for game in all_games if (game["game_id"], game["type"]) not in existing_ids
            ]

            if new_games:
                logger.info("Found %d new game IDs", len(new_games))
                self.upload_new_ids(new_games)
                return True
            else:
                logger.info("No new game IDs found")
                return False

        except Exception as e:
            logger.error(f"ID fetcher failed: {e}")
            raise

    def _fetch_via_browser(self) -> List[dict]:
        """Fetch game IDs directly from BGG using browser automation.

        Returns:
            List of game dicts with game_id and type
        """
        try:
            from .id_fetcher_browser import BrowserIDFetcher
        except ImportError:
            logger.error(
                "Browser-based fetching requires playwright. "
                "Install with: uv add playwright && playwright install chromium"
            )
            raise

        headless = os.getenv("BROWSER_HEADLESS", "true").lower() == "true"
        fetcher = BrowserIDFetcher(headless=headless)
        return fetcher.fetch_all_ids()
