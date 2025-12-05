"""Module for fetching and managing BoardGameGeek IDs."""

import datetime
import logging
from pathlib import Path
from typing import List, Set, Dict, Optional
from urllib.error import URLError
from urllib.request import urlretrieve

import polars as pl
from dotenv import load_dotenv
from google.cloud import bigquery

from ..config import get_bigquery_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IDFetcher:
    """Fetches and manages BoardGameGeek IDs."""

    BGG_IDS_URL = "http://bgg.activityclub.org/bggdata/thingids.txt"

    def __init__(self, environment: str = "prod", config: Optional[Dict] = None) -> None:
        """Initialize the fetcher with BigQuery configuration.

        Args:
            environment: Environment to use (prod/dev/test)
            config: Optional configuration dictionary
        """
        self.config = config or get_bigquery_config(environment)
        self.environment = environment
        self.client = bigquery.Client(project=self.config["project"]["id"])
        self.dataset_id = self.config["datasets"]["raw"]
        self.table_id = self.config["raw_tables"]["thing_ids"]["name"]

    def download_ids(self, output_dir: Path) -> Path:
        """Download the BGG IDs file.

        Args:
            output_dir: Directory to save the downloaded file

        Returns:
            Path to the downloaded file

        Raises:
            URLError: If the download fails
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "thingids.txt"

        try:
            logger.info(
                "Downloading BGG IDs from %s to %s", self.BGG_IDS_URL, output_path.absolute()
            )
            urlretrieve(self.BGG_IDS_URL, output_path)
            logger.info("Successfully downloaded BGG IDs to %s", output_path.absolute())
            return output_path
        except URLError as e:
            logger.error("Failed to download BGG IDs: %s", e)
            raise

    def parse_ids(self, file_path: Path) -> List[dict]:
        """Parse game IDs and types from the downloaded file.

        Args:
            file_path: Path to the IDs file

        Returns:
            List of dictionaries containing game IDs and their types
        """
        logger.info("Parsing game IDs from %s", file_path)
        games = []
        with open(file_path, "r") as f:
            content = f.read()
            logger.debug(
                "File content: %s", content[:1000]
            )  # Print first 1000 chars only in debug mode
            # File contains "ID type" per line (e.g., "12345 boardgame" or "67890 boardgameexpansion")
            for line in content.splitlines():
                if line.strip() and len(line.split()) >= 2:
                    parts = line.split()
                    if parts[0].isdigit():
                        games.append({"game_id": int(parts[0]), "type": parts[1]})
        logger.info("Found %d game IDs", len(games))
        return games

    def get_existing_ids(self) -> Set[tuple]:
        """Get existing game IDs and types from BigQuery.

        Returns:
            Set of tuples containing (game_id, type)
        """
        query = f"""
        SELECT DISTINCT game_id, type
        FROM `{self.config['project']['id']}.{self.dataset_id}.{self.table_id}`
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
        temp_table = f"{self.config['project']['id']}.{self.dataset_id}.temp_thing_ids"

        # Create DataFrame with new IDs
        now = datetime.datetime.now(datetime.UTC)
        df = pl.DataFrame(
            {
                "game_id": [game["game_id"] for game in new_games],
                "type": [game["type"] for game in new_games],
                "processed": [False] * len(new_games),
                "process_timestamp": [None] * len(new_games),
                "source": ["bgg.activityclub.org"] * len(new_games),
                "load_timestamp": [now] * len(new_games),
            }
        )

        # Log DataFrame info before upload
        logger.info("DataFrame preview:")
        try:
            # Convert to string and encode safely for Windows console
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
            MERGE `{self.config['project']['id']}.{self.dataset_id}.{self.table_id}` T
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

    def run(self) -> bool:
        """Run the ID fetcher pipeline.

        Returns:
            bool: True if new IDs were found and added, False otherwise
        """
        logger.info(f"Starting ID fetcher in {self.environment} environment")

        # Create temp directory for ID updates
        temp_dir = Path("temp")
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Download and parse IDs
            ids_file = self.download_ids(temp_dir)
            all_games = self.parse_ids(ids_file)

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
        finally:
            # Cleanup
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()
