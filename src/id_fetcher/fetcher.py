"""Module for fetching and managing BoardGameGeek IDs."""

import datetime
import logging
from pathlib import Path
from typing import List, Set
from urllib.error import URLError
from urllib.request import urlretrieve

import polars as pl
from google.cloud import bigquery

from ..config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BGGIDFetcher:
    """Fetches and manages BoardGameGeek IDs."""

    BGG_IDS_URL = "http://bgg.activityclub.org/bggdata/thingids.txt"
    
    def __init__(self) -> None:
        """Initialize the fetcher with BigQuery configuration."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client(project=self.config["project"]["id"])
        self.dataset_id = self.config["datasets"]["raw"]
        self.table_id = self.config["tables"]["raw"]["thing_ids"]

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
            logger.info("Downloading BGG IDs from %s", self.BGG_IDS_URL)
            urlretrieve(self.BGG_IDS_URL, output_path)
            logger.info("Downloaded BGG IDs to %s", output_path)
            return output_path
        except URLError as e:
            logger.error("Failed to download BGG IDs: %s", e)
            raise

    def parse_ids(self, file_path: Path) -> Set[int]:
        """Parse game IDs from the downloaded file.
        
        Args:
            file_path: Path to the IDs file
            
        Returns:
            Set of game IDs
        """
        logger.info("Parsing game IDs from %s", file_path)
        with open(file_path, "r") as f:
            content = f.read()
            logger.info("File content: %s", content[:1000])  # Print first 1000 chars
            # File contains "ID boardgame" per line
            ids = {int(line.split()[0]) for line in content.splitlines() if line.strip() and line.split()[0].isdigit()}
        logger.info("Found %d game IDs", len(ids))
        return ids

    def get_existing_ids(self) -> Set[int]:
        """Get existing game IDs from BigQuery.
        
        Returns:
            Set of existing game IDs
        """
        query = f"""
        SELECT DISTINCT game_id
        FROM `{self.config['project']['id']}.{self.dataset_id}.{self.table_id}`
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            existing_ids = set(df["game_id"].tolist())
            logger.info("Found %d existing game IDs in BigQuery", len(existing_ids))
            return existing_ids
        except Exception as e:
            logger.error("Failed to fetch existing IDs: %s", e)
            return set()

    def upload_new_ids(self, new_ids: Set[int]) -> None:
        """Upload new game IDs to BigQuery.
        
        Args:
            new_ids: Set of new game IDs to upload
        """
        if not new_ids:
            logger.info("No new IDs to upload")
            return

        # Create DataFrame with new IDs
        now = datetime.datetime.utcnow()
        df = pl.DataFrame({
            "game_id": list(new_ids),
            "processed": [False] * len(new_ids),
            "process_timestamp": [None] * len(new_ids),
            "source": ["bgg.activityclub.org"] * len(new_ids),
            "load_timestamp": [now] * len(new_ids)
        })

        # Convert to pandas for BigQuery upload
        pandas_df = df.to_pandas()
        
        # Upload to BigQuery
        table_ref = f"{self.config['project']['id']}.{self.dataset_id}.{self.table_id}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        try:
            job = self.client.load_table_from_dataframe(
                pandas_df, table_ref, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logger.info("Uploaded %d new IDs to BigQuery", len(new_ids))
        except Exception as e:
            logger.error("Failed to upload new IDs: %s", e)
            raise

    def update_ids(self, temp_dir: Path) -> None:
        """Update game IDs in BigQuery with new IDs from BGG.
        
        Args:
            temp_dir: Directory for temporary files
        """
        # Download and parse IDs
        ids_file = self.download_ids(temp_dir)
        all_ids = self.parse_ids(ids_file)
        
        # Get existing IDs and find new ones
        existing_ids = self.get_existing_ids()
        new_ids = all_ids - existing_ids
        
        if new_ids:
            logger.info("Found %d new game IDs", len(new_ids))
            self.upload_new_ids(new_ids)
        else:
            logger.info("No new game IDs found")

def main() -> None:
    """Main function to update game IDs."""
    temp_dir = Path("temp")
    fetcher = BGGIDFetcher()
    
    try:
        fetcher.update_ids(temp_dir)
    except Exception as e:
        logger.error("Failed to update game IDs: %s", e)
        raise
    finally:
        # Cleanup
        if temp_dir.exists():
            for file in temp_dir.glob("*"):
                file.unlink()
            temp_dir.rmdir()

if __name__ == "__main__":
    main()
