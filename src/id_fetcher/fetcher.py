"""Module for fetching and managing BoardGameGeek IDs."""

import datetime
import logging
from pathlib import Path
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

    def __init__(self, config: dict | None = None) -> None:
        """Initialize the fetcher with BigQuery configuration.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or get_bigquery_config()
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
            logger.info("Downloading BGG IDs from %s", self.BGG_IDS_URL)
            urlretrieve(self.BGG_IDS_URL, output_path)
            logger.info("Downloaded BGG IDs to %s", output_path)
            return output_path
        except URLError as e:
            logger.error("Failed to download BGG IDs: %s", e)
            raise

    def parse_ids(self, file_path: Path) -> list[dict]:
        """Parse game IDs and types from the downloaded file.

        Args:
            file_path: Path to the IDs file

        Returns:
            List of dictionaries containing game IDs and their types
        """
        logger.info("Parsing game IDs from %s", file_path)
        games = []
        with open(file_path) as f:
            content = f.read()
            logger.info("File content: %s", content[:1000])  # Print first 1000 chars
            # File contains "ID type" per line (e.g., "12345 boardgame" or "67890 boardgameexpansion")
            for line in content.splitlines():
                if line.strip() and len(line.split()) >= 2:
                    parts = line.split()
                    if parts[0].isdigit():
                        games.append({"game_id": int(parts[0]), "type": parts[1]})
        logger.info("Found %d game IDs", len(games))
        return games

    def get_existing_ids(self) -> set[tuple]:
        """Get existing game IDs and types from BigQuery.

        Returns:
            Set of tuples containing (game_id, type)
        """
        query = f"""
        SELECT DISTINCT game_id, type
        FROM `{self.config["project"]["id"]}.{self.dataset_id}.{self.table_id}`
        """

        try:
            df = self.client.query(query).to_dataframe()
            existing_ids = {(row["game_id"], row["type"]) for _, row in df.iterrows()}
            logger.info("Found %d existing game IDs in BigQuery", len(existing_ids))
            return existing_ids
        except Exception as e:
            logger.error("Failed to fetch existing IDs: %s", e)
            return set()

    def upload_new_ids(self, new_games: list[dict]) -> None:
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
            MERGE `{self.config["project"]["id"]}.{self.dataset_id}.{self.table_id}` T
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

    def update_ids(self, temp_dir: Path) -> None:
        """Update game IDs in BigQuery with new IDs from BGG.

        Args:
            temp_dir: Directory for temporary files
        """
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
        else:
            logger.info("No new game IDs found")

    def fetch_game_ids(self, config: dict | None = None) -> list[int]:
        """
        Fetch game IDs from the downloaded file.

        Args:
            config: Optional configuration dictionary with parameters:
                - max_games_to_fetch (int): Maximum number of games to return
                - game_type (str): Type of game to filter (default: boardgame)

        Returns:
            List of game IDs
        """
        # Merge config with default configuration
        merged_config = {"max_games_to_fetch": 50, "game_type": "boardgame"}
        if config:
            merged_config.update(config)

        # Create a temporary directory for downloading
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)

        try:
            # Download and parse IDs
            ids_file = self.download_ids(temp_dir)
            all_games = self.parse_ids(ids_file)

            # Filter by game type
            filtered_games = [
                game["game_id"] for game in all_games if game["type"] == merged_config["game_type"]
            ]

            # Limit number of games
            filtered_games = filtered_games[: merged_config["max_games_to_fetch"]]

            logger.info(f"Fetched {len(filtered_games)} {merged_config['game_type']} game IDs")
            return filtered_games

        except Exception as e:
            logger.error(f"Failed to fetch game IDs: {e}")
            return []
        finally:
            # Cleanup temporary directory
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()


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
