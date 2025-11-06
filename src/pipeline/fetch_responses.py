"""Pipeline module for fetching and storing raw BGG API responses."""

import logging
import random
import inspect
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Union

from google.cloud import bigquery

from ..id_fetcher.fetcher import BGGIDFetcher
from ..api_client.client import BGGAPIClient
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class BGGResponseFetcher:
    """Fetches and stores raw BGG API responses."""

    def __init__(
        self,
        batch_size: int = 1000,
        chunk_size: int = 20,
        environment: str = "prod",
        max_retries: int = 1,
    ) -> None:
        """Initialize the fetcher.

        Args:
            batch_size: Number of games to fetch in each batch from BigQuery
            chunk_size: Number of games to request in each API call
            environment: Environment to use (prod/dev/test)
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.config = get_bigquery_config(environment)
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.environment = environment
        self.max_retries = max_retries
        self.id_fetcher = BGGIDFetcher(self.config)
        self.api_client = BGGAPIClient()
        self.bq_client = bigquery.Client()

    def get_unfetched_ids(self, game_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get IDs that haven't had responses fetched yet.

        Args:
            game_ids: Optional list of specific game IDs to fetch

        Returns:
            List of dictionaries containing unfetched game IDs and their types
        """
        try:
            # Clean up old in-progress entries first
            cleanup_query = f"""
            DELETE FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
            WHERE fetch_start_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
            """
            self.bq_client.query(cleanup_query).result()

            # Build the base query depending on whether specific game_ids were provided
            if game_ids:
                base_query = f"""
                WITH input_ids AS (
                    SELECT game_id
                    FROM UNNEST({game_ids}) AS game_id
                ),
                candidates AS (
                    SELECT i.game_id, 'boardgame' as type
                    FROM input_ids i
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}` r
                        WHERE i.game_id = r.game_id
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}` f
                        WHERE i.game_id = f.game_id
                    )
                    LIMIT {self.batch_size}
                )
                """
            else:
                base_query = f"""
                WITH candidates AS (
                    SELECT t.game_id, t.type
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}` t
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}` r
                        WHERE t.game_id = r.game_id
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}` f
                        WHERE t.game_id = f.game_id
                    )
                    AND t.type = 'boardgame'
                    ORDER BY t.game_id
                    LIMIT {self.batch_size}
                )
                """

            # First get candidate games
            candidates_query = (
                base_query
                + """
            SELECT game_id, type 
            FROM candidates;
            """
            )
            candidates_df = self.bq_client.query(candidates_query).to_dataframe()

            if len(candidates_df) > 0:
                # Then mark them as in progress
                game_ids_str = ", ".join(str(id) for id in candidates_df["game_id"])
                mark_query = f"""
                INSERT INTO `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
                    (game_id, fetch_start_timestamp)
                SELECT c.game_id, CURRENT_TIMESTAMP()
                FROM (
                    {base_query}
                    SELECT game_id, type 
                    FROM candidates
                ) c
                WHERE c.game_id IN ({game_ids_str})
                """
                self.bq_client.query(mark_query).result()

            logger.info(f"Found {len(candidates_df)} unfetched games")

            # Convert pandas DataFrame to list of dicts
            return [
                {"game_id": row["game_id"], "type": row.get("type", "boardgame")}
                for _, row in candidates_df.iterrows()
            ]

        except Exception as e:
            logger.error(f"Failed to fetch unprocessed IDs: {e}")
            return []

    def store_response(
        self, game_ids: List[int], response_data: str, no_response_ids: Optional[List[int]] = None
    ) -> None:
        """Store raw API response in BigQuery using load jobs.

        Args:
            game_ids: List of game IDs in the response
            response_data: Raw API response data
            no_response_ids: List of game IDs with no response
        """
        # Parse the response data to extract individual game responses
        import ast

        base_time = datetime.now(UTC)
        rows = []

        # Logging input parameters for debugging
        logger.info(
            f"store_response called with: game_ids={game_ids}, no_response_ids={no_response_ids}"
        )

        # Handle game IDs with no response
        if no_response_ids:
            logger.info(f"Processing {len(no_response_ids)} no-response game IDs")
            for game_id in no_response_ids:
                row = {
                    "game_id": game_id,
                    "response_data": "",  # Use empty string instead of None
                    "fetch_timestamp": base_time.isoformat(),
                    "processed": True,
                    "process_timestamp": base_time.isoformat(),
                    "process_status": "no_response",
                    "process_attempt": 1,
                }
                rows.append(row)
                logger.debug(f"No-response row for game_id {game_id}: {row}")

        # If response data is provided, process it
        if response_data:
            logger.info(f"Processing response data for {len(game_ids)} game IDs")

            try:
                # Parse the response data
                parsed_response = ast.literal_eval(response_data)

                # Extract items from the response
                items = parsed_response.get("items", {}).get("item", [])

                # Ensure items is a list
                if not isinstance(items, list):
                    items = [items] if items else []

                logger.info(f"Found {len(items)} items in the response")

                # Create a mapping of game IDs to their specific response
                game_responses = {}
                for item in items:
                    game_id = int(item.get("@id", 0))
                    if game_id in game_ids:
                        # Store the specific item as a response for this game
                        game_responses[game_id] = str({"items": {"item": item}})
                        logger.debug(f"Processed response for game_id {game_id}")

                # Create rows for each game with its specific response
                for game_id in game_ids:
                    if game_id in game_responses:
                        row = {
                            "game_id": game_id,
                            "response_data": game_responses[game_id],
                            "fetch_timestamp": base_time.isoformat(),
                            "processed": False,
                            "process_timestamp": None,
                            "process_status": None,
                            "process_attempt": 0,
                        }
                        rows.append(row)
                        logger.debug(f"Created row for game_id {game_id}: {row}")

            except Exception as parse_error:
                logger.error(f"Failed to parse response data: {parse_error}")
                logger.error(f"Raw response data: {response_data}")
                # If parsing fails, mark all game IDs as processed with an error status
                for game_id in game_ids:
                    row = {
                        "game_id": game_id,
                        "response_data": "",  # Use empty string instead of None
                        "fetch_timestamp": base_time.isoformat(),
                        "processed": True,
                        "process_timestamp": base_time.isoformat(),
                        "process_status": "parse_error",
                        "process_attempt": 1,
                    }
                    rows.append(row)
                    logger.debug(f"Parse error row for game_id {game_id}: {row}")

        # Log total number of rows to be inserted
        logger.info(f"Total rows to insert: {len(rows)}")

        table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['raw_responses']['name']}"

        try:
            # In test/dev environment, use insert_rows_json directly
            if self.environment in ["test", "dev"]:
                errors = self.bq_client.insert_rows_json(table_id, rows)
                if errors:
                    logger.error(f"Failed to insert rows: {errors}")
            else:
                # Get table schema
                table = self.bq_client.get_table(table_id)

                # Validate schema to ensure response_data can be empty
                response_data_field = next(
                    (field for field in table.schema if field.name == "response_data"), None
                )
                if response_data_field and not response_data_field.is_nullable:
                    logger.warning(
                        "response_data field is not nullable. This may cause insertion errors."
                    )

                # Configure the load job
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=table.schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    # Add max bad records to prevent job failure
                    max_bad_records=len(rows),  # Allow all rows to be considered bad if needed
                )

                # Load data using load job
                load_job = self.bq_client.load_table_from_json(
                    rows, table_id, job_config=job_config
                )

                # Wait for job to complete
                load_job_result = load_job.result()

                # Check for specific errors
                if load_job.errors:
                    logger.error(f"Detailed load job errors: {load_job.errors}")

                    # Log individual row details for debugging
                    for row in rows:
                        logger.debug(f"Problematic row details: {row}")

                # Log job statistics
                logger.info(f"Load job input rows: {load_job_result.input_file_bytes}")
                logger.info(f"Load job output rows: {load_job_result.output_rows}")

            logger.info(f"Successfully processed responses for {len(rows)} games")

            # Clean up fetch_in_progress entries for these games
            try:
                game_ids_str = ",".join(str(row["game_id"]) for row in rows)
                cleanup_query = f"""
                DELETE FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}`
                WHERE game_id IN ({game_ids_str})
                """
                self.bq_client.query(cleanup_query).result()
                logger.info(f"Cleaned up fetch_in_progress entries for {len(rows)} games")
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up fetch_in_progress entries: {cleanup_error}")

        except Exception as e:
            logger.error(f"Failed to store responses: {e}")

            # Log detailed row information for debugging
            for row in rows:
                logger.error(f"Problematic row: {row}")

            raise

    def fetch_batch(self, game_ids: Optional[List[int]] = None) -> bool:
        """Fetch and store a batch of responses.

        Args:
            game_ids: Optional list of specific game IDs to fetch

        Returns:
            bool: Whether any responses were fetched
        """
        # Get unfetched IDs
        try:
            unfetched = self.get_unfetched_ids(game_ids)
        except Exception as e:
            logger.error(f"Failed to get unfetched IDs: {e}")
            return False

        if not unfetched:
            logger.info("No unfetched games found")
            return False

        # Process in chunks
        for i in range(0, len(unfetched), self.chunk_size):
            chunk = unfetched[i : i + self.chunk_size]
            chunk_ids = [game["game_id"] for game in chunk]

            try:
                # Fetch data from API
                logger.info(f"Fetching data for games {chunk_ids}...")

                # Single attempt with error handling
                try:
                    response = self.api_client.get_thing(chunk_ids)

                    # Detailed logging and robust response handling
                    if response:
                        # Log the raw response for debugging
                        logger.debug(f"Raw response: {response}")

                        # Attempt to parse and validate the response
                        try:
                            import ast

                            parsed_response = ast.literal_eval(str(response))

                            # Check if items exist in the response
                            items = parsed_response.get("items", {}).get("item", [])

                            # Ensure items is a list
                            if not isinstance(items, list):
                                items = [items] if items else []

                            # Extract game IDs from the response
                            response_game_ids = [
                                int(item.get("@id", 0))
                                for item in items
                                if int(item.get("@id", 0)) in chunk_ids
                            ]

                            # Log the found game IDs
                            logger.info(f"Found game IDs in response: {response_game_ids}")

                            # Store the response, handling both found and not found game IDs
                            if response_game_ids:
                                self.store_response(response_game_ids, str(response))

                            # Mark game IDs not found in the response
                            not_found_ids = [
                                game_id for game_id in chunk_ids if game_id not in response_game_ids
                            ]
                            if not_found_ids:
                                logger.warning(f"No data found for games: {not_found_ids}")
                                self.store_response([], None, not_found_ids)

                        except Exception as parse_error:
                            logger.error(f"Failed to parse response for {chunk_ids}: {parse_error}")
                            # Mark all chunk IDs as processed due to parsing error
                            self.store_response([], None, chunk_ids)
                    else:
                        logger.warning(f"No data returned for games {chunk_ids}")
                        # Mark all chunk IDs as processed
                        self.store_response([], None, chunk_ids)

                except Exception as e:
                    logger.error(f"Failed to fetch chunk {chunk_ids}: {e}")
                    # In test/dev environment, handle rate limiting and API errors
                    if self.environment in ["test", "dev"]:
                        if "Rate limited" in str(e) or "API Error" in str(e):
                            # Try the same chunk again
                            try:
                                response = self.api_client.get_thing(chunk_ids)
                                if response:
                                    self.store_response(chunk_ids, str(response))
                            except Exception as retry_e:
                                logger.error(f"Retry failed for chunk {chunk_ids}: {retry_e}")
                                raise
                        else:
                            raise

            except Exception as e:
                logger.error(f"Unhandled error in fetch_batch: {e}")
                # Continue processing other chunks
                continue

        return True

    def run(self, game_ids: Optional[List[int]] = None) -> None:
        """Run the fetcher pipeline.

        Args:
            game_ids: Optional list of specific game IDs to fetch
        """
        logger.info("Starting BGG response fetcher")

        try:
            # Create temp directory for ID updates if needed
            temp_dir = Path("temp")
            if self.environment != "test":
                self.id_fetcher.update_ids(temp_dir)
            try:
                while True:
                    if not self.fetch_batch(game_ids):
                        break
                logger.info("Fetcher completed - all responses fetched")
            finally:
                # Cleanup
                if temp_dir.exists():
                    for file in temp_dir.glob("*"):
                        file.unlink()
                    temp_dir.rmdir()

        except Exception as e:
            logger.error(f"Fetcher failed: {e}")
            raise


def main() -> None:
    """Main entry point for the fetcher."""
    import os

    environment = os.getenv("ENVIRONMENT", "dev")
    logger.info(f"Starting fetcher in {environment} environment")

    fetcher = BGGResponseFetcher(
        batch_size=1000,
        chunk_size=20,
        environment=environment,
    )
    fetcher.run()


if __name__ == "__main__":
    main()
