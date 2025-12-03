"""Module for fetching and storing raw BGG API responses."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Optional

from google.cloud import bigquery

from ..api_client.client import BGGAPIClient
from ..config import get_bigquery_config
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class ResponseFetcher:
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
                retry_counts AS (
                    SELECT
                        game_id,
                        COUNT(*) as attempt_count,
                        MAX(fetch_timestamp) as last_attempt_time
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses`
                    WHERE fetch_status IN ('no_response', 'parse_error')
                    GROUP BY game_id
                ),
                successful_fetches AS (
                    SELECT DISTINCT game_id
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses`
                    WHERE fetch_status = 'success'
                ),
                candidates AS (
                    SELECT i.game_id, 'boardgame' as type
                    FROM input_ids i
                    LEFT JOIN successful_fetches sf ON i.game_id = sf.game_id
                    LEFT JOIN retry_counts rc ON i.game_id = rc.game_id
                    LEFT JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}` p
                        ON i.game_id = p.game_id
                    WHERE
                        -- Exclude successful fetches
                        sf.game_id IS NULL
                        -- Exclude games that have been retried too many times
                        AND (rc.attempt_count IS NULL OR rc.attempt_count < 3)
                        -- Exclude games that were recently attempted (within 1 hour)
                        AND (rc.last_attempt_time IS NULL OR rc.last_attempt_time <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
                        -- Exclude games currently being fetched
                        AND p.game_id IS NULL
                    LIMIT {self.batch_size}
                )
                """
            else:
                base_query = f"""
                WITH retry_counts AS (
                    SELECT
                        game_id,
                        COUNT(*) as attempt_count,
                        MAX(fetch_timestamp) as last_attempt_time
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses`
                    WHERE fetch_status IN ('no_response', 'parse_error')
                    GROUP BY game_id
                ),
                successful_fetches AS (
                    SELECT DISTINCT game_id
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses`
                    WHERE fetch_status = 'success'
                ),
                candidates AS (
                    SELECT t.game_id, t.type
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['thing_ids']['name']}` t
                    LEFT JOIN successful_fetches sf ON t.game_id = sf.game_id
                    LEFT JOIN retry_counts rc ON t.game_id = rc.game_id
                    LEFT JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.{self.config['raw_tables']['fetch_in_progress']['name']}` p
                        ON t.game_id = p.game_id
                    WHERE
                        t.type = 'boardgame'
                        -- Exclude successful fetches
                        AND sf.game_id IS NULL
                        -- Exclude games that have been retried too many times
                        AND (rc.attempt_count IS NULL OR rc.attempt_count < 3)
                        -- Exclude games that were recently attempted (within 1 hour)
                        AND (rc.last_attempt_time IS NULL OR rc.last_attempt_time <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
                        -- Exclude games currently being fetched
                        AND p.game_id IS NULL
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
        fetch_statuses = {}  # Track fetch status for each game_id

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
                }
                rows.append(row)
                fetch_statuses[game_id] = "no_response"
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
                        }
                        rows.append(row)
                        fetch_statuses[game_id] = "success"
                        logger.debug(f"Created row for game_id {game_id}: {row}")

            except Exception as parse_error:
                logger.error(f"Failed to parse response data: {parse_error}")
                logger.error(f"Raw response data: {response_data}")
                # If parsing fails, store empty responses for these game IDs
                for game_id in game_ids:
                    row = {
                        "game_id": game_id,
                        "response_data": "",  # Use empty string instead of None
                        "fetch_timestamp": base_time.isoformat(),
                    }
                    rows.append(row)
                    fetch_statuses[game_id] = "parse_error"
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

            logger.info(f"Successfully fetched responses for {len(rows)} games")

            # Insert into fetched_responses tracking table
            try:
                fetched_tracking_rows = []
                for row in rows:
                    # Get fetch status from our tracking dictionary
                    fetch_status = fetch_statuses.get(row["game_id"], "success")

                    # Query to get the record_id that was just inserted
                    # We match on game_id and fetch_timestamp since they're unique for this batch
                    record_id_query = f"""
                    SELECT record_id
                    FROM `{table_id}`
                    WHERE game_id = {row['game_id']}
                        AND fetch_timestamp = TIMESTAMP('{row['fetch_timestamp']}')
                    ORDER BY fetch_timestamp DESC
                    LIMIT 1
                    """
                    result = self.bq_client.query(record_id_query).result()
                    record_row = next(result, None)

                    if record_row:
                        fetched_tracking_rows.append({
                            "record_id": record_row.record_id,
                            "game_id": row["game_id"],
                            "fetch_timestamp": row["fetch_timestamp"],
                            "fetch_status": fetch_status
                        })

                if fetched_tracking_rows:
                    fetched_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses"
                    errors = self.bq_client.insert_rows_json(fetched_table_id, fetched_tracking_rows)
                    if errors:
                        logger.error(f"Failed to insert into fetched_responses: {errors}")
                    else:
                        logger.info(f"Successfully inserted {len(fetched_tracking_rows)} records into fetched_responses")
            except Exception as tracking_error:
                logger.error(f"Failed to insert into fetched_responses tracking table: {tracking_error}")

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

    def run(self, game_ids: Optional[List[int]] = None) -> bool:
        """Run the fetcher pipeline.

        Args:
            game_ids: Optional list of specific game IDs to fetch

        Returns:
            bool: True if any responses were fetched, False otherwise
        """
        logger.info(f"Starting response fetcher in {self.environment} environment")

        try:
            responses_fetched = False
            while True:
                if not self.fetch_batch(game_ids):
                    break
                responses_fetched = True

            if responses_fetched:
                logger.info("Fetcher completed - responses fetched")
            else:
                logger.info("No responses to fetch")

            return responses_fetched

        except Exception as e:
            logger.error(f"Fetcher failed: {e}")
            raise
