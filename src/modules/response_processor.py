"""Module for processing raw BGG API responses."""

import logging
import os
from datetime import datetime, UTC
from typing import List, Dict, Optional, Any

from dotenv import load_dotenv
from google.cloud import bigquery

from ..config import get_bigquery_config
from ..data_processor.processor import BGGDataProcessor
from ..data_processor.loader import BigQueryLoader
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


class ResponseProcessor:
    """Processes raw BGG API responses into normalized data."""

    def __init__(
        self,
        batch_size: int = 100,
        max_retries: int = 3,
        environment: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> None:
        """Initialize the processor.

        Args:
            batch_size: Number of responses to process in each batch
            max_retries: Maximum number of retry attempts for processing
            environment: Environment to run in (prod/dev/test)
            config: Optional configuration dictionary
        """
        # Get environment from config
        self.config = config or get_bigquery_config(environment)

        # Set processing parameters
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")

        # Initialize clients and processors
        self.bq_client = bigquery.Client()
        self.processor = BGGDataProcessor()
        self.loader = BigQueryLoader(environment)

        # Construct table references with fallback logic
        self.raw_responses_table = (
            f"{self.config['project']['id']}."
            f"{self.config['datasets']['raw']}."
            f"{self.config.get('raw_tables', {}).get('raw_responses', {}).get('name', 'raw_responses')}"
        )

        # Use the main dataset for processed tables
        self.processed_games_table = (
            f"{self.config['project']['id']}." f"{self.config['project']['dataset']}." "games"
        )

    def _convert_dataframe_to_list(self, df: Any) -> List[Dict]:
        """Convert various DataFrame types to a list of dictionaries.

        Args:
            df: DataFrame-like object to convert

        Returns:
            List of dictionaries containing game data
        """
        try:
            # Polars DataFrame
            if hasattr(df, "to_dicts"):
                return [
                    {
                        "record_id": row.get("record_id"),
                        "game_id": row["game_id"],
                        "response_data": row["response_data"],
                        "fetch_timestamp": row.get("fetch_timestamp"),
                    }
                    for row in df.to_dicts()
                ]

            # Pandas DataFrame - check for pandas-specific attributes first
            if hasattr(df, "to_dict") and hasattr(df, "iterrows"):
                records = df.to_dict("records")
                return [
                    {
                        "record_id": record.get("record_id"),
                        "game_id": record["game_id"],
                        "response_data": record["response_data"],
                        "fetch_timestamp": record.get("fetch_timestamp"),
                    }
                    for record in records
                ]

            # Mock object handling (for testing)
            if hasattr(df, "to_dict"):
                records = df.to_dict()
                if isinstance(records, dict):
                    # Handle dictionary-style mock
                    record_ids = records.get("record_id", [None] * len(records.get("game_id", [])))
                    game_ids = records.get("game_id", [])
                    response_data = records.get("response_data", [])
                    fetch_timestamps = records.get("fetch_timestamp", [None] * len(game_ids))
                    return [
                        {
                            "record_id": record_id,
                            "game_id": game_id,
                            "response_data": data,
                            "fetch_timestamp": ts,
                        }
                        for record_id, game_id, data, ts in zip(
                            record_ids, game_ids, response_data, fetch_timestamps
                        )
                    ]
                elif isinstance(records, list):
                    # Handle list-style mock
                    return [
                        {
                            "record_id": record.get("record_id"),
                            "game_id": record.get("game_id"),
                            "response_data": record.get("response_data"),
                            "fetch_timestamp": record.get("fetch_timestamp"),
                        }
                        for record in records
                    ]

            # Fallback for other mock objects
            if hasattr(df, "_data"):
                return [
                    {
                        "record_id": row.get("record_id"),
                        "game_id": row["game_id"],
                        "response_data": row["response_data"],
                        "fetch_timestamp": row.get("fetch_timestamp"),
                    }
                    for row in df._data
                ]

            logger.warning(f"Unsupported DataFrame type: {type(df)}")
            return []

        except Exception as e:
            logger.error(f"Failed to convert DataFrame: {e}")
            return []

    def get_unprocessed_count(self) -> int:
        """Get count of remaining unprocessed responses.

        Returns:
            Number of unprocessed responses remaining
        """
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.raw_responses_table}` r
        INNER JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses` f
            ON r.record_id = f.record_id
        LEFT JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses` p
            ON r.record_id = p.record_id
        WHERE p.record_id IS NULL
            AND f.fetch_status = 'success'
        """

        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            row = next(results)
            return row.count
        except Exception as e:
            logger.error(f"Failed to get unprocessed count: {e}")
            return 0

    def get_unprocessed_responses(self) -> List[Dict]:
        """Retrieve unprocessed responses from BigQuery.

        Returns:
            List of unprocessed game responses
        """
        query = f"""
        WITH responses AS (
            SELECT
                r.record_id,
                r.game_id,
                r.response_data,
                r.fetch_timestamp,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.fetch_timestamp, MINUTE) >= 30 as is_old
            FROM `{self.raw_responses_table}` r
            INNER JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.fetched_responses` f
                ON r.record_id = f.record_id
            LEFT JOIN `{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses` p
                ON r.record_id = p.record_id
            WHERE p.record_id IS NULL  -- Not yet processed
                AND f.fetch_status = 'success'  -- Only process successful fetches
        )
        SELECT record_id, game_id, response_data, fetch_timestamp
        FROM responses
        ORDER BY
            is_old DESC,  -- Process older responses first
            fetch_timestamp ASC  -- Then oldest to newest within each group
        LIMIT {self.batch_size}
        """

        try:
            # Execute query and get DataFrame
            query_result = self.bq_client.query(query)
            df = query_result.to_dataframe()

            # Convert DataFrame to list using helper method
            rows = self._convert_dataframe_to_list(df)

            # Process each row and parse response_data
            responses = []
            games_marked_no_response = []
            games_marked_parse_error = []

            for row in rows:
                # Skip empty or whitespace-only response_data
                response_data = row["response_data"]
                if not response_data or (
                    isinstance(response_data, str) and response_data.isspace()
                ):
                    logger.info(
                        f"Marking game {row['game_id']} as 'no_response' (empty response data)"
                    )
                    games_marked_no_response.append(row["game_id"])

                    # Mark as no_response in processed_responses
                    try:
                        processed_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses"
                        no_response_row = [{
                            "record_id": row.get("record_id"),
                            "process_timestamp": datetime.now(UTC).isoformat(),
                            "process_status": "no_response",
                            "process_attempt": 1,
                            "error_message": "Empty response data"
                        }]
                        errors = self.bq_client.insert_rows_json(processed_table_id, no_response_row)
                        if errors:
                            logger.error(f"Failed to insert no_response status: {errors}")
                    except Exception as update_error:
                        logger.error(
                            f"Failed to update status for game {row['game_id']}: {update_error}"
                        )
                    continue

                try:
                    # Handle response_data based on its type
                    if isinstance(response_data, str):
                        import json

                        try:
                            # Try JSON first
                            parsed_data = json.loads(response_data)
                        except json.JSONDecodeError:
                            # Fall back to ast.literal_eval for string dict
                            import ast

                            parsed_data = ast.literal_eval(response_data)
                    else:
                        # Already a dict/object, use as-is
                        parsed_data = response_data

                    responses.append(
                        {
                            "record_id": row.get("record_id"),
                            "game_id": row["game_id"],
                            "response_data": parsed_data,
                            "fetch_timestamp": row.get("fetch_timestamp", datetime.now(UTC)),
                        }
                    )
                except Exception as e:
                    logger.info(
                        f"Marking game {row['game_id']} as 'parse_error' (failed to parse response data): {e}"
                    )
                    games_marked_parse_error.append(row["game_id"])

                    # Mark as parse_error in processed_responses
                    try:
                        processed_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses"
                        parse_error_row = [{
                            "record_id": row.get("record_id"),
                            "process_timestamp": datetime.now(UTC).isoformat(),
                            "process_status": "parse_error",
                            "process_attempt": 1,
                            "error_message": str(e)[:500]
                        }]
                        errors = self.bq_client.insert_rows_json(processed_table_id, parse_error_row)
                        if errors:
                            logger.error(f"Failed to insert parse_error status: {errors}")
                    except Exception as update_error:
                        logger.error(
                            f"Failed to update status for game {row['game_id']}: {update_error}"
                        )

            # Log summary of what happened during retrieval
            total_retrieved = len(rows)
            total_valid_responses = len(responses)
            total_no_response = len(games_marked_no_response)
            total_parse_error = len(games_marked_parse_error)

            logger.info(
                f"Retrieval summary: {total_retrieved} games retrieved, {total_valid_responses} valid responses, {total_no_response} marked as 'no_response', {total_parse_error} marked as 'parse_error'"
            )

            if games_marked_no_response:
                logger.info(f"Games marked as 'no_response': {games_marked_no_response}")
            if games_marked_parse_error:
                logger.info(f"Games marked as 'parse_error': {games_marked_parse_error}")

            return responses

        except Exception as e:
            logger.error(f"Failed to retrieve unprocessed responses: {e}")
            return []

    def process_batch(self) -> bool:
        """Process a batch of game responses.

        Returns:
            bool: Whether processing was successful
        """
        # Retrieve unprocessed responses
        responses = self.get_unprocessed_responses()

        processed_games = []
        games_marked_failed = []
        games_marked_error = []

        # Process each response and track the specific records we're processing
        for response in responses:
            try:

                # Attempt to process game with game_type
                processed_game = self.processor.process_game(
                    response["game_id"],
                    response["response_data"],
                    game_type="boardgame",  # Default game type
                    load_timestamp=response[
                        "fetch_timestamp"
                    ],  # Use fetch timestamp as load timestamp
                )

                if processed_game:
                    # Add the record_id and fetch_timestamp to track the specific record
                    processed_game["record_id"] = response["record_id"]
                    processed_game["fetch_timestamp"] = response["fetch_timestamp"]
                    processed_games.append(processed_game)
                else:
                    logger.info(
                        f"Marking game {response['game_id']} as 'failed' (processing returned None)"
                    )
                    games_marked_failed.append(response["game_id"])

                    # Mark as failed in processed_responses
                    try:
                        processed_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses"
                        failed_row = [{
                            "record_id": response["record_id"],
                            "process_timestamp": datetime.now(UTC).isoformat(),
                            "process_status": "failed",
                            "process_attempt": 1,
                            "error_message": "Processing returned None"
                        }]
                        errors = self.bq_client.insert_rows_json(processed_table_id, failed_row)
                        if errors:
                            logger.error(f"Failed to insert failed status: {errors}")
                    except Exception as e:
                        logger.error(f"Failed to update process status: {e}")

            except Exception as e:
                logger.info(
                    f"Marking game {response['game_id']} as 'error' (exception during processing): {e}"
                )
                games_marked_error.append(response["game_id"])

                # Mark as error in processed_responses
                try:
                    processed_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses"
                    error_row = [{
                        "record_id": response["record_id"],
                        "process_timestamp": datetime.now(UTC).isoformat(),
                        "process_status": "error",
                        "process_attempt": 1,
                        "error_message": str(e)[:500]  # Limit error message length
                    }]
                    errors = self.bq_client.insert_rows_json(processed_table_id, error_row)
                    if errors:
                        logger.error(f"Failed to insert error status: {errors}")
                except Exception as update_error:
                    logger.error(f"Failed to update process status: {update_error}")

        # Log processing summary
        total_responses_received = len(responses)
        total_successfully_processed = len(processed_games)
        total_failed = len(games_marked_failed)
        total_error = len(games_marked_error)

        logger.info(
            f"Processing summary: {total_responses_received} responses received, {total_successfully_processed} successfully processed, {total_failed} marked as 'failed', {total_error} marked as 'error'"
        )

        if games_marked_failed:
            logger.info(f"Games marked as 'failed': {games_marked_failed}")
        if games_marked_error:
            logger.info(f"Games marked as 'error': {games_marked_error}")

        # Validate processed data
        if not processed_games:
            logger.warning("No games processed in this batch")
            return False

        # Prepare data for BigQuery
        try:
            processed_data = self.processor.prepare_for_bigquery(processed_games)

            # Validate data before loading
            if not self.processor.validate_data(processed_data.get("games"), "games"):
                logger.warning("Data validation failed")

                return False

            # Load processed games
            self.loader.load_games(processed_games)

            # Mark responses as processed by inserting into processed_responses tracking table
            record_ids = [game["record_id"] for game in processed_games if game.get("record_id")]

            if record_ids:
                logger.info(f"Marking {len(record_ids)} records as processed using processed_responses")

                # Build insert rows for processed_responses
                processed_tracking_rows = []
                for record_id in record_ids:
                    processed_tracking_rows.append({
                        "record_id": record_id,
                        "process_timestamp": datetime.now(UTC).isoformat(),
                        "process_status": "success",
                        "process_attempt": 1,
                        "error_message": None
                    })

                try:
                    # Insert into processed_responses tracking table
                    processed_table_id = f"{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses"
                    errors = self.bq_client.insert_rows_json(processed_table_id, processed_tracking_rows)

                    if errors:
                        logger.error(f"Failed to insert into processed_responses: {errors}")
                        return False

                    logger.info(f"Successfully marked {len(record_ids)} records as processed")

                    # Verify the insert
                    record_ids_str = "', '".join(record_ids)
                    verify_query = f"""
                    SELECT COUNT(*) as count
                    FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.processed_responses`
                    WHERE record_id IN ('{record_ids_str}')
                    """
                    verify_job = self.bq_client.query(verify_query)
                    verify_result = next(verify_job.result())
                    logger.info(f"Verified {verify_result.count} records were marked as processed")

                    if verify_result.count != len(record_ids):
                        logger.error(
                            f"Insert verification failed: expected {len(record_ids)} inserts but found {verify_result.count}"
                        )
                        return False

                except Exception as e:
                    logger.error(f"Failed to mark responses as processed: {e}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Failed to process batch: {e}")

            return False

    def run(self) -> bool:
        """Run the full processing pipeline until no unprocessed responses remain.

        Returns:
            bool: True if any responses were processed, False otherwise
        """
        logger.info(f"Starting response processor in {self.environment} environment")
        logger.info(f"Reading responses from: {self.raw_responses_table}")
        logger.info(f"Loading processed data to: {self.processed_games_table}")

        try:
            total_unprocessed = self.get_unprocessed_count()
            batch_count = 0

            logger.info(f"Found {total_unprocessed} unprocessed responses")

            if total_unprocessed == 0:
                logger.info("No unprocessed responses found")
                return False

            while total_unprocessed > 0:
                batch_count += 1
                logger.info(
                    f"Processing batch {batch_count} ({total_unprocessed} responses remaining)"
                )

                if not self.process_batch():
                    logger.warning("Batch processing failed, stopping pipeline")
                    break

                # Update count for next iteration
                total_unprocessed = self.get_unprocessed_count()

            logger.info(f"Processor completed - processed {batch_count} batches")
            logger.info(f"Remaining unprocessed responses: {total_unprocessed}")

            return batch_count > 0

        except Exception as e:
            logger.error(f"Processor failed: {e}")
            raise
