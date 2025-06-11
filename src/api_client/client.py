"""BoardGameGeek XML API2 client with rate limiting and request tracking."""

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
import xmltodict
from google.cloud import bigquery

from ..config import get_bigquery_config

# Get logger
logger = logging.getLogger(__name__)

class BGGAPIClient:
    """Client for the BoardGameGeek XML API2."""

    BASE_URL = "https://boardgamegeek.com/xmlapi2/"
    RATE_LIMIT = 2.0  # Maximum requests per second
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    THROTTLE_DELAY = 0.5  # seconds

    def __init__(self) -> None:
        """Initialize the API client."""
        self.last_request_time = datetime.min
        self.session = requests.Session()

    def _wait_for_rate_limit(self) -> None:
        """Wait to respect the rate limit."""
        now = datetime.now()
        elapsed = (now - self.last_request_time).total_seconds()
        if elapsed < self.THROTTLE_DELAY:
            time.sleep(self.THROTTLE_DELAY - elapsed)
        self.last_request_time = datetime.now()

    def _log_request(
        self,
        request_id: str,
        game_ids: Optional[Union[int, List[int]]],
        start_time: datetime,
        end_time: datetime,
        status_code: int,
        success: bool,
        error_message: Optional[str],
        retry_count: int,
    ) -> None:
        """Log request details to BigQuery and console.
        
        Args:
            request_id: Unique identifier for the request
            game_ids: ID or list of IDs of the requested games
            start_time: When the request was initiated
            end_time: When the response was received
            status_code: HTTP status code
            success: Whether the request was successful
            error_message: Error message if request failed
            retry_count: Number of retries attempted
        """
        duration = (end_time - start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"
        
        # Log to console
        logger.info(
            f"API Request {request_id} for games {game_ids}: {status} "
            f"(status={status_code}, duration={duration:.2f}s, retries={retry_count})"
        )
        if error_message:
            logger.error(f"Error details: {error_message}")
            
        # Log to BigQuery
        try:
            config = get_bigquery_config()
            client = bigquery.Client()
            
            # Prepare request log entry
            table_id = f"{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['request_log']['name']}"
            rows_to_insert = [{
                "request_id": request_id,
                "url": f"{self.BASE_URL}thing",
                "method": "GET",
                "game_ids": str(game_ids) if game_ids else None,
                "status_code": status_code,
                "response_time": duration,
                "error": error_message,
                "request_timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            }]
            
            # Insert into BigQuery
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Failed to log request to BigQuery: {errors}")
                
        except Exception as e:
            logger.error(f"Failed to log request to BigQuery: {e}")

    def get_thing(self, game_ids: Union[int, List[int]], stats: bool = True) -> Optional[Dict]:
        """Get details for one or more games.
        
        Args:
            game_ids: Single game ID or list of game IDs to fetch
            stats: Whether to include statistics
            
        Returns:
            Dictionary containing game details or None if request fails
        """
        request_id = str(uuid.uuid4())
        endpoint = urljoin(self.BASE_URL, "thing")
        # Convert single ID to list
        if isinstance(game_ids, int):
            game_ids = [game_ids]
            
        # Convert IDs to comma-separated string
        ids_str = ",".join(str(id) for id in game_ids)
        
        params = {
            "id": ids_str,
            "stats": int(stats),
            "type": "boardgame",
        }

        retry_count = 0
        while retry_count <= self.MAX_RETRIES:
            self._wait_for_rate_limit()
            start_time = datetime.utcnow()
            
            try:
                response = self.session.get(endpoint, params=params)
                end_time = datetime.utcnow()
                
                # Handle response
                if response.status_code == 200:
                    try:
                        data = xmltodict.parse(response.text)
                        self._log_request(
                            request_id=request_id,
                            game_ids=game_ids,
                            start_time=start_time,
                            end_time=end_time,
                            status_code=response.status_code,
                            success=True,
                            error_message=None,
                            retry_count=retry_count,
                        )
                        return data
                    except Exception as e:
                        logger.error("Failed to parse XML for games %s: %s", ids_str, e)
                        self._log_request(
                            request_id=request_id,
                            game_ids=game_ids,
                            start_time=start_time,
                            end_time=end_time,
                            status_code=response.status_code,
                            success=False,
                            error_message=f"XML parsing error: {str(e)}",
                            retry_count=retry_count,
                        )
                        return None

                # Handle rate limiting
                elif response.status_code == 429:
                    logger.warning("Rate limited for games %s, retrying...", ids_str)
                    time.sleep(self.RETRY_DELAY * (retry_count + 1))
                    retry_count += 1
                    continue

                # Handle other errors
                else:
                    logger.error(
                        "Failed to fetch games %s: %s %s",
                        ids_str,
                        response.status_code,
                        response.text,
                    )
                    self._log_request(
                        request_id=request_id,
                        game_ids=game_ids,
                        start_time=start_time,
                        end_time=end_time,
                        status_code=response.status_code,
                        success=False,
                        error_message=response.text,
                        retry_count=retry_count,
                    )
                    if retry_count < self.MAX_RETRIES:
                        retry_count += 1
                        time.sleep(self.RETRY_DELAY * retry_count)
                        continue
                    return None

            except requests.exceptions.RequestException as e:
                end_time = datetime.utcnow()
                logger.error("Request failed for games %s: %s", ids_str, e)
                self._log_request(
                    request_id=request_id,
                    game_ids=game_ids,
                    start_time=start_time,
                    end_time=end_time,
                    status_code=0,
                    success=False,
                    error_message=str(e),
                    retry_count=retry_count,
                )
                if retry_count < self.MAX_RETRIES:
                    retry_count += 1
                    time.sleep(self.RETRY_DELAY * retry_count)
                    continue
                return None

        return None

    def get_request_stats(
        self, 
        minutes: int = 60
    ) -> Dict[str, Union[int, float]]:
        """Get statistics about API requests from BigQuery.
        
        Args:
            minutes: Number of minutes to look back
            
        Returns:
            Dictionary containing request statistics
        """
        try:
            config = get_bigquery_config()
            client = bigquery.Client()
            
            # Query request log table
            query = f"""
            WITH recent_requests AS (
                SELECT *
                FROM `{config['project']['id']}.{config['datasets']['raw']}.{config['raw_tables']['request_log']['name']}`
                WHERE request_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {minutes} MINUTE)
            )
            SELECT
                COUNT(*) as total_requests,
                COUNTIF(status_code = 200) as successful_requests,
                COUNTIF(status_code != 200) as failed_requests,
                AVG(response_time) as avg_response_time,
                AVG(CAST(REGEXP_EXTRACT(error, r'retries=([0-9]+)') AS INT64)) as avg_retries
            FROM recent_requests
            """
            
            df = client.query(query).to_dataframe()
            if len(df) == 0:
                return {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "avg_response_time": 0,
                    "avg_retries": 0,
                }
                
            stats = df.iloc[0].to_dict()
            # Convert NaN to 0
            stats = {k: 0 if pd.isna(v) else v for k, v in stats.items()}
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get request stats: {e}")
            return {
                "total_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "avg_response_time": 0,
                "avg_retries": 0,
            }
