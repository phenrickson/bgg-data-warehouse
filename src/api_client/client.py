"""BoardGameGeek XML API2 client with rate limiting and request tracking."""

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Union
from urllib.parse import urljoin

import requests
import xmltodict
from google.cloud import bigquery

from ..config import get_bigquery_config

from ..utils.logging_config import setup_logging

# Set up logging
logger = setup_logging(__name__)

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
        game_id: Optional[int],
        start_time: datetime,
        end_time: datetime,
        status_code: int,
        success: bool,
        error_message: Optional[str],
        retry_count: int,
    ) -> None:
        """Log request details (currently just logs to console).
        
        Args:
            request_id: Unique identifier for the request
            game_id: ID of the requested game (if applicable)
            start_time: When the request was initiated
            end_time: When the response was received
            status_code: HTTP status code
            success: Whether the request was successful
            error_message: Error message if request failed
            retry_count: Number of retries attempted
        """
        duration = (end_time - start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"
        logger.info(
            f"API Request {request_id} for game {game_id}: {status} "
            f"(status={status_code}, duration={duration:.2f}s, retries={retry_count})"
        )
        if error_message:
            logger.error(f"Error details: {error_message}")

    def get_thing(self, game_id: int, stats: bool = True) -> Optional[Dict]:
        """Get details for a specific game.
        
        Args:
            game_id: ID of the game to fetch
            stats: Whether to include statistics
            
        Returns:
            Dictionary containing game details or None if request fails
        """
        request_id = str(uuid.uuid4())
        endpoint = urljoin(self.BASE_URL, "thing")
        params = {
            "id": game_id,
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
                            game_id=game_id,
                            start_time=start_time,
                            end_time=end_time,
                            status_code=response.status_code,
                            success=True,
                            error_message=None,
                            retry_count=retry_count,
                        )
                        return data
                    except Exception as e:
                        logger.error("Failed to parse XML for game %d: %s", game_id, e)
                        self._log_request(
                            request_id=request_id,
                            game_id=game_id,
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
                    logger.warning("Rate limited for game %d, retrying...", game_id)
                    time.sleep(self.RETRY_DELAY * (retry_count + 1))
                    retry_count += 1
                    continue

                # Handle other errors
                else:
                    logger.error(
                        "Failed to fetch game %d: %s %s",
                        game_id,
                        response.status_code,
                        response.text,
                    )
                    self._log_request(
                        request_id=request_id,
                        game_id=game_id,
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
                logger.error("Request failed for game %d: %s", game_id, e)
                self._log_request(
                    request_id=request_id,
                    game_id=game_id,
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
        """Get statistics about API requests (currently a stub).
        
        Args:
            minutes: Number of minutes to look back
            
        Returns:
            Dictionary containing request statistics
        """
        return {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "avg_response_time": 0,
            "avg_retries": 0,
        }
