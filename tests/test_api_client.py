"""Tests for the BGG API client."""

from datetime import UTC, datetime
from unittest import mock

import pandas as pd
import pytest
import requests

from src.api_client.client import BGGAPIClient


@pytest.fixture
def api_client(sample_config, mock_bigquery_client, mock_env):
    """Create API client with mocked configuration and clients."""
    with mock.patch("src.api_client.client.get_bigquery_config", return_value=sample_config):
        client = BGGAPIClient()
        client.client = mock_bigquery_client
        yield client


@pytest.fixture
def mock_response():
    """Create mock API response."""
    response = mock.Mock()
    response.status_code = 200
    response.text = """
    <?xml version="1.0" encoding="utf-8"?>
    <items termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
        <item type="boardgame" id="13">
            <name type="primary" value="Catan" />
            <yearpublished value="1995" />
        </item>
    </items>
    """
    return response


def test_rate_limiting(api_client):
    """Test rate limiting behavior."""
    with mock.patch("time.sleep") as mock_sleep:
        api_client._wait_for_rate_limit()
        api_client._wait_for_rate_limit()

        # Should sleep to maintain rate limit
        mock_sleep.assert_called()


def test_successful_request(api_client, mock_response):
    """Test successful API request."""
    # Ensure XML starts at the beginning without extra whitespace
    mock_response.text = """<?xml version="1.0" encoding="utf-8"?>
<items termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
    <item type="boardgame" id="13">
        <name type="primary" value="Catan" />
        <yearpublished value="1995" />
    </item>
</items>"""

    with mock.patch.object(api_client.session, "get", return_value=mock_response):
        with mock.patch.object(api_client, "_log_request") as mock_log:
            result = api_client.get_thing(13)

            assert result is not None
            assert "items" in result
            assert result["items"]["item"]["@id"] == "13"

            # Should log successful request
            mock_log.assert_called_once()
            args = mock_log.call_args[1]
            assert args["game_ids"] == [13]
            assert args["success"] is True


def test_rate_limit_retry(api_client):
    """Test retry behavior on rate limit."""
    responses = [
        mock.Mock(status_code=429),  # Rate limited
        mock.Mock(
            status_code=200, text="<?xml version='1.0'?><items><item id='13'></item></items>"
        ),
    ]

    with mock.patch.object(api_client.session, "get", side_effect=responses):
        with mock.patch("time.sleep") as mock_sleep:
            result = api_client.get_thing(13)

            assert result is not None
            # Verify sleep is called with retry delay
            # Actual implementation may call sleep multiple times
            assert any(call[0][0] == 5 for call in mock_sleep.call_args_list)


def test_max_retries_exceeded(api_client):
    """Test behavior when max retries are exceeded."""
    response = mock.Mock(status_code=429)

    with mock.patch.object(api_client.session, "get", return_value=response):
        with mock.patch("time.sleep"):
            result = api_client.get_thing(13)

            assert result is None


def test_request_error(api_client):
    """Test handling of request errors."""
    with mock.patch.object(
        api_client.session, "get", side_effect=requests.exceptions.RequestException("Network error")
    ):
        with mock.patch.object(api_client, "_log_request") as mock_log:
            result = api_client.get_thing(13)

            assert result is None

            # Actual implementation logs for each retry attempt
            assert mock_log.call_count == api_client.MAX_RETRIES + 1
            args = mock_log.call_args[1]
            assert args["game_ids"] == [13]
            assert args["success"] is False
            assert "Network error" in args["error_message"]


def test_invalid_xml(api_client):
    """Test handling of invalid XML response."""
    response = mock.Mock(status_code=200, text="Invalid XML")

    with mock.patch.object(api_client.session, "get", return_value=response):
        with mock.patch.object(api_client, "_log_request") as mock_log:
            result = api_client.get_thing(13)

            assert result is None

            # Should log failed request
            mock_log.assert_called_once()
            args = mock_log.call_args[1]
            assert args["game_ids"] == [13]
            assert args["success"] is False
            assert "XML parsing error" in args["error_message"]


def test_request_stats(api_client):
    """Test request statistics calculation."""
    # Mock BigQuery configuration to avoid 'raw_tables' error
    with mock.patch(
        "src.api_client.client.get_bigquery_config",
        return_value={
            "project": {"id": "test-project"},
            "datasets": {"raw": "test_dataset"},
            "raw_tables": {"request_log": {"name": "request_log"}},
        },
    ):
        mock_query_result = mock.Mock()
        mock_query_result.to_dataframe.return_value = pd.DataFrame(
            [
                {
                    "total_requests": 100,
                    "successful_requests": 95,
                    "failed_requests": 5,
                    "avg_response_time": 1.5,
                    "avg_retries": 0.1,
                }
            ]
        )

        with mock.patch.object(api_client.client, "query", return_value=mock_query_result):
            stats = api_client.get_request_stats(minutes=60)

            assert stats["total_requests"] == 100
            assert stats["successful_requests"] == 95
            assert stats["failed_requests"] == 5
            assert stats["avg_response_time"] == 1.5
            assert stats["avg_retries"] == 0.1


def test_request_stats_error(api_client):
    """Test handling of stats calculation errors."""
    with mock.patch.object(api_client.client, "query", side_effect=Exception("Query failed")):
        stats = api_client.get_request_stats(minutes=60)

        assert stats["total_requests"] == 0
        assert stats["successful_requests"] == 0
        assert stats["avg_response_time"] == 0
        assert stats["avg_retries"] == 0


def test_log_request_error(api_client):
    """Test handling of request logging errors."""
    mock_client = mock.Mock()
    mock_client.insert_rows_json.return_value = ["Error"]
    api_client.client = mock_client

    with mock.patch("src.api_client.client.logger.error") as mock_logger:
        api_client._log_request(
            request_id="test",
            game_ids=[13],
            start_time=datetime.now(UTC),
            end_time=datetime.now(UTC),
            status_code=200,
            success=True,
            error_message=None,
            retry_count=0,
        )

        mock_logger.assert_called_once()
