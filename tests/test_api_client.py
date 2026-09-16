"""Unit tests for BGGAPIClient construction. No network."""

import pytest

from src.api_client.client import BGGAPIClient


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("abc123", "abc123"),
        ("abc123\n", "abc123"),  # trailing newline from a pasted secret
        ("abc123\r\n", "abc123"),
        ("  abc123  ", "abc123"),
        ("", None),
        ("   \n", None),  # whitespace-only is the same as unset
    ],
)
def test_api_token_is_normalized(monkeypatch, raw, expected):
    """A newline in the token makes http.client reject the Authorization header."""
    monkeypatch.setenv("BGG_API_TOKEN", raw)

    client = BGGAPIClient(log_requests=False)

    assert client.api_token == expected


def test_api_token_missing(monkeypatch):
    monkeypatch.delenv("BGG_API_TOKEN", raising=False)

    client = BGGAPIClient(log_requests=False)

    assert client.api_token is None


def test_throttle_delay_defaults_to_class_constant():
    client = BGGAPIClient(log_requests=False)

    assert client.throttle_delay == BGGAPIClient.THROTTLE_DELAY


def test_throttle_delay_override():
    client = BGGAPIClient(throttle_delay=2.0, log_requests=False)

    assert client.throttle_delay == 2.0
