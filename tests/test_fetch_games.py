"""Tests for fetch_games pipeline script."""

import os
from unittest.mock import patch, MagicMock

import pytest

from src.pipeline.fetch_games import parse_game_ids, main


class TestParseGameIds:
    def test_single_id(self):
        assert parse_game_ids("467694") == [467694]

    def test_multiple_ids(self):
        assert parse_game_ids("467694,12345,99999") == [467694, 12345, 99999]

    def test_whitespace_handling(self):
        assert parse_game_ids(" 467694 , 12345 ") == [467694, 12345]

    def test_deduplication(self):
        assert parse_game_ids("467694,467694,12345") == [467694, 12345]

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="No game IDs provided"):
            parse_game_ids("")

    def test_none_raises(self):
        with pytest.raises(ValueError, match="No game IDs provided"):
            parse_game_ids(None)

    def test_invalid_id_raises(self):
        with pytest.raises(ValueError, match="Invalid game ID"):
            parse_game_ids("abc,123")
