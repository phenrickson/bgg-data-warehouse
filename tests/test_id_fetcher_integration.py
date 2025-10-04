"""Integration tests for the BGG ID fetcher that hit the real BGG source URL."""

import tempfile
from pathlib import Path
import pytest
import requests
from src.id_fetcher.fetcher import BGGIDFetcher


@pytest.mark.integration
def test_bgg_url_accessible():
    """Test that the BGG thingids.txt URL is accessible."""
    fetcher = BGGIDFetcher()

    try:
        response = requests.head(fetcher.BGG_IDS_URL, timeout=10)
        assert response.status_code == 200, f"BGG URL returned status {response.status_code}"
    except requests.RequestException as e:
        pytest.fail(f"Failed to access BGG URL: {e}")


@pytest.mark.integration
def test_can_download_and_parse_ids():
    """Test downloading and parsing the actual BGG IDs file."""
    fetcher = BGGIDFetcher()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Download actual file
            file_path = fetcher.download_ids(Path(temp_dir))
            assert file_path.exists(), "Downloaded file does not exist"

            # Parse and validate content
            games = fetcher.parse_ids(file_path)
            assert len(games) > 0, "No games parsed from file"
            assert (
                len(games) > 100000
            ), f"Expected many games, got {len(games)}"  # BGG has 100k+ games

            # Validate data structure of first few games
            for i, game in enumerate(games[:10]):
                assert "game_id" in game, f"Game {i} missing game_id"
                assert "type" in game, f"Game {i} missing type"
                assert isinstance(
                    game["game_id"], int
                ), f"Game {i} game_id is not int: {type(game['game_id'])}"
                assert game["type"] in [
                    "boardgame",
                    "boardgameexpansion",
                ], f"Game {i} has invalid type: {game['type']}"

        except Exception as e:
            pytest.fail(f"Failed to download or parse BGG IDs: {e}")


@pytest.mark.integration
def test_file_format_validation():
    """Test that the downloaded file follows expected format."""
    fetcher = BGGIDFetcher()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            file_path = fetcher.download_ids(Path(temp_dir))

            # Read raw content and check first 20 lines
            with open(file_path) as f:
                lines = [line.strip() for line in f.readlines()[:20] if line.strip()]

            assert len(lines) > 0, "File appears to be empty"

            # Each line should be "ID type"
            for i, line in enumerate(lines):
                parts = line.split()
                assert len(parts) == 2, f"Line {i+1} has {len(parts)} parts, expected 2: '{line}'"
                assert parts[0].isdigit(), f"Line {i+1} first part is not numeric: '{parts[0]}'"
                assert parts[1] in [
                    "boardgame",
                    "boardgameexpansion",
                ], f"Line {i+1} has invalid type: '{parts[1]}'"

                # Validate the ID is reasonable (positive integer)
                game_id = int(parts[0])
                assert game_id > 0, f"Line {i+1} has invalid game ID: {game_id}"

        except Exception as e:
            pytest.fail(f"Failed to validate file format: {e}")


@pytest.mark.integration
def test_file_size_reasonable():
    """Test that the downloaded file is a reasonable size (not empty, not suspiciously small)."""
    fetcher = BGGIDFetcher()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            file_path = fetcher.download_ids(Path(temp_dir))
            file_size = file_path.stat().st_size

            # File should be at least 1MB (BGG has many games)
            assert file_size > 1024 * 1024, f"File size {file_size} bytes seems too small"

            # File shouldn't be unreasonably large (>100MB suggests something is wrong)
            assert file_size < 100 * 1024 * 1024, f"File size {file_size} bytes seems too large"

        except Exception as e:
            pytest.fail(f"Failed to check file size: {e}")


@pytest.mark.integration
def test_game_types_distribution():
    """Test that we get a reasonable distribution of game types."""
    fetcher = BGGIDFetcher()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            file_path = fetcher.download_ids(Path(temp_dir))
            games = fetcher.parse_ids(file_path)

            # Count game types
            type_counts = {}
            for game in games:
                game_type = game["type"]
                type_counts[game_type] = type_counts.get(game_type, 0) + 1

            # Should have both boardgames and expansions
            assert "boardgame" in type_counts, "No boardgames found"
            assert "boardgameexpansion" in type_counts, "No expansions found"

            # Boardgames should be much more numerous than expansions
            assert (
                type_counts["boardgame"] > type_counts["boardgameexpansion"]
            ), f"Expected more boardgames than expansions: {type_counts}"

            # Should have many of each type
            assert (
                type_counts["boardgame"] > 10000
            ), f"Too few boardgames: {type_counts['boardgame']}"

        except Exception as e:
            pytest.fail(f"Failed to analyze game type distribution: {e}")


@pytest.mark.integration
def test_fetch_game_ids_method():
    """Test the fetch_game_ids method with actual data."""
    fetcher = BGGIDFetcher()

    try:
        # Test with default config
        game_ids = fetcher.fetch_game_ids()
        assert len(game_ids) == 50, f"Expected 50 games, got {len(game_ids)}"
        assert all(isinstance(gid, int) for gid in game_ids), "All game IDs should be integers"

        # Test with custom config
        game_ids = fetcher.fetch_game_ids({"max_games_to_fetch": 10, "game_type": "boardgame"})
        assert len(game_ids) == 10, f"Expected 10 games, got {len(game_ids)}"

    except Exception as e:
        pytest.fail(f"Failed to test fetch_game_ids method: {e}")
