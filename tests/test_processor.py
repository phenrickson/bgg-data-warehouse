"""Tests for the BGG data processor."""

from datetime import datetime
from unittest import mock

import polars as pl
import pytest

from src.data_processor.processor import BGGDataProcessor

@pytest.fixture
def processor():
    """Create a data processor instance."""
    return BGGDataProcessor()

@pytest.fixture
def sample_game_response():
    """Create a sample game API response."""
    return {
        "items": {
            "item": {
                "@id": "13",
                "@type": "boardgame",
                "name": [
                    {
                        "@type": "primary",
                        "@value": "Catan"
                    },
                    {
                        "@type": "alternate",
                        "@value": "Settlers of Catan"
                    }
                ],
                "yearpublished": {"@value": "1995"},
                "minplayers": {"@value": "3"},
                "maxplayers": {"@value": "4"},
                "playingtime": {"@value": "120"},
                "minage": {"@value": "10"},
                "description": "Build, trade, settle!",
                "thumbnail": "thumbnail.jpg",
                "image": "image.jpg",
                "link": [
                    {"@type": "boardgamecategory", "@value": "Negotiation"},
                    {"@type": "boardgamemechanic", "@value": "Dice Rolling"},
                    {"@type": "boardgamefamily", "@value": "Base Game"}
                ],
                "statistics": {
                    "ratings": {
                        "average": {"@value": "7.5"},
                        "usersrated": {"@value": "1000"},
                        "owned": {"@value": "500"},
                        "averageweight": {"@value": "2.5"}
                    }
                }
            }
        }
    }

def test_extract_name(processor, sample_game_response):
    """Test extracting the primary name."""
    item = sample_game_response["items"]["item"]
    name = processor._extract_name(item)
    assert name == "Catan"

def test_extract_name_single(processor):
    """Test extracting name when only one name exists."""
    item = {"name": {"@type": "primary", "@value": "Catan"}}
    name = processor._extract_name(item)
    assert name == "Catan"

def test_extract_name_missing(processor):
    """Test extracting name when no name exists."""
    item = {}
    name = processor._extract_name(item)
    assert name == "Unknown"

def test_extract_year(processor, sample_game_response):
    """Test extracting publication year."""
    item = sample_game_response["items"]["item"]
    year = processor._extract_year(item)
    assert year == 1995

def test_extract_year_invalid(processor):
    """Test extracting invalid year."""
    item = {"yearpublished": {"@value": "invalid"}}
    year = processor._extract_year(item)
    assert year is None

def test_extract_list_field(processor, sample_game_response):
    """Test extracting list fields."""
    item = sample_game_response["items"]["item"]
    categories = processor._extract_list_field(item, "link")
    assert "Negotiation" in categories
    assert "Dice Rolling" in categories

def test_extract_list_field_single(processor):
    """Test extracting list field with single item."""
    item = {"link": {"@type": "boardgamecategory", "@value": "Strategy"}}
    categories = processor._extract_list_field(item, "link")
    assert categories == ["Strategy"]

def test_extract_list_field_empty(processor):
    """Test extracting empty list field."""
    item = {}
    result = processor._extract_list_field(item, "link")
    assert result == []

def test_extract_stats(processor, sample_game_response):
    """Test extracting game statistics."""
    item = sample_game_response["items"]["item"]
    stats = processor._extract_stats(item)
    
    assert stats["average"] == 7.5
    assert stats["num_ratings"] == 1000
    assert stats["owned"] == 500
    assert stats["weight"] == 2.5

def test_process_game(processor, sample_game_response):
    """Test processing a complete game response."""
    result = processor.process_game(13, sample_game_response)
    
    assert result is not None
    assert result["game_id"] == 13
    assert result["name"] == "Catan"
    assert result["year_published"] == 1995
    assert result["min_players"] == 3
    assert result["max_players"] == 4
    assert result["playing_time"] == 120
    assert result["min_age"] == 10
    assert "Negotiation" in result["categories"]
    assert "Dice Rolling" in result["mechanics"]
    assert isinstance(result["load_timestamp"], datetime)

def test_process_game_invalid(processor):
    """Test processing invalid game response."""
    result = processor.process_game(13, {"items": {}})
    assert result is None

def test_prepare_for_bigquery(processor):
    """Test preparing data for BigQuery."""
    processed_games = [
        {
            "game_id": 13,
            "name": "Catan",
            "categories": ["Strategy", "Negotiation"],
            "mechanics": ["Dice Rolling", "Trading"],
            "load_timestamp": datetime.utcnow()
        },
        {
            "game_id": 14,
            "name": "Ticket to Ride",
            "categories": ["Strategy", "Family"],
            "mechanics": ["Set Collection", "Route Building"],
            "load_timestamp": datetime.utcnow()
        }
    ]
    
    games_df, categories_df, mechanics_df = processor.prepare_for_bigquery(processed_games)
    
    # Check games DataFrame
    assert len(games_df) == 2
    assert "game_id" in games_df.columns
    assert "name" in games_df.columns
    
    # Check categories DataFrame
    assert len(categories_df) == 3  # Unique categories
    assert "category_id" in categories_df.columns
    assert "category_name" in categories_df.columns
    
    # Check mechanics DataFrame
    assert len(mechanics_df) == 4  # Unique mechanics
    assert "mechanic_id" in mechanics_df.columns
    assert "mechanic_name" in mechanics_df.columns

def test_validate_data(processor):
    """Test data validation."""
    # Valid games data
    valid_games = pl.DataFrame({
        "game_id": [1, 2],
        "name": ["Game 1", "Game 2"],
        "load_timestamp": [datetime.utcnow(), datetime.utcnow()]
    })
    assert processor.validate_data(valid_games, "games") is True
    
    # Invalid games data (missing required column)
    invalid_games = pl.DataFrame({
        "game_id": [1, 2]
    })
    assert processor.validate_data(invalid_games, "games") is False
    
    # Invalid games data (duplicate IDs)
    duplicate_games = pl.DataFrame({
        "game_id": [1, 1],
        "name": ["Game 1", "Game 1"],
        "load_timestamp": [datetime.utcnow(), datetime.utcnow()]
    })
    assert processor.validate_data(duplicate_games, "games") is False
