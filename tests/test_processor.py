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

def test_extract_names_single(processor):
    """Test extracting names when only one name exists."""
    item = {"name": {"@type": "primary", "@value": "Birds and Binoculars", "@sortindex": "1"}}
    primary_name, alternate_names = processor._extract_names(item)
    assert primary_name == "Birds and Binoculars"
    assert alternate_names == []

def test_extract_names_single_alternate(processor):
    """Test extracting names when only one alternate name exists."""
    item = {"name": {"@type": "alternate", "@value": "Birds", "@sortindex": "1"}}
    primary_name, alternate_names = processor._extract_names(item)
    assert primary_name == "Unknown"
    assert len(alternate_names) == 1
    assert alternate_names[0]["name"] == "Birds"

def test_extract_names_string(processor):
    """Test extracting names when name is a string."""
    item = {"name": "Birds"}
    primary_name, alternate_names = processor._extract_names(item)
    assert primary_name == "Unknown"
    assert len(alternate_names) == 1
    assert alternate_names[0]["name"] == "Birds"

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

def test_extract_year_zero(processor):
    """Test extracting year when value is zero."""
    item = {"yearpublished": {"@value": "0"}}
    year = processor._extract_year(item)
    assert year is None

def test_extract_year_string(processor):
    """Test extracting year when value is a string."""
    item = {"yearpublished": "1995"}
    year = processor._extract_year(item)
    assert year == 1995

def test_extract_year_string_zero(processor):
    """Test extracting year when string value is zero."""
    item = {"yearpublished": "0"}
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

def test_poll_results_single_result(processor):
    """Test extracting poll results when results is a single dict."""
    item = {
        "poll": {
            "@name": "suggested_numplayers",
            "results": {
                "@numplayers": "2",
                "result": [
                    {"@value": "Best", "@numvotes": "10"},
                    {"@value": "Recommended", "@numvotes": "5"},
                    {"@value": "Not Recommended", "@numvotes": "2"}
                ]
            }
        }
    }
    results = processor._extract_poll_results(item)
    assert len(results["suggested_players"]) == 1
    assert results["suggested_players"][0]["player_count"] == "2"
    assert results["suggested_players"][0]["best_votes"] == 10

def test_poll_results_empty_results(processor):
    """Test extracting poll results when results is empty."""
    item = {
        "poll": {
            "@name": "language_dependence",
            "results": {}
        }
    }
    results = processor._extract_poll_results(item)
    assert len(results["language_dependence"]) == 0

def test_poll_results_string_result(processor):
    """Test extracting poll results when result is a string."""
    item = {
        "poll": {
            "@name": "language_dependence",
            "results": {
                "result": "No votes"
            }
        }
    }
    results = processor._extract_poll_results(item)
    assert len(results["language_dependence"]) == 0

def test_poll_results_single_vote(processor):
    """Test extracting poll results when result is a single dict."""
    item = {
        "poll": {
            "@name": "language_dependence",
            "results": {
                "result": {
                    "@level": "1",
                    "@value": "No necessary in-game text",
                    "@numvotes": "5"
                }
            }
        }
    }
    results = processor._extract_poll_results(item)
    assert len(results["language_dependence"]) == 1
    assert results["language_dependence"][0]["level"] == 1
    assert results["language_dependence"][0]["votes"] == 5

def test_game_stats_string_values(processor):
    """Test GameStats handling string values."""
    stats = {
        "statistics": {
            "ratings": {
                "usersrated": "100",
                "average": "7.5",
                "owned": "50",
                "trading": "10",
                "wanting": "5",
                "wishing": "15",
                "numcomments": "25",
                "numweights": "20",
                "averageweight": "2.5"
            }
        }
    }
    game_stats = processor.GameStats(stats)
    assert game_stats.users_rated == 100
    assert game_stats.average == 7.5
    assert game_stats.owned == 50

def test_game_ranks_string_values(processor):
    """Test GameRanks handling string values."""
    stats = {
        "statistics": {
            "ratings": {
                "ranks": {
                    "rank": {
                        "@type": "subtype",
                        "@id": "1",
                        "@name": "boardgame",
                        "@friendlyname": "Board Game Rank",
                        "@value": "100",
                        "@bayesaverage": "7.5"
                    }
                }
            }
        }
    }
    game_ranks = processor.GameRanks(stats)
    assert len(game_ranks.ranks) == 1
    assert game_ranks.ranks[0]["value"] == 100
    assert game_ranks.ranks[0]["bayes_average"] == 7.5

def test_game_ranks_invalid_values(processor):
    """Test GameRanks handling invalid values."""
    stats = {
        "statistics": {
            "ratings": {
                "ranks": {
                    "rank": {
                        "@type": "subtype",
                        "@id": "1",
                        "@name": "boardgame",
                        "@friendlyname": "Board Game Rank",
                        "@value": "invalid",
                        "@bayesaverage": "N/A"
                    }
                }
            }
        }
    }
    game_ranks = processor.GameRanks(stats)
    assert len(game_ranks.ranks) == 1
    assert game_ranks.ranks[0]["value"] == 0
    assert game_ranks.ranks[0]["bayes_average"] == 0.0

def test_game_ranks_not_ranked(processor):
    """Test GameRanks handling 'Not Ranked' values."""
    stats = {
        "statistics": {
            "ratings": {
                "ranks": {
                    "rank": {
                        "@type": "subtype",
                        "@id": "1",
                        "@name": "boardgame",
                        "@friendlyname": "Board Game Rank",
                        "@value": "Not Ranked",
                        "@bayesaverage": "Not Ranked"
                    }
                }
            }
        }
    }
    game_ranks = processor.GameRanks(stats)
    assert len(game_ranks.ranks) == 0

def test_game_stats_invalid_values(processor):
    """Test GameStats handling invalid values."""
    stats = {
        "statistics": {
            "ratings": {
                "usersrated": "invalid",
                "average": "N/A",
                "owned": "-1",
                "trading": None,
                "wanting": "",
                "wishing": "0",
                "numcomments": "invalid",
                "numweights": "-5",
                "averageweight": "invalid"
            }
        }
    }
    game_stats = processor.GameStats(stats)
    assert game_stats.users_rated == 0
    assert game_stats.average == 0.0
    assert game_stats.owned == 0
    assert game_stats.trading == 0
    assert game_stats.wanting == 0
    assert game_stats.wishing == 0
    assert game_stats.num_comments == 0
    assert game_stats.num_weights == 0
    assert game_stats.average_weight == 0.0

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
    result = processor.process_game(13, {"items": {}}, "boardgame")
    assert result is None

def test_process_game_single_name(processor):
    """Test processing game with single name entry."""
    response = {
        "items": {
            "item": {
                "@id": "11546",
                "@type": "boardgame",
                "name": {
                    "@type": "primary",
                    "@sortindex": "1",
                    "@value": "Birds and Binoculars"
                },
                "yearpublished": {"@value": "2004"},
                "statistics": {
                    "ratings": {
                        "average": {"@value": "5"},
                        "usersrated": {"@value": "3"},
                        "owned": {"@value": "15"}
                    }
                }
            }
        }
    }
    result = processor.process_game(11546, response, "boardgame")
    assert result is not None
    assert result["primary_name"] == "Birds and Binoculars"
    assert result["type"] == "boardgame"

def test_process_game_expansion(processor):
    """Test processing a game expansion."""
    response = {
        "items": {
            "item": {
                "@id": "39953",
                "@type": "boardgameexpansion",
                "name": {
                    "@type": "primary",
                    "@value": "Catan: Seafarers",
                    "@sortindex": "1"
                },
                "yearpublished": {"@value": "1997"},
                "statistics": {
                    "ratings": {
                        "average": {"@value": "7.2"},
                        "usersrated": {"@value": "100"},
                        "owned": {"@value": "50"},
                        "ranks": {
                            "rank": {
                                "@type": "family",
                                "@id": "7481",
                                "@name": "expansions",
                                "@friendlyname": "Expansion Rank",
                                "@value": "100",
                                "@bayesaverage": "7.0"
                            }
                        }
                    }
                }
            }
        }
    }
    result = processor.process_game(39953, response, "boardgameexpansion")
    assert result is not None
    assert result["type"] == "boardgameexpansion"
    assert result["primary_name"] == "Catan: Seafarers"
    assert result["year_published"] == 1997
    assert len(result["rankings"]) == 1
    assert result["rankings"][0]["name"] == "expansions"
    assert result["rankings"][0]["value"] == 100

def test_process_game_zero_values(processor):
    """Test processing game with zero/invalid numeric values."""
    response = {
        "items": {
            "item": {
                "@id": "10448",
                "@type": "boardgame",
                "name": {"@type": "primary", "@value": "Test Game"},
                "yearpublished": {"@value": "0"},
                "minplayers": {"@value": "0"},
                "maxplayers": {"@value": "0"},
                "playingtime": {"@value": "invalid"},
                "statistics": {
                    "ratings": {
                        "average": "invalid",
                        "usersrated": "0",
                        "owned": "-1"
                    }
                }
            }
        }
    }
    result = processor.process_game(10448, response, "boardgame")
    assert result is not None
    assert result["year_published"] is None
    assert result["min_players"] == 0
    assert result["max_players"] == 0
    assert result["playing_time"] == 0

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
