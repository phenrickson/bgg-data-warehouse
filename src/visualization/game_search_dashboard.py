"""BGG Game Search Dashboard."""

import os
import sys
import streamlit as st
import pandas as pd
import yaml
from google.auth import default
from google.cloud import bigquery
from dotenv import load_dotenv

# Add project to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Page config
st.set_page_config(page_title="BGG Game Search", page_icon="🎲", layout="wide")

# Load environment variables
load_dotenv()


# BigQuery configuration
def get_bigquery_config():
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "bigquery.yaml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    env = os.getenv("ENVIRONMENT", "dev")
    env_config = config["environments"][env]
    return {
        "project": {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "location": env_config["location"],
        }
    }


# Initialize BigQuery client
credentials, _ = default()
config = get_bigquery_config()
client = bigquery.Client(credentials=credentials, project=config["project"]["id"])


@st.cache_data(ttl=3600)
def get_publishers():
    """Fetch list of unique publishers sorted by frequency."""
    query = f"""
    SELECT p.name, COUNT(*) as game_count
    FROM `{config['project']['id']}.{config['project']['dataset']}.game_publishers` gp
    JOIN `{config['project']['id']}.{config['project']['dataset']}.publishers` p 
        ON gp.publisher_id = p.publisher_id
    GROUP BY p.name
    ORDER BY game_count DESC, p.name
    """
    return [row.name for row in client.query(query)]


@st.cache_data(ttl=3600)
def search_games(
    min_year=None,
    max_year=None,
    publishers=None,
    min_best_player_count=None,
    max_best_player_count=None,
    min_rating=None,
    max_rating=None,
):
    """Advanced game search with comprehensive filtering."""
    # Build query based on whether we need player count filtering
    if min_best_player_count or max_best_player_count:
        base_query = f"""
        WITH game_publishers_agg AS (
            SELECT 
                game_id, 
                STRING_AGG(DISTINCT p.name, ', ') AS publishers
            FROM `{config['project']['id']}.{config['project']['dataset']}.game_publishers` gp
            JOIN `{config['project']['id']}.{config['project']['dataset']}.publishers` p 
                ON gp.publisher_id = p.publisher_id
            GROUP BY game_id
        ),
        player_count_best AS (
            SELECT 
                game_id, 
                SAFE_CAST(player_count AS INT64) as best_player_count,
                best_percentage
            FROM `{config['project']['id']}.{config['project']['dataset']}.player_count_recommendations`
            WHERE best_percentage > 50
            AND SAFE_CAST(player_count AS INT64) IS NOT NULL
        )
        SELECT 
            g.game_id,
            g.name,
            g.year_published,
            g.average_rating,
            g.bayes_average,
            g.average_weight,
            g.users_rated,
            gpa.publishers,
            pcb.best_player_count,
            pcb.best_percentage
        FROM `{config['project']['id']}.{config['project']['dataset']}.games_active` g
        JOIN player_count_best pcb ON g.game_id = pcb.game_id
        LEFT JOIN game_publishers_agg gpa ON g.game_id = gpa.game_id
        WHERE 
            g.bayes_average IS NOT NULL 
            AND g.bayes_average > 0
        """
    else:
        base_query = f"""
        WITH game_publishers_agg AS (
            SELECT 
                game_id, 
                STRING_AGG(DISTINCT p.name, ', ') AS publishers
            FROM `{config['project']['id']}.{config['project']['dataset']}.game_publishers` gp
            JOIN `{config['project']['id']}.{config['project']['dataset']}.publishers` p 
                ON gp.publisher_id = p.publisher_id
            GROUP BY game_id
        )
        SELECT 
            g.game_id,
            g.name,
            g.year_published,
            g.average_rating,
            g.bayes_average,
            g.average_weight,
            g.users_rated,
            gpa.publishers
        FROM `{config['project']['id']}.{config['project']['dataset']}.games_active` g
        LEFT JOIN game_publishers_agg gpa ON g.game_id = gpa.game_id
        WHERE 
            g.bayes_average IS NOT NULL 
            AND g.bayes_average > 0
        """

    conditions = []
    if min_year:
        conditions.append(f"g.year_published >= {min_year}")
    if max_year:
        conditions.append(f"g.year_published <= {max_year}")
    if publishers:
        publisher_condition = " OR ".join([f"gpa.publishers LIKE '%{p}%'" for p in publishers])
        conditions.append(f"({publisher_condition})")
    if min_best_player_count:
        conditions.append(f"pcb.best_player_count >= {min_best_player_count}")
    if max_best_player_count:
        conditions.append(f"pcb.best_player_count <= {max_best_player_count}")
    if min_rating:
        conditions.append(f"g.bayes_average >= {min_rating}")
    if max_rating:
        conditions.append(f"g.bayes_average <= {max_rating}")

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    base_query += """
    ORDER BY g.bayes_average DESC
    LIMIT 500
    """

    return client.query(base_query).to_dataframe()


def main():
    st.title("🎲 BGG Game Search")

    # Sidebar for filters
    st.sidebar.header("🔍 Game Search Filters")

    # Year Published Filter
    year_range = st.sidebar.slider(
        "Year Published", min_value=1900, max_value=2025, value=(1990, 2025)
    )

    # Rating Filter
    rating_range = st.sidebar.slider(
        "Bayes Average Rating", min_value=0.0, max_value=10.0, value=(6.0, 10.0), step=0.1
    )

    # Publishers Filter
    all_publishers = get_publishers()
    selected_publishers = st.sidebar.multiselect("Select Publishers", all_publishers)

    # Player Count Filter
    player_count_range = st.sidebar.slider(
        "Best Player Count", min_value=1, max_value=10, value=(2, 4)
    )

    # Search Button
    search_clicked = st.sidebar.button("🔎 Search Games")

    # Perform search when button is clicked
    if search_clicked:
        with st.spinner("Searching games..."):
            results = search_games(
                min_year=year_range[0],
                max_year=year_range[1],
                publishers=selected_publishers,
                min_best_player_count=player_count_range[0],
                max_best_player_count=player_count_range[1],
                min_rating=rating_range[0],
                max_rating=rating_range[1],
            )

        if not results.empty:
            # Game Count and Stats
            st.subheader(f"🎲 Found {len(results)} Games")

            # Add BGG URL column
            results["bgg_url"] = results["game_id"].apply(
                lambda x: f"https://boardgamegeek.com/boardgame/{x}"
            )

            # Simple table with requested columns
            st.dataframe(
                results,
                column_config={
                    "game_id": "Game ID",
                    "name": st.column_config.TextColumn("Game Name"),
                    "bgg_url": st.column_config.LinkColumn("BGG Link", display_text="View on BGG"),
                    "year_published": "Year Published",
                    "average_rating": "Average Rating",
                    "bayes_average": "Bayes Average",
                    "average_weight": "Average Weight",
                    "users_rated": "Users Rated",
                    "publishers": "Publishers",
                },
                hide_index=True,
            )
        else:
            st.warning("No games found matching your criteria.")


if __name__ == "__main__":
    main()
