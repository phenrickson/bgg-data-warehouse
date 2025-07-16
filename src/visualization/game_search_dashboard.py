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


@st.cache_data(ttl=86400)  # Cache publishers for 24 hours
def get_publishers():
    """Fetch list of unique publishers sorted by frequency."""
    query = f"""
    SELECT p.publisher_id, p.name, COUNT(DISTINCT gp.game_id) as game_count
    FROM `{config['project']['id']}.{config['project']['dataset']}.publishers` p
    JOIN `{config['project']['id']}.{config['project']['dataset']}.game_publishers` gp 
        ON p.publisher_id = gp.publisher_id
    GROUP BY p.publisher_id, p.name
    ORDER BY game_count DESC, p.name
    """
    return [{"id": row.publisher_id, "name": row.name} for row in client.query(query)]


@st.cache_data(ttl=86400)  # Cache for 24 hours (86400 seconds)
def search_games(
    min_year=1900,  # Default to earliest possible year
    max_year=2025,  # Default to latest possible year
    publishers=None,
    min_best_player_count=None,
    max_best_player_count=None,
    min_rating=6.0,  # Default to a reasonable minimum rating
    max_rating=10.0,
    limit=25000,  # Increased limit for initial load
):
    """Advanced game search with comprehensive filtering."""
    base_query = f"""
    WITH player_count_best AS (
        SELECT 
            game_id, 
            STRING_AGG(DISTINCT CONCAT(player_count, ' (Best: ', CAST(best_percentage AS STRING), '%)'), '; ') AS player_count_recommendations,
            MAX(SAFE_CAST(player_count AS INT64)) as max_best_player_count
        FROM `{config['project']['id']}.{config['project']['dataset']}.player_count_recommendations`
        WHERE best_percentage > 50
        GROUP BY game_id
    ),
    filtered_games AS (
        SELECT 
            g.game_id,
            g.name,
            g.year_published,
            g.average_rating,
            g.bayes_average,
            g.average_weight,
            g.users_rated
        FROM `{config['project']['id']}.{config['project']['dataset']}.games_active` g
        WHERE 
            g.bayes_average IS NOT NULL 
            AND g.bayes_average > 0
            {f'AND g.game_id IN (SELECT game_id FROM `{config["project"]["id"]}.{config["project"]["dataset"]}.game_publishers` WHERE publisher_id IN ({", ".join(map(str, publishers))}))' if publishers else ''}
            {f'AND g.year_published >= {min_year}' if min_year else ''}
            {f'AND g.year_published <= {max_year}' if max_year else ''}
            {f'AND g.bayes_average >= {min_rating}' if min_rating else ''}
            {f'AND g.bayes_average <= {max_rating}' if max_rating else ''}
    )
    SELECT 
        fg.game_id,
        fg.name,
        fg.year_published,
        fg.average_rating,
        fg.bayes_average,
        fg.average_weight,
        fg.users_rated,
        pcb.player_count_recommendations
    FROM filtered_games fg
    LEFT JOIN player_count_best pcb ON fg.game_id = pcb.game_id
    WHERE 1=1
    {f'AND SAFE_CAST(SPLIT(pcb.player_count_recommendations, " ")[SAFE_OFFSET(0)] AS INT64) >= {min_best_player_count}' if min_best_player_count else ''}
    {f'AND SAFE_CAST(SPLIT(pcb.player_count_recommendations, " ")[SAFE_OFFSET(0)] AS INT64) <= {max_best_player_count}' if max_best_player_count else ''}
    ORDER BY fg.bayes_average DESC
    LIMIT {limit}
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
    selected_publishers = st.sidebar.multiselect(
        "Select Publishers", [p["name"] for p in all_publishers], format_func=lambda x: x
    )

    # Convert selected publisher names back to IDs
    selected_publisher_ids = [p["id"] for p in all_publishers if p["name"] in selected_publishers]

    # Player Count Filter
    player_count_range = st.sidebar.slider(
        "Best Player Count", min_value=1, max_value=10, value=(2, 4)
    )

    # Initial load of games
    with st.spinner("Loading games..."):
        results = search_games(
            min_year=year_range[0],
            max_year=year_range[1],
            publishers=selected_publisher_ids,
            min_best_player_count=player_count_range[0],
            max_best_player_count=player_count_range[1],
            min_rating=rating_range[0],
            max_rating=rating_range[1],
        )

    # Display summary statistics
    if not results.empty:
        # Calculate summary statistics
        total_games = len(results)
        avg_complexity = results["average_weight"].mean()
        avg_rating = results["bayes_average"].mean()
        avg_users_rated = results["users_rated"].mean()
        avg_geek_rating = results["average_rating"].mean()

        # Display summary statistics
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Games", f"{total_games:,}")
        with col2:
            st.metric("Avg Complexity", f"{avg_complexity:.2f}")
        with col3:
            st.metric("Avg Bayes Rating", f"{avg_rating:.2f}")
        with col4:
            st.metric("Avg Users Rated", f"{avg_users_rated:,.0f}")
        with col5:
            st.metric("Avg Geek Rating", f"{avg_geek_rating:.2f}")

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
            },
            hide_index=True,
        )
    else:
        st.warning("No games found matching your criteria.")


if __name__ == "__main__":
    main()
