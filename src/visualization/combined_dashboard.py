"""BGG Data Warehouse Combined Dashboard."""

import os
import sys
import streamlit as st
import pandas as pd
import yaml
from datetime import datetime, timezone
from google.auth import default
from google.cloud import bigquery
from dotenv import load_dotenv

# Add project to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Page config
st.set_page_config(page_title="BGG Data Warehouse", page_icon="🎲", layout="wide")

# Load environment variables
load_dotenv()

# Import local modules
import src.visualization.queries as queries
import src.visualization.components as components


# BigQuery configuration
def get_bigquery_config():
    """Get BigQuery configuration."""
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
        },
        "datasets": config.get("datasets", {}),
    }


# Initialize BigQuery client
credentials, _ = default()
config = get_bigquery_config()
client = bigquery.Client(credentials=credentials, project=config["project"]["id"])


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return results as DataFrame with caching."""
    formatted_query = query.replace("${project_id}", config["project"]["id"]).replace(
        "${dataset}", config["project"]["dataset"]
    )
    return client.query(formatted_query).to_dataframe()


# Utility functions from game_search_dashboard.py
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_publishers():
    """Fetch top 500 unique publishers sorted alphabetically."""
    query = f"""
    WITH publisher_counts AS (
        SELECT 
            p.publisher_id, 
            p.name, 
            COUNT(DISTINCT gp.game_id) as game_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT gp.game_id) DESC) as rank
        FROM `{config['project']['id']}.{config['project']['dataset']}.publishers` p
        JOIN `{config['project']['id']}.{config['project']['dataset']}.game_publishers` gp 
            ON p.publisher_id = gp.publisher_id
        GROUP BY p.publisher_id, p.name
    )
    SELECT publisher_id, name
    FROM publisher_counts
    WHERE rank <= 500
    ORDER BY name
    """
    return [{"id": row.publisher_id, "name": row.name} for row in client.query(query)]


@st.cache_data(ttl=86400)  # Cache designers for 24 hours
def get_designers():
    """Fetch top 1000 unique designers sorted alphabetically."""
    query = f"""
    WITH designer_counts AS (
        SELECT 
            d.designer_id, 
            d.name, 
            COUNT(DISTINCT gd.game_id) as game_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT gd.game_id) DESC) as rank
        FROM `{config['project']['id']}.{config['project']['dataset']}.designers` d
        JOIN `{config['project']['id']}.{config['project']['dataset']}.game_designers` gd 
            ON d.designer_id = gd.designer_id
        GROUP BY d.designer_id, d.name
    )
    SELECT designer_id, name
    FROM designer_counts
    WHERE rank <= 1000
    ORDER BY name
    """
    return [{"id": row.designer_id, "name": row.name} for row in client.query(query)]


@st.cache_data(ttl=86400)  # Cache categories for 24 hours
def get_categories():
    """Fetch top 500 unique categories sorted alphabetically."""
    query = f"""
    WITH category_counts AS (
        SELECT 
            c.category_id, 
            c.name, 
            COUNT(DISTINCT gc.game_id) as game_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT gc.game_id) DESC) as rank
        FROM `{config['project']['id']}.{config['project']['dataset']}.categories` c
        JOIN `{config['project']['id']}.{config['project']['dataset']}.game_categories` gc 
            ON c.category_id = gc.category_id
        GROUP BY c.category_id, c.name
    )
    SELECT category_id, name
    FROM category_counts
    WHERE rank <= 500
    ORDER BY name
    """
    return [{"id": row.category_id, "name": row.name} for row in client.query(query)]


@st.cache_data(ttl=86400)  # Cache mechanics for 24 hours
def get_mechanics():
    """Fetch top 500 unique mechanics sorted alphabetically."""
    query = f"""
    WITH mechanic_counts AS (
        SELECT 
            m.mechanic_id, 
            m.name, 
            COUNT(DISTINCT gm.game_id) as game_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT gm.game_id) DESC) as rank
        FROM `{config['project']['id']}.{config['project']['dataset']}.mechanics` m
        JOIN `{config['project']['id']}.{config['project']['dataset']}.game_mechanics` gm 
            ON m.mechanic_id = gm.mechanic_id
        GROUP BY m.mechanic_id, m.name
    )
    SELECT mechanic_id, name
    FROM mechanic_counts
    WHERE rank <= 500
    ORDER BY name
    """
    return [{"id": row.mechanic_id, "name": row.name} for row in client.query(query)]


@st.cache_data(ttl=86400)  # Cache for 24 hours (86400 seconds)
def search_games(
    min_year=1900,
    max_year=2025,
    publishers=None,
    designers=None,
    categories=None,
    mechanics=None,
    min_recommended_player_count=None,
    max_recommended_player_count=None,
    min_best_player_count=None,
    max_best_player_count=None,
    min_recommended_percentage=50,
    min_best_percentage=50,
    min_rating=6.0,
    max_rating=10.0,
    min_complexity_weight=0.0,
    max_complexity_weight=5.0,
    limit=50000,
):
    """Advanced game search with comprehensive filtering."""
    base_query = f"""
    WITH player_count_data AS (
        SELECT 
            game_id, 
            STRING_AGG(
                DISTINCT CASE 
                    WHEN recommended_percentage >= {min_recommended_percentage} 
                    THEN CAST(player_count AS STRING) 
                END, 
                '; '
            ) AS recommended_player_counts,
            STRING_AGG(
                DISTINCT CASE 
                    WHEN best_percentage >= {min_best_percentage} 
                    THEN CAST(player_count AS STRING) 
                END, 
                '; '
            ) AS best_player_counts
        FROM `{config['project']['id']}.{config['project']['dataset']}.player_count_recommendations`
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
            {f'AND g.game_id IN (SELECT game_id FROM `{config["project"]["id"]}.{config["project"]["dataset"]}.game_designers` WHERE designer_id IN ({", ".join(map(str, designers))}))' if designers else ''}
            {f'AND g.game_id IN (SELECT game_id FROM `{config["project"]["id"]}.{config["project"]["dataset"]}.game_categories` WHERE category_id IN ({", ".join(map(str, categories))}))' if categories else ''}
            {f'AND g.game_id IN (SELECT game_id FROM `{config["project"]["id"]}.{config["project"]["dataset"]}.game_mechanics` WHERE mechanic_id IN ({", ".join(map(str, mechanics))}))' if mechanics else ''}
            {f'AND g.year_published >= {min_year}' if min_year else ''}
            {f'AND g.year_published <= {max_year}' if max_year else ''}
            {f'AND g.bayes_average >= {min_rating}' if min_rating else ''}
            {f'AND g.bayes_average <= {max_rating}' if max_rating else ''}
            {f'AND g.average_weight >= {min_complexity_weight}' if min_complexity_weight is not None else ''}
            {f'AND g.average_weight <= {max_complexity_weight}' if max_complexity_weight is not None else ''}
    )
    SELECT 
        fg.game_id,
        fg.name,
        fg.year_published,
        fg.average_rating,
        fg.bayes_average,
        fg.average_weight,
        fg.users_rated,
        pcd.recommended_player_counts,
        pcd.best_player_counts
    FROM filtered_games fg
    LEFT JOIN player_count_data pcd ON fg.game_id = pcd.game_id
    WHERE 1=1
    {f'AND SAFE_CAST(SPLIT(pcd.recommended_player_counts, "; ")[SAFE_OFFSET(0)] AS INT64) >= {min_recommended_player_count}' if min_recommended_player_count else ''}
    {f'AND SAFE_CAST(SPLIT(pcd.recommended_player_counts, "; ")[SAFE_OFFSET(0)] AS INT64) <= {max_recommended_player_count}' if max_recommended_player_count else ''}
    {f'AND SAFE_CAST(SPLIT(pcd.best_player_counts, "; ")[SAFE_OFFSET(0)] AS INT64) >= {min_best_player_count}' if min_best_player_count else ''}
    {f'AND SAFE_CAST(SPLIT(pcd.best_player_counts, "; ")[SAFE_OFFSET(0)] AS INT64) <= {max_best_player_count}' if max_best_player_count else ''}
    ORDER BY fg.bayes_average DESC
    LIMIT {limit}
    """

    return client.query(base_query).to_dataframe()


def database_monitor_panel():
    """Database monitoring panel functionality."""
    st.write("Real-time monitoring of the BGG data pipeline")

    # Current timestamp
    current_time = datetime.now(timezone.utc)
    st.write(f"Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Load all metrics data at once
    with st.spinner("Loading dashboard data..."):
        # Get all the data we need upfront
        total_games = run_query(queries.TOTAL_GAMES_QUERY)
        games_with_bayes = run_query(queries.GAMES_WITH_BAYESAVERAGE_QUERY)
        processing_status = run_query(queries.PROCESSING_STATUS)
        unprocessed = run_query(queries.UNPROCESSED_RESPONSES_QUERY)

        # Get all entity counts with a single query
        entity_counts = run_query(queries.ALL_ENTITY_COUNTS_QUERY)

    # Top metrics row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        components.create_metric_card("Total Games", total_games.iloc[0]["total_games"])

    with col2:
        components.create_metric_card(
            "Ranked Games", games_with_bayes.iloc[0]["games_with_bayesaverage"]
        )

    with col3:
        components.create_metric_card(
            "Responses Last 7 Days", processing_status.iloc[0]["total_responses"]
        )

    with col4:
        components.create_metric_card(
            "Processing Success Rate", f"{processing_status.iloc[0]['success_rate']}%"
        )

    with col5:
        components.create_metric_card(
            "Unprocessed Responses", unprocessed.iloc[0]["unprocessed_count"]
        )

    # Entity counts row
    st.subheader("Game Metadata Counts")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        components.create_metric_card("Categories", entity_counts.iloc[0]["category_count"])

    with col2:
        components.create_metric_card("Mechanics", entity_counts.iloc[0]["mechanic_count"])

    with col3:
        components.create_metric_card("Families", entity_counts.iloc[0]["family_count"])

    with col4:
        components.create_metric_card("Designers", entity_counts.iloc[0]["designer_count"])

    with col5:
        components.create_metric_card("Artists", entity_counts.iloc[0]["artist_count"])

    with col6:
        components.create_metric_card("Publishers", entity_counts.iloc[0]["publisher_count"])

    # Time series charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Fetch Activity")
        fetch_data = run_query(queries.RECENT_FETCH_ACTIVITY)
        components.create_time_series(fetch_data, "date", "responses_fetched", "Daily Fetch Counts")

    with col2:
        st.subheader("Processing Activity")
        processing_data = run_query(queries.DAILY_PROCESSING_COUNTS)
        components.create_time_series(
            processing_data, "date", "processed_count", "Daily Processing Counts", color="#2ca02c"
        )

    # Latest games
    st.subheader("Latest Games Added")
    latest_games = run_query(queries.LATEST_GAMES)
    components.create_latest_games_table(latest_games)

    # Error trends
    st.subheader("Error Trends")
    error_trends = run_query(queries.PROCESSING_ERROR_TRENDS)
    components.create_time_series(
        error_trends, "date", "error_count", "Daily Error Counts", color="#d62728"
    )

    # Recent errors
    st.subheader("Recent Processing Errors")
    recent_errors = run_query(queries.RECENT_ERRORS)
    if not recent_errors.empty:
        components.create_error_table(recent_errors)
    else:
        st.info("No recent processing errors! 🎉")


def game_search_panel():
    """Game search panel functionality."""
    st.write("Search and filter board games from the BGG database")

    # Sidebar for filters
    st.sidebar.header("🔍 Game Filters")

    # Year Published Filter
    year_range = st.sidebar.slider(
        "Year Published", min_value=1900, max_value=2025, value=(1990, 2025)
    )

    # Rating Filter
    rating_range = st.sidebar.slider(
        "Geek Rating", min_value=6.0, max_value=10.0, value=(0.0, 10.0), step=0.1
    )

    # Complexity Weight Filter
    complexity_weight_range = st.sidebar.slider(
        "Complexity Weight", min_value=0.0, max_value=5.0, value=(0.0, 5.0), step=0.25
    )

    # Publishers Filter
    all_publishers = get_publishers()
    selected_publishers = st.sidebar.multiselect(
        "Select Publishers", [p["name"] for p in all_publishers], format_func=lambda x: x
    )

    # Convert selected publisher names back to IDs
    selected_publisher_ids = [p["id"] for p in all_publishers if p["name"] in selected_publishers]

    # Designers Filter
    all_designers = get_designers()
    selected_designers = st.sidebar.multiselect(
        "Select Designers", [d["name"] for d in all_designers], format_func=lambda x: x
    )

    # Convert selected designer names back to IDs
    selected_designer_ids = [d["id"] for d in all_designers if d["name"] in selected_designers]

    # Categories Filter
    all_categories = get_categories()
    selected_categories = st.sidebar.multiselect(
        "Select Categories", [c["name"] for c in all_categories], format_func=lambda x: x
    )

    # Convert selected category names back to IDs
    selected_category_ids = [c["id"] for c in all_categories if c["name"] in selected_categories]

    # Mechanics Filter
    all_mechanics = get_mechanics()
    selected_mechanics = st.sidebar.multiselect(
        "Select Mechanics", [m["name"] for m in all_mechanics], format_func=lambda x: x
    )

    # Convert selected mechanic names back to IDs
    selected_mechanic_ids = [m["id"] for m in all_mechanics if m["name"] in selected_mechanics]

    # Recommended Player Count Filter
    recommended_player_count_range = st.sidebar.slider(
        "Recommended Player Count", min_value=1, max_value=10, value=(1, 10)
    )

    # Best Player Count Filter
    best_player_count_range = st.sidebar.slider(
        "Best Player Count", min_value=1, max_value=10, value=(1, 10)
    )

    # Initial load of games
    with st.spinner("Loading games..."):
        results = search_games(
            min_year=year_range[0],
            max_year=year_range[1],
            publishers=selected_publisher_ids,
            designers=selected_designer_ids,
            categories=selected_category_ids,
            mechanics=selected_mechanic_ids,
            min_rating=rating_range[0],
            max_rating=rating_range[1],
            min_complexity_weight=complexity_weight_range[0],
            max_complexity_weight=complexity_weight_range[1],
            min_recommended_player_count=recommended_player_count_range[0],
            max_recommended_player_count=recommended_player_count_range[1],
            min_best_player_count=best_player_count_range[0],
            max_best_player_count=best_player_count_range[1],
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
            st.metric("Avg Rating", f"{avg_rating:.2f}")
        with col4:
            st.metric("Avg Users Rated", f"{avg_users_rated:,.0f}")
        with col5:
            st.metric("Avg Geek Rating", f"{avg_geek_rating:.2f}")

        # Add BGG URL column
        results["bgg_url"] = results["game_id"].apply(
            lambda x: f"https://boardgamegeek.com/boardgame/{x}"
        )

        # Paged table with requested columns
        st.dataframe(
            results,
            column_config={
                "game_id": "Game ID",
                "name": st.column_config.TextColumn("Game Name"),
                "bgg_url": st.column_config.LinkColumn("BGG Link", display_text="View on BGG"),
                "year_published": "Year Published",
                "average_rating": "Average Rating",
                "bayes_average": "Geek Rating",
                "average_weight": "Average Weight",
                "users_rated": "Users Rated",
                "recommended_player_counts": "Recommended Players",
                "best_player_counts": "Best Players",
            },
            use_container_width=True,  # Use full width of container
            hide_index=True,
            height=800,  # Optional: set a fixed height, or remove for auto-sizing
        )
    else:
        st.warning("No games found matching your criteria.")


def main():
    """Main dashboard function with tabs."""
    st.title("🎲 BGG Data Warehouse")

    # Create tabs
    monitor_tab, search_tab = st.tabs(["Database Monitor", "Game Search"])

    with monitor_tab:
        database_monitor_panel()

    with search_tab:
        game_search_panel()


if __name__ == "__main__":
    main()
