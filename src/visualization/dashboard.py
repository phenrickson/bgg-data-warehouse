"""BGG Data Warehouse Monitoring Dashboard."""

import os
import sys
from datetime import datetime, timezone
import streamlit as st

# Page config must be the first Streamlit command
st.set_page_config(
    page_title="BGG Data Warehouse Monitor",
    page_icon="🎲",
    layout="wide"
)

import pandas as pd
import yaml
from google.cloud import bigquery
from dotenv import load_dotenv

# add to project path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Load environment variables from .env file
load_dotenv()

# Set Google Application Credentials
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/service-account-key.json"

# Import local modules directly
import src.visualization.queries as queries
import src.visualization.components as components

# Load BigQuery config directly
def get_bigquery_config():
    """Get BigQuery configuration directly."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "bigquery.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Get environment from .env
    env = os.getenv("ENVIRONMENT", "dev")
    
    # Build config with environment-specific values
    env_config = config["environments"][env]
    return {
        "project": {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "location": env_config["location"]
        },
        "datasets": config.get("datasets", {})
    }

# Page config is now at the top of the file

# Initialize BigQuery client
client = bigquery.Client()

def format_project_dataset(query: str) -> str:
    """Format query with project and dataset."""
    # Get BigQuery configuration from config module
    config = get_bigquery_config()
    project_id = config["project"]["id"]
    dataset = config["project"]["dataset"]
    return query.replace("${project_id}", project_id).replace("${dataset}", dataset)

def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return results as DataFrame."""
    formatted_query = format_project_dataset(query)
    return client.query(formatted_query).to_dataframe()

def main():
    """Main dashboard function."""
    st.title("🎲 BGG Data Warehouse Monitoring")
    st.write("Real-time monitoring of the BGG data pipeline")
    
    # Current timestamp
    current_time = datetime.now(timezone.utc)
    st.write(f"Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # Top metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
        
    with col1:
        total_games = run_query(queries.TOTAL_GAMES_QUERY)
        components.create_metric_card(
            "Total Games",
            total_games.iloc[0]["total_games"]
        )
    
    with col2:
        games_with_bayes = run_query(queries.GAMES_WITH_BAYESAVERAGE_QUERY)
        components.create_metric_card(
            "Ranked Games",
            games_with_bayes.iloc[0]["games_with_bayesaverage"]
        )
    
    with col3:
        processing_status = run_query(queries.PROCESSING_STATUS)
        components.create_metric_card(
            "Responses Last 7 Days",
            processing_status.iloc[0]["total_responses"]
        )
    
    with col4:
        components.create_metric_card(
            "Processing Success Rate",
            f"{processing_status.iloc[0]['success_rate']}%"
        )
        
    with col5:
        unprocessed = run_query(queries.UNPROCESSED_RESPONSES_QUERY)
        components.create_metric_card(
            "Unprocessed Responses",
            unprocessed.iloc[0]["unprocessed_count"]
        )
    
    # Entity counts row
    st.subheader("Game Metadata Counts")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        categories = run_query(queries.DISTINCT_CATEGORIES_QUERY)
        components.create_metric_card(
            "Categories",
            categories.iloc[0]["category_count"]
        )
    
    with col2:
        mechanics = run_query(queries.DISTINCT_MECHANICS_QUERY)
        components.create_metric_card(
            "Mechanics",
            mechanics.iloc[0]["mechanic_count"]
        )
    
    with col3:
        families = run_query(queries.DISTINCT_FAMILIES_QUERY)
        components.create_metric_card(
            "Families",
            families.iloc[0]["family_count"]
        )
    
    with col4:
        designers = run_query(queries.DISTINCT_DESIGNERS_QUERY)
        components.create_metric_card(
            "Designers",
            designers.iloc[0]["designer_count"]
        )
    
    with col5:
        artists = run_query(queries.DISTINCT_ARTISTS_QUERY)
        components.create_metric_card(
            "Artists",
            artists.iloc[0]["artist_count"]
        )
    
    with col6:
        publishers = run_query(queries.DISTINCT_PUBLISHERS_QUERY)
        components.create_metric_card(
            "Publishers",
            publishers.iloc[0]["publisher_count"]
        )
    
    # Time series charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Fetch Activity")
        fetch_data = run_query(queries.RECENT_FETCH_ACTIVITY)
        components.create_time_series(
            fetch_data,
            "date",
            "responses_fetched",
            "Daily Fetch Counts"
        )
    
    with col2:
        st.subheader("Processing Activity")
        processing_data = run_query(queries.DAILY_PROCESSING_COUNTS)
        components.create_time_series(
            processing_data,
            "date",
            "processed_count",
            "Daily Processing Counts",
            color="#2ca02c"
        )
    
    # Latest games
    st.subheader("Latest Games Added")
    latest_games = run_query(queries.LATEST_GAMES)
    components.create_latest_games_table(latest_games)
    
    # Error trends
    st.subheader("Error Trends")
    error_trends = run_query(queries.PROCESSING_ERROR_TRENDS)
    components.create_time_series(
        error_trends,
        "date",
        "error_count",
        "Daily Error Counts",
        color="#d62728"
    )
    
    # Recent errors
    st.subheader("Recent Processing Errors")
    recent_errors = run_query(queries.RECENT_ERRORS)
    if not recent_errors.empty:
        components.create_error_table(recent_errors)
    else:
        st.info("No recent processing errors! 🎉")

if __name__ == "__main__":
    main()
