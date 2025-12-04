"""New Games Added - Monitoring Page."""

import os
import sys
import logging
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
import yaml
from google.auth import default
from google.cloud import bigquery
from dotenv import load_dotenv

# Add to project path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

# Page config
st.set_page_config(
    page_title="New Games Added",
    page_icon="📥",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Import local modules
from src.utils.logging_config import setup_logging
import src.visualization.queries as queries
import src.visualization.components as components

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)


# Load BigQuery config
def get_bigquery_config():
    """Get BigQuery configuration directly."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "config",
        "bigquery.yaml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    env = os.getenv("ENVIRONMENT", "prod")
    env_config = config["environments"][env]
    result = {
        "project": {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "raw": env_config["raw"],
            "location": env_config["location"],
        },
        "datasets": config.get("datasets", {}),
    }
    return result


# Initialize BigQuery client
credentials, _ = default()
config = get_bigquery_config()
project_id = os.getenv("GCP_PROJECT_ID", config["project"]["id"])
client = bigquery.Client(credentials=credentials, project=project_id)


def format_project_dataset(query: str) -> str:
    """Format query with project and dataset."""
    config = get_bigquery_config()
    project_id = config["project"]["id"]
    dataset = config["project"]["dataset"]
    raw_dataset = config["project"].get("raw", "bgg_raw_prod")

    formatted_query = query.replace("${project_id}", project_id)
    formatted_query = formatted_query.replace("${dataset}", dataset)
    formatted_query = formatted_query.replace("${raw_dataset}", raw_dataset)
    return formatted_query


@st.cache_data(ttl=3600)
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return results as DataFrame with caching."""
    formatted_query = format_project_dataset(query)
    result = client.query(formatted_query).to_dataframe()
    return result


def main():
    """Main new games page function."""
    st.title("📥 New Games Added")
    st.write("Monitoring new games added to the warehouse")

    # Current timestamp
    current_time = datetime.now(timezone.utc)
    st.write(f"Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Load metrics
    with st.spinner("Loading new games metrics..."):
        new_games_fetched = run_query(queries.NEW_GAMES_FETCHED_QUERY)
        new_games_processed = run_query(queries.NEW_GAMES_PROCESSED_QUERY)

    # Metrics row
    st.subheader("New Games Activity (Last 7 Days)")
    col1, col2 = st.columns(2)

    with col1:
        components.create_metric_card(
            "New Games Fetched",
            new_games_fetched.iloc[0]["new_games_count"]
        )

    with col2:
        components.create_metric_card(
            "New Games Processed",
            new_games_processed.iloc[0]["new_games_processed"]
        )

    # Daily trend chart
    st.subheader("Daily New Games Fetched")
    new_games_data = run_query(queries.DAILY_NEW_GAMES_FETCHED)
    if not new_games_data.empty:
        components.create_time_series(
            new_games_data,
            "date",
            "new_games_count",
            "Daily New Games Fetched",
            color="#1f77b4"
        )
    else:
        st.info("No new games fetched in the last 7 days")

    # Latest new games table
    st.subheader("Latest New Games Added")
    latest_games = run_query(queries.LATEST_GAMES)
    if not latest_games.empty:
        components.create_latest_games_table(latest_games)
    else:
        st.info("No new games found")


if __name__ == "__main__":
    main()
