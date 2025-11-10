"""BGG Data Warehouse Monitoring Dashboard."""

import os
import sys
import logging
from datetime import datetime, timezone
import streamlit as st

# Import logging configuration
import pandas as pd
import yaml
from google.auth import default
from google.cloud import bigquery
from dotenv import load_dotenv

# add to project path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Page config must be the first Streamlit command
st.set_page_config(page_title="BGG Data Warehouse Monitor", page_icon="🎲", layout="wide")

# Load environment variables from .env file
load_dotenv()

# Get environment variable
env = os.getenv("ENVIRONMENT", "prod")

# Import local modules directly
from src.utils.logging_config import setup_logging
import src.visualization.queries as queries
import src.visualization.components as components

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

# Use a hardcoded port value to avoid any issues with environment variables
port = 8501
logger.info(f"Starting Streamlit on port {port}")
# Set STREAMLIT_SERVER_PORT environment variable to ensure Streamlit uses this port
os.environ["STREAMLIT_SERVER_PORT"] = str(port)

logger.info(f"Using environment: {env}")


# Start health check server for Cloud Run
try:
    from src.visualization.health_check import start_health_check_server

    health_server = start_health_check_server()
    logger.info("Health check server started")
except Exception as e:
    logger.warning(f"Could not start health check server: {e}")


# Load BigQuery config directly
def get_bigquery_config():
    """Get BigQuery configuration directly."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "bigquery.yaml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Get environment from .env
    env = os.getenv("ENVIRONMENT", "prod")
    logger.debug(f"Loading config for environment: {env}")

    # Build config with environment-specific values
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

    # Debug: Log configuration
    logger.debug(f"Config loaded: {result}")
    return result


# Page config is now at the top of the file

# Initialize BigQuery client
# Get credentials using google.auth.default()
credentials, _ = default()

# Get project ID from config or environment variable
config = get_bigquery_config()
project_id = os.getenv("GCP_PROJECT_ID", config["project"]["id"])

# Create BigQuery client with explicit credentials and project
client = bigquery.Client(credentials=credentials, project=project_id)


def format_project_dataset(query: str) -> str:
    """Format query with project and dataset."""
    # Get BigQuery configuration from config module
    config = get_bigquery_config()
    project_id = config["project"]["id"]
    dataset = config["project"]["dataset"]
    raw_dataset = config["project"].get(
        "raw", "bgg_raw_prod"
    )  # Get raw dataset or default to bgg_raw_prod

    logger.debug(
        f"Formatting query with: project_id={project_id}, dataset={dataset}, raw_dataset={raw_dataset}"
    )

    # Replace all placeholders
    formatted_query = query.replace("${project_id}", project_id)
    formatted_query = formatted_query.replace("${dataset}", dataset)
    formatted_query = formatted_query.replace("${raw_dataset}", raw_dataset)

    # Debug: Log a sample of the formatted query (first 100 chars)
    logger.debug(f"Formatted query sample: {formatted_query[:min(100, len(formatted_query))]}")

    return formatted_query


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return results as DataFrame with caching."""
    formatted_query = format_project_dataset(query)
    result = client.query(formatted_query).to_dataframe()
    logger.debug(f"Query returned {len(result)} rows")
    if not result.empty and len(result) < 10:
        logger.debug(f"Sample data: {result.head().to_dict()}")
    return result


def debug_raw_data():
    """Debug function to check raw data directly."""
    config = get_bigquery_config()
    project_id = config["project"]["id"]
    raw_dataset = config["project"]["raw"]

    # Check for recent data in raw_responses
    query = f"""
    SELECT 
        DATE(fetch_timestamp) as date,
        COUNT(*) as count
    FROM `{project_id}.{raw_dataset}.raw_responses`
    WHERE fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    GROUP BY date
    ORDER BY date DESC
    LIMIT 14
    """

    logger.debug(f"Executing debug query: {query}")
    result = client.query(query).to_dataframe()
    logger.debug(f"Debug query results: {result.to_dict()}")
    return result


def main():
    """Main dashboard function."""
    st.title("🎲 BGG Data Warehouse Monitoring")
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


if __name__ == "__main__":
    main()
