"""BGG Data Warehouse - Home Page."""

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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Page config must be the first Streamlit command
st.set_page_config(
    page_title="BGG Data Warehouse",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Get environment variable
env = os.getenv("ENVIRONMENT", "prod")

# Import local modules
from src.utils.logging_config import setup_logging
import src.visualization.queries as queries
import src.visualization.components as components

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

# Start health check server for Cloud Run
try:
    from src.visualization.health_check import start_health_check_server
    health_server = start_health_check_server()
    logger.info("Health check server started")
except Exception as e:
    logger.warning(f"Could not start health check server: {e}")


# Load BigQuery config
def get_bigquery_config():
    """Get BigQuery configuration directly."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "bigquery.yaml"
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
    """Main home page function."""
    st.title("BGG Data Warehouse")
    st.write("Monitoring and analytics for the BoardGameGeek data warehouse")

    # Current timestamp
    current_time = datetime.now(timezone.utc)
    st.write(f"Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    st.write(f"Environment: **{env}**")

    # Load summary metrics
    with st.spinner("Loading warehouse metrics..."):
        total_games = run_query(queries.TOTAL_GAMES_QUERY)
        games_with_bayes = run_query(queries.GAMES_WITH_BAYESAVERAGE_QUERY)
        entity_counts = run_query(queries.ALL_ENTITY_COUNTS_QUERY)
        processing_status = run_query(queries.PROCESSING_STATUS)
        unprocessed = run_query(queries.UNPROCESSED_RESPONSES_QUERY)

    # Top metrics row
    st.subheader("Warehouse Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        components.create_metric_card("Total Games", total_games.iloc[0]["total_games"])

    with col2:
        components.create_metric_card(
            "Ranked Games", games_with_bayes.iloc[0]["games_with_bayesaverage"]
        )

    with col3:
        components.create_metric_card(
            "Responses (7d)", processing_status.iloc[0]["total_responses"]
        )

    with col4:
        components.create_metric_card(
            "Success Rate", f"{processing_status.iloc[0]['success_rate']}%"
        )

    with col5:
        components.create_metric_card(
            "Unprocessed", unprocessed.iloc[0]["unprocessed_count"]
        )

    # Entity counts row
    st.subheader("Game Metadata")
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

    # Dataset information
    st.subheader("BigQuery Datasets")

    # Display dataset info
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Processed")
        st.markdown(f"`{config['project']['id']}.{config['project']['dataset']}`")

    with col2:
        st.markdown("#### Responses")
        st.markdown(f"`{config['project']['id']}.{config['project']['raw']}`")

    # List tables in expandable sections
    col1, col2 = st.columns(2)

    with col1:
        with st.expander("View Tables", expanded=False):
            try:
                processed_dataset = client.get_dataset(f"{config['project']['id']}.{config['project']['dataset']}")
                tables = list(client.list_tables(processed_dataset))
                if tables:
                    table_names = [table.table_id for table in tables]
                    # Display in a more compact format
                    st.code("\n".join(sorted(table_names)), language="text")
                else:
                    st.info("No tables found")
            except Exception as e:
                st.error(f"Could not list tables: {e}")

    with col2:
        with st.expander("View Tables", expanded=False):
            try:
                raw_dataset = client.get_dataset(f"{config['project']['id']}.{config['project']['raw']}")
                tables = list(client.list_tables(raw_dataset))
                if tables:
                    table_names = [table.table_id for table in tables]
                    # Display in a more compact format
                    st.code("\n".join(sorted(table_names)), language="text")
                else:
                    st.info("No tables found")
            except Exception as e:
                st.error(f"Could not list tables: {e}")

    # Error monitoring
    st.subheader("Recent Processing Errors")
    recent_errors = run_query(queries.RECENT_ERRORS)
    if not recent_errors.empty:
        components.create_error_table(recent_errors)
    else:
        st.success("No recent processing errors! 🎉")


if __name__ == "__main__":
    main()
