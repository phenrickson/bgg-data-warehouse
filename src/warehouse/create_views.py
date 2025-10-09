"""Script to create BigQuery views for BGG Data Warehouse."""

import logging

from google.cloud import bigquery

from src.config import get_bigquery_config

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_views(environment=None):
    """Create BigQuery views for the data warehouse."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]

        logger.info(f"Creating views for project: {project_id}, dataset: {dataset}")

        with open("src/warehouse/bigquery_views.sql") as f:
            views_sql = f.read()

        # Replace placeholders with actual values
        views_sql = views_sql.replace("${project_id}", project_id)
        views_sql = views_sql.replace("${dataset}", dataset)

        # Split SQL into individual view creation statements
        view_statements = [stmt.strip() for stmt in views_sql.split(";") if stmt.strip()]

        for view_statement in view_statements:
            try:
                query_job = client.query(view_statement)
                query_job.result()
                logger.info("Successfully created/updated view")
            except Exception as view_error:
                logger.error(f"Error creating view: {view_error}")
                logger.error(f"Problematic SQL: {view_statement}")

        logger.info("View creation process completed")

    except Exception as e:
        logger.error(f"Unexpected error during view creation: {e}")
        raise


if __name__ == "__main__":
    create_views()
