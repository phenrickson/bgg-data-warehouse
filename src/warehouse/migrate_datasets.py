"""Module for migrating BigQuery datasets."""

import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery

from src.utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def get_tables_and_views(client: bigquery.Client, dataset_ref: str) -> tuple[list[str], list[str]]:
    """
    Retrieve all table and view names in a given dataset, separated by type.

    Args:
        client (bigquery.Client): BigQuery client
        dataset_ref (str): Full dataset reference (project.dataset)

    Returns:
        tuple[List[str], List[str]]: Lists of (table_names, view_names)
    """
    tables = []
    views = []

    for table in client.list_tables(dataset_ref):
        table_obj = client.get_table(table.reference)
        if table_obj.table_type == "TABLE":
            tables.append(table.table_id)
        elif table_obj.table_type == "VIEW":
            views.append(table.table_id)

    return tables, views


def migrate_dataset(
    source_dataset: str,
    dest_dataset: str,
    project_id: str | None = None,
) -> None:
    """
    Migrate an entire dataset, copying all tables.

    Args:
        source_dataset (str): Source dataset name
        dest_dataset (str): Destination dataset name
        project_id (str, optional): Google Cloud project ID. If not provided,
            uses the project from the default client configuration.
    """
    # Create BigQuery client
    if not project_id:
        raise ValueError("project_id is required")
    client = bigquery.Client(project=project_id)

    # Source and destination dataset references
    source_ref = f"{client.project}.{source_dataset}"
    dest_ref = f"{client.project}.{dest_dataset}"

    # Check if destination dataset exists, create if not
    try:
        client.get_dataset(dest_ref)
    except Exception:
        # Create destination dataset with same location as source
        source_dataset_obj = client.get_dataset(source_ref)
        dataset = bigquery.Dataset(dest_ref)
        dataset.location = source_dataset_obj.location
        client.create_dataset(dataset)
        logger.info(f"Created destination dataset: {dest_ref}")

    # Get tables and views separately
    tables, views = get_tables_and_views(client, source_ref)
    logger.info(f"Found {len(tables)} tables and {len(views)} views")

    # Copy each table (not views)
    for table_name in tables:
        source_table = f"{source_ref}.{table_name}"
        dest_table = f"{dest_ref}.{table_name}"

        # Get source table metadata to preserve partitioning and clustering
        source_table_obj = client.get_table(source_table)

        # Check if destination table exists and drop it if it does
        try:
            client.get_table(dest_table)
            logger.info(f"Dropping existing table: {table_name}")
            client.delete_table(dest_table)
        except Exception:
            pass  # Table doesn't exist, that's fine

        # Copy table with proper configuration
        job_config = bigquery.CopyJobConfig()

        # Copy the table
        copy_job = client.copy_table(source_table, dest_table, job_config=job_config)
        copy_job.result()

        logger.info(f"Migrated table: {table_name} (preserved partitioning/clustering)")

    # Handle views
    if views:
        logger.warning(f"Found {len(views)} views that were NOT migrated:")
        for view_name in views:
            logger.warning(f"  - {view_name} (VIEW)")
        logger.warning("Views should be recreated using: python src/warehouse/create_views.py")


def main():
    """Main entry point for dataset migration CLI."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(description="Migrate a BigQuery dataset")
    parser.add_argument("--source-dataset", required=True, help="Source dataset name")
    parser.add_argument("--dest-dataset", required=True, help="Destination dataset name")
    parser.add_argument(
        "--project-id",
        default=os.getenv("GCP_PROJECT_ID"),
        help="Google Cloud project ID (defaults to GCP_PROJECT_ID env var)",
    )

    args = parser.parse_args()

    migrate_dataset(args.source_dataset, args.dest_dataset, args.project_id)


if __name__ == "__main__":
    main()
