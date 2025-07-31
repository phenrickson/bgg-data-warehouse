"""Script to migrate a BigQuery dataset."""

import argparse
import logging
from typing import List, Optional
from google.cloud import bigquery


def get_all_tables(client: bigquery.Client, dataset_ref: str) -> List[str]:
    """
    Retrieve all table names in a given dataset.

    Args:
        client (bigquery.Client): BigQuery client
        dataset_ref (str): Full dataset reference (project.dataset)

    Returns:
        List[str]: List of table names in the dataset
    """
    tables = client.list_tables(dataset_ref)
    return [table.table_id for table in tables]


def migrate_dataset(
    source_dataset: str,
    dest_dataset: str,
    project_id: Optional[str] = None,
) -> None:
    """
    Migrate an entire dataset, copying all tables.

    Args:
        source_dataset (str): Source dataset name
        dest_dataset (str): Destination dataset name
        project_id (str, optional): Google Cloud project ID
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create BigQuery client
    client = bigquery.Client(project=project_id)

    # Source and destination dataset references
    source_ref = f"{project_id}.{source_dataset}"
    dest_ref = f"{project_id}.{dest_dataset}"

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

    # Get all tables in source dataset
    tables = get_all_tables(client, source_ref)
    logger.info(f"Found {len(tables)} tables to migrate")

    # Copy each table
    for table_name in tables:
        source_table = f"{source_ref}.{table_name}"
        dest_table = f"{dest_ref}.{table_name}"

        # Copy entire table
        query = f"""
        CREATE TABLE `{dest_table}` AS
        SELECT * 
        FROM `{source_table}`
        """

        job = client.query(query)
        job.result()
        logger.info(f"Migrated table: {table_name}")


def main():
    import os
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(description="Migrate a BigQuery dataset")
    parser.add_argument("--source-dataset", required=True, help="Source dataset name")
    parser.add_argument("--dest-dataset", required=True, help="Destination dataset name")
    parser.add_argument(
        "--project-id",
        default=os.getenv("GCP_PROJECT_ID", "gcp-demos-411520"),
        help="Google Cloud project ID",
    )

    args = parser.parse_args()

    migrate_dataset(args.source_dataset, args.dest_dataset, args.project_id)


if __name__ == "__main__":
    main()
