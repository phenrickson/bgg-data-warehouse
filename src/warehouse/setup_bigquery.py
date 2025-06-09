"""BigQuery setup module for creating datasets and tables."""

from google.cloud import bigquery
from google.api_core import exceptions

from ..config import get_bigquery_config

def create_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Create a BigQuery dataset if it doesn't exist.
    
    Args:
        client: BigQuery client
        dataset_id: ID of the dataset to create
    """
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    try:
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    except exceptions.Conflict:
        print(f"Dataset {dataset_id} already exists")

def create_raw_tables(client: bigquery.Client, dataset_id: str) -> None:
    """Create tables in the raw dataset.
    
    Args:
        client: BigQuery client
        dataset_id: ID of the dataset to create tables in
    """
    tables = {
        "games": [
            bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("year_published", "INTEGER"),
            bigquery.SchemaField("min_players", "INTEGER"),
            bigquery.SchemaField("max_players", "INTEGER"),
            bigquery.SchemaField("playing_time", "INTEGER"),
            bigquery.SchemaField("min_age", "INTEGER"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("thumbnail", "STRING"),
            bigquery.SchemaField("image", "STRING"),
            bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
            bigquery.SchemaField("mechanics", "STRING", mode="REPEATED"),
            bigquery.SchemaField("families", "STRING", mode="REPEATED"),
            bigquery.SchemaField("raw_data", "STRING"),  # Store original XML
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ],
        "request_log": [
            bigquery.SchemaField("request_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("game_id", "INTEGER"),
            bigquery.SchemaField("request_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("response_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("status_code", "INTEGER"),
            bigquery.SchemaField("success", "BOOLEAN"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("retry_count", "INTEGER"),
        ],
        "thing_ids": [
            bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("processed", "BOOLEAN"),
            bigquery.SchemaField("process_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ],
    }

    for table_id, schema in tables.items():
        table = bigquery.Table(f"{client.project}.{dataset_id}.{table_id}", schema=schema)
        
        if table_id == "request_log":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="request_timestamp",
            )
        elif table_id == "games":
            table.clustering_fields = ["game_id"]

        try:
            client.create_table(table)
            print(f"Created table {table_id}")
        except exceptions.Conflict:
            print(f"Table {table_id} already exists")

def main() -> None:
    """Create BigQuery datasets and tables."""
    config = get_bigquery_config()
    client = bigquery.Client(project=config["project"]["id"])

    # Create datasets
    for dataset_id in config["datasets"].values():
        create_dataset(client, dataset_id)

    # Create raw tables
    create_raw_tables(client, config["datasets"]["raw"])

    print("BigQuery setup complete")

if __name__ == "__main__":
    main()
