"""Script to set up monitoring tables in BigQuery."""
import logging

from google.cloud import bigquery

from src.config import get_bigquery_config
from src.utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

def create_monitoring_tables(client: bigquery.Client, config: dict) -> None:
    """Create monitoring tables in BigQuery.
    
    Args:
        client: BigQuery client
        config: Configuration dictionary
    """
    project_id = config["project"]["id"]
    dataset_id = config["datasets"]["monitoring"]
    
    # Create monitoring dataset if it doesn't exist
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    
    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Created/confirmed dataset: {dataset_ref}")
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        raise

    # Create quality check results table
    quality_check_schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("check_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("records_checked", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("failed_records", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("details", "STRING", mode="REQUIRED")
    ]
    
    quality_check_table = bigquery.Table(
        f"{dataset_ref}.quality_check_results",
        schema=quality_check_schema
    )
    quality_check_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )
    
    try:
        client.create_table(quality_check_table, exists_ok=True)
        logger.info("Created/confirmed quality_check_results table")
    except Exception as e:
        logger.error(f"Error creating quality_check_results table: {e}")
        raise

    # Create API requests monitoring table
    api_requests_schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("endpoint", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_code", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("response_time", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("retry_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE")
    ]
    
    api_requests_table = bigquery.Table(
        f"{dataset_ref}.api_requests",
        schema=api_requests_schema
    )
    api_requests_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )
    
    try:
        client.create_table(api_requests_table, exists_ok=True)
        logger.info("Created/confirmed api_requests table")
    except Exception as e:
        logger.error(f"Error creating api_requests table: {e}")
        raise

def main() -> None:
    """Set up monitoring tables in BigQuery."""
    try:
        config = get_bigquery_config()
        client = bigquery.Client()
        
        create_monitoring_tables(client, config)
        logger.info("Successfully set up monitoring tables")
        
    except Exception as e:
        logger.error(f"Error setting up monitoring tables: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
