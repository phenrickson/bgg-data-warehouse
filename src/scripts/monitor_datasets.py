"""Script to monitor basic dataset information."""
import logging
from datetime import datetime

from google.cloud import bigquery

from src.config import get_bigquery_config
from src.utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

def get_table_info(client: bigquery.Client, table_id: str) -> None:
    """Print basic information about a table.
    
    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
    """
    table = client.get_table(table_id)
    
    # Get row count
    query = f"""
    SELECT COUNT(*) as row_count
    FROM `{table_id}`
    """
    result = client.query(query).result()
    row = next(iter(result))
    
    # Try to get last update time if timestamp field exists
    last_updated = None
    if any(f.name in ['fetch_timestamp', 'load_timestamp'] for f in table.schema):
        timestamp_field = "fetch_timestamp" if "raw_responses" in table_id else "load_timestamp"
        query = f"""
        SELECT MAX({timestamp_field}) as last_updated
        FROM `{table_id}`
        """
        result = client.query(query).result()
        last_updated = next(iter(result)).last_updated
    
    print(f"\nTable: {table.table_id}")
    print(f"Columns: {len(table.schema)}")
    print(f"Rows: {row.row_count:,}")
    if last_updated:
        print(f"Last updated: {last_updated}")
    print("\nColumns:")
    for field in table.schema:
        print(f"  {field.name}: {field.field_type}")

def main() -> None:
    """Show basic dataset information."""
    try:
        # Get config
        config = get_bigquery_config()
        client = bigquery.Client()
        
        project_id = config['project']['id']
        
        # Raw dataset
        print("\nRaw Dataset (bgg_raw_dev):")
        print("=" * 50)
        get_table_info(client, f"{project_id}.bgg_raw_dev.raw_responses")
        
        # Transformed dataset
        print("\nTransformed Dataset (bgg_data_dev):")
        print("=" * 50)
        dataset_ref = client.dataset("bgg_data_dev", project=project_id)
        tables = client.list_tables(dataset_ref)
        for table in tables:
            get_table_info(client, f"{project_id}.bgg_data_dev.{table.table_id}")
        
    except Exception as e:
        logger.error(f"Error getting dataset info: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
