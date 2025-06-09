"""Module for loading processed data into BigQuery."""

import logging
from typing import List, Optional

import polars as pl
from google.cloud import bigquery
from google.cloud import storage

from ..config import get_bigquery_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BigQueryLoader:
    """Handles loading data into BigQuery."""

    def __init__(self) -> None:
        """Initialize the loader with configuration."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client(project=self.config["project"]["id"])
        self.storage_client = storage.Client(project=self.config["project"]["id"])
        self.bucket = self.storage_client.bucket(self.config["storage"]["bucket"])

    def _upload_to_gcs(
        self, 
        df: pl.DataFrame, 
        table_name: str
    ) -> Optional[str]:
        """Upload DataFrame to Google Cloud Storage.
        
        Args:
            df: DataFrame to upload
            table_name: Name of the target table
            
        Returns:
            GCS URI of the uploaded file or None if upload fails
        """
        try:
            # Convert to parquet for efficient loading
            temp_path = f"{self.config['storage']['temp_prefix']}{table_name}.parquet"
            
            # Convert polars DataFrame to pandas and save as parquet
            pandas_df = df.to_pandas()
            blob = self.bucket.blob(temp_path)
            
            # Upload to GCS
            with blob.open("wb") as f:
                pandas_df.to_parquet(f)
            
            gcs_uri = f"gs://{self.config['storage']['bucket']}/{temp_path}"
            logger.info("Uploaded data to %s", gcs_uri)
            return gcs_uri

        except Exception as e:
            logger.error("Failed to upload to GCS: %s", e)
            return None

    def _load_from_gcs(
        self, 
        gcs_uri: str, 
        table_ref: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> bool:
        """Load data from GCS to BigQuery.
        
        Args:
            gcs_uri: URI of the file in GCS
            table_ref: Full reference to the BigQuery table
            write_disposition: How to handle existing data
            
        Returns:
            True if load succeeds, False otherwise
        """
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.PARQUET,
            )

            load_job = self.client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config,
            )
            load_job.result()  # Wait for the job to complete

            logger.info("Loaded data into %s", table_ref)
            return True

        except Exception as e:
            logger.error("Failed to load to BigQuery: %s", e)
            return False

    def _cleanup_gcs(self, table_name: str) -> None:
        """Clean up temporary files from GCS.
        
        Args:
            table_name: Name of the table whose files to clean up
        """
        try:
            temp_path = f"{self.config['storage']['temp_prefix']}{table_name}.parquet"
            blob = self.bucket.blob(temp_path)
            blob.delete()
            logger.info("Cleaned up temporary file %s", temp_path)
        except Exception as e:
            logger.error("Failed to cleanup GCS: %s", e)

    def load_table(
        self, 
        df: pl.DataFrame, 
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> bool:
        """Load a DataFrame into a BigQuery table.
        
        Args:
            df: DataFrame to load
            dataset_id: ID of the target dataset
            table_id: ID of the target table
            write_disposition: How to handle existing data
            
        Returns:
            True if load succeeds, False otherwise
        """
        try:
            # Upload to GCS first
            gcs_uri = self._upload_to_gcs(df, table_id)
            if not gcs_uri:
                return False

            # Load from GCS to BigQuery
            table_ref = f"{self.config['project']['id']}.{dataset_id}.{table_id}"
            success = self._load_from_gcs(gcs_uri, table_ref, write_disposition)

            # Cleanup
            self._cleanup_gcs(table_id)

            return success

        except Exception as e:
            logger.error("Failed to load table %s: %s", table_id, e)
            return False

    def archive_raw_data(self, table_name: str) -> None:
        """Archive raw data to GCS.
        
        Args:
            table_name: Name of the table to archive
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.config['project']['id']}.{self.config['datasets']['raw']}.{table_name}`
            WHERE DATE(load_timestamp) = CURRENT_DATE()
            """
            
            df = self.client.query(query).to_dataframe()
            if df.empty:
                logger.info("No data to archive for %s", table_name)
                return

            # Convert to polars
            pl_df = pl.from_pandas(df)
            
            # Archive path includes date
            archive_path = (
                f"{self.config['storage']['archive_prefix']}"
                f"{table_name}/{table_name}_{pl_df['load_timestamp'][0].date()}.parquet"
            )
            
            blob = self.bucket.blob(archive_path)
            with blob.open("wb") as f:
                df.to_parquet(f)
            
            logger.info("Archived %s data to %s", table_name, archive_path)

        except Exception as e:
            logger.error("Failed to archive %s: %s", table_name, e)

def main() -> None:
    """Main function to load data into BigQuery."""
    loader = BigQueryLoader()
    
    # Archive raw data
    for table in ["games", "request_log", "thing_ids"]:
        loader.archive_raw_data(table)

if __name__ == "__main__":
    main()
