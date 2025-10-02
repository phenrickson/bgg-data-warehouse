"""CLI script for migrating BigQuery datasets."""

import argparse
import os
from dotenv import load_dotenv

from src.warehouse.migrate_datasets import migrate_dataset


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
