"""Script to create BigQuery tables populated by scheduled queries."""

import logging
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.api_core.exceptions import NotFound, Conflict
from src.config import get_bigquery_config

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_games_active_table(environment=None):
    """Create a table for games_active populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        table_id = f"{project_id}.{dataset}.games_active_table"

        logger.info(f"Creating games_active table in project: {project_id}, dataset: {dataset}")

        # SQL query to create the table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        WITH game_latest_timestamps AS (
          SELECT 
            game_id,
            MAX(load_timestamp) AS latest_game_timestamp
          FROM `{project_id}.{dataset}.games`
          GROUP BY game_id
        ),
        latest_game_data AS (
          SELECT g.*
          FROM `{project_id}.{dataset}.games` g
          JOIN game_latest_timestamps lt 
            ON g.game_id = lt.game_id 
            AND g.load_timestamp = lt.latest_game_timestamp
        )
        SELECT DISTINCT
            game_id,
            type,
            primary_name AS name,
            year_published,
            average_rating,
            average_weight,
            bayes_average,
            users_rated,
            owned_count,
            trading_count,
            wanting_count,
            wishing_count,
            num_comments,
            num_weights,
            min_players,
            max_players,
            playing_time,
            min_playtime,
            max_playtime,
            min_age,
            description,
            thumbnail,
            image,
            load_timestamp,
            CURRENT_TIMESTAMP() AS last_updated
        FROM latest_game_data
        """

        # Execute the query to create the table
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {table_id}")

        # Set up a scheduled query to refresh the table
        setup_games_active_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating games_active table: {e}")
        raise


def setup_games_active_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the games_active_table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for games_active_table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # SQL query to refresh the table
        query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset}.games_active_table` AS
        WITH game_latest_timestamps AS (
          SELECT 
            game_id,
            MAX(load_timestamp) AS latest_game_timestamp
          FROM `{project_id}.{dataset}.games`
          GROUP BY game_id
        ),
        latest_game_data AS (
          SELECT g.*
          FROM `{project_id}.{dataset}.games` g
          JOIN game_latest_timestamps lt 
            ON g.game_id = lt.game_id 
            AND g.load_timestamp = lt.latest_game_timestamp
        )
        SELECT DISTINCT
            game_id,
            type,
            primary_name AS name,
            year_published,
            average_rating,
            average_weight,
            bayes_average,
            users_rated,
            owned_count,
            trading_count,
            wanting_count,
            wishing_count,
            num_comments,
            num_weights,
            min_players,
            max_players,
            playing_time,
            min_playtime,
            max_playtime,
            min_age,
            description,
            thumbnail,
            image,
            load_timestamp,
            CURRENT_TIMESTAMP() AS last_updated
        FROM latest_game_data
        """

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh games_active_table",
            data_source_id="scheduled_query",
            params={
                "query": query,
                "destination_table_name_template": "games_active_table",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:00",  # 8:00 AM CST (14:00 UTC)
            destination_dataset_id=dataset,
        )

        # Create the scheduled query
        transfer_config = transfer_client.create_transfer_config(
            parent=parent, transfer_config=transfer_config
        )

        logger.info(f"Successfully created scheduled query: {transfer_config.name}")
        logger.info(f"Schedule: {transfer_config.schedule}")
        logger.info(f"Next run time: {transfer_config.next_run_time}")

        return True

    except Exception as e:
        logger.error(f"Error setting up scheduled query for games_active_table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        logger.error(
            "Visit: https://console.cloud.google.com/apis/library/bigquerydatatransfer.googleapis.com"
        )
        logger.error("Then try running this script again.")
        return False


def create_best_player_counts_table(environment=None):
    """Create a table for best_player_counts populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        table_id = f"{project_id}.{dataset}.best_player_counts_table"

        logger.info(
            f"Creating best_player_counts table in project: {project_id}, dataset: {dataset}"
        )

        # SQL query to create the table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        WITH normalized_player_counts AS (
          SELECT 
            game_id,
            -- Only include exact player counts 1-8
            player_count,
            -- For sorting and range comparisons, convert to numeric value
            SAFE_CAST(player_count AS INT64) AS player_count_int,
            best_votes,
            recommended_votes,
            not_recommended_votes
          FROM `{project_id}.{dataset}.player_counts`
        ),
        player_count_thresholds AS (
          SELECT 
            game_id,
            player_count,
            player_count_int,
            best_votes,
            recommended_votes,
            not_recommended_votes,
            best_votes + recommended_votes + not_recommended_votes AS total_votes,
            CASE 
                WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
                THEN 0 
                ELSE ROUND(best_votes / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
            END as best_percentage,
            CASE 
                WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
                THEN 0 
                ELSE ROUND((best_votes + recommended_votes) / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
            END as positive_percentage
          FROM normalized_player_counts
          WHERE (best_votes + recommended_votes + not_recommended_votes) > 5  -- Minimum number of votes to consider reliable
            AND player_count IN ('1', '2', '3', '4', '5', '6', '7', '8')  -- Only include exact player counts 1-8
        ),
        ranked_player_counts AS (
          SELECT
            game_id,
            player_count,
            player_count_int,
            best_percentage,
            positive_percentage,
            total_votes,
            -- Rank player counts by best percentage within each game
            ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY best_percentage DESC, total_votes DESC) as best_rank,
            -- Rank player counts by positive percentage within each game
            ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY positive_percentage DESC, total_votes DESC) as recommended_rank
          FROM player_count_thresholds
          WHERE best_percentage >= 40 OR positive_percentage >= 70  -- Thresholds for consideration
        )
        SELECT
          g.game_id,
          g.name,
          g.min_players,
          g.max_players,
          -- Best player counts (top 3)
          STRING_AGG(
            CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count END, 
            ', ' 
            ORDER BY best_rank
          ) AS best_player_counts,
          -- Recommended player counts (top 5)
          STRING_AGG(
            CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count END, 
            ', ' 
            ORDER BY recommended_rank
          ) AS recommended_player_counts,
          -- Min/Max best player count
          MIN(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count_int END) AS min_best_player_count,
          MAX(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count_int END) AS max_best_player_count,
          -- Min/Max recommended player count
          MIN(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count_int END) AS min_recommended_player_count,
          MAX(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count_int END) AS max_recommended_player_count,
          -- Boolean flags for efficient filtering
          CASE WHEN COUNT(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN 1 END) > 0 THEN TRUE ELSE FALSE END AS has_best_count,
          CASE WHEN COUNT(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN 1 END) > 0 THEN TRUE ELSE FALSE END AS has_recommended_count,
          -- Add timestamp for when this data was generated
          CURRENT_TIMESTAMP() AS last_updated
        FROM `{project_id}.{dataset}.games_active_table` g
        LEFT JOIN ranked_player_counts rpc ON g.game_id = rpc.game_id
        GROUP BY g.game_id, g.name, g.min_players, g.max_players
        """

        # Execute the query to create the table
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {table_id}")

        # Set up a scheduled query to refresh the table
        setup_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating best_player_counts table: {e}")
        raise


def setup_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the best_player_counts_table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query using BigQuery Data Transfer Service...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # SQL query to refresh the table
        query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset}.best_player_counts_table` AS
        WITH normalized_player_counts AS (
          SELECT 
            game_id,
            -- Only include exact player counts 1-8
            player_count,
            -- For sorting and range comparisons, convert to numeric value
            SAFE_CAST(player_count AS INT64) AS player_count_int,
            best_votes,
            recommended_votes,
            not_recommended_votes
          FROM `{project_id}.{dataset}.player_counts`
        ),
        player_count_thresholds AS (
          SELECT 
            game_id,
            player_count,
            player_count_int,
            best_votes,
            recommended_votes,
            not_recommended_votes,
            best_votes + recommended_votes + not_recommended_votes AS total_votes,
            CASE 
                WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
                THEN 0 
                ELSE ROUND(best_votes / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
            END as best_percentage,
            CASE 
                WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
                THEN 0 
                ELSE ROUND((best_votes + recommended_votes) / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
            END as positive_percentage
          FROM normalized_player_counts
          WHERE (best_votes + recommended_votes + not_recommended_votes) > 5  -- Minimum number of votes to consider reliable
            AND player_count IN ('1', '2', '3', '4', '5', '6', '7', '8')  -- Only include exact player counts 1-8
        ),
        ranked_player_counts AS (
          SELECT
            game_id,
            player_count,
            player_count_int,
            best_percentage,
            positive_percentage,
            total_votes,
            -- Rank player counts by best percentage within each game
            ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY best_percentage DESC, total_votes DESC) as best_rank,
            -- Rank player counts by positive percentage within each game
            ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY positive_percentage DESC, total_votes DESC) as recommended_rank
          FROM player_count_thresholds
          WHERE best_percentage >= 40 OR positive_percentage >= 70  -- Thresholds for consideration
        )
        SELECT
          g.game_id,
          g.name,
          g.min_players,
          g.max_players,
          -- Best player counts (top 3)
          STRING_AGG(
            CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count END, 
            ', ' 
            ORDER BY best_rank
          ) AS best_player_counts,
          -- Recommended player counts (top 5)
          STRING_AGG(
            CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count END, 
            ', ' 
            ORDER BY recommended_rank
          ) AS recommended_player_counts,
          -- Min/Max best player count
          MIN(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count_int END) AS min_best_player_count,
          MAX(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN player_count_int END) AS max_best_player_count,
          -- Min/Max recommended player count
          MIN(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count_int END) AS min_recommended_player_count,
          MAX(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN player_count_int END) AS max_recommended_player_count,
          -- Boolean flags for efficient filtering
          CASE WHEN COUNT(CASE WHEN best_rank <= 3 AND best_percentage >= 40 THEN 1 END) > 0 THEN TRUE ELSE FALSE END AS has_best_count,
          CASE WHEN COUNT(CASE WHEN recommended_rank <= 5 AND positive_percentage >= 70 THEN 1 END) > 0 THEN TRUE ELSE FALSE END AS has_recommended_count,
          -- Add timestamp for when this data was generated
          CURRENT_TIMESTAMP() AS last_updated
        FROM `{project_id}.{dataset}.games_active_table` g
        LEFT JOIN ranked_player_counts rpc ON g.game_id = rpc.game_id
        GROUP BY g.game_id, g.name, g.min_players, g.max_players
        """

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh best_player_counts_table",
            data_source_id="scheduled_query",
            params={
                "query": query,
                "destination_table_name_template": "best_player_counts_table",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:00",  # 8:00 AM CST (14:00 UTC)
            destination_dataset_id=dataset,
        )

        # Create the scheduled query
        transfer_config = transfer_client.create_transfer_config(
            parent=parent, transfer_config=transfer_config
        )

        logger.info(f"Successfully created scheduled query: {transfer_config.name}")
        logger.info(f"Schedule: {transfer_config.schedule}")
        logger.info(f"Next run time: {transfer_config.next_run_time}")

        return True

    except Exception as e:
        logger.error(f"Error setting up scheduled query: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        logger.error(
            "Visit: https://console.cloud.google.com/apis/library/bigquerydatatransfer.googleapis.com"
        )
        logger.error("Then try running this script again.")
        return False


if __name__ == "__main__":
    # Create games_active_table
    create_games_active_table()

    # Create best_player_counts_table
    create_best_player_counts_table()
