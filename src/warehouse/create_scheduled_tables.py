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


def get_filter_publishers_query(project_id, dataset):
    """Get the SQL query for filter_publishers table."""
    return f"""
    SELECT
        p.publisher_id,
        p.name,
        COUNT(DISTINCT gp.game_id) as game_count
    FROM `{project_id}.{dataset}.publishers` p
    JOIN `{project_id}.{dataset}.game_publishers` gp
        ON p.publisher_id = gp.publisher_id
    -- Only include publishers with games in the active games table
    JOIN `{project_id}.{dataset}.games_active_table` g
        ON gp.game_id = g.game_id
    WHERE g.bayes_average IS NOT NULL
        AND g.bayes_average > 0
    GROUP BY p.publisher_id, p.name
    ORDER BY game_count DESC, p.name ASC
    LIMIT 500
    """


def create_filter_publishers_table(environment=None):
    """Create a table for filter_publishers populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        logger.info(
            f"Creating filter_publishers table in project: {project_id}, dataset: {dataset}"
        )

        # Execute the query to create the table
        query = get_filter_publishers_query(project_id, dataset)
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {project_id}.{dataset}.filter_publishers")

        # Set up a scheduled query to refresh the table
        setup_filter_publishers_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating filter_publishers table: {e}")
        raise


def setup_filter_publishers_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the filter_publishers table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for filter_publishers table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh filter_publishers table",
            data_source_id="scheduled_query",
            params={
                "query": get_filter_publishers_query(project_id, dataset),
                "destination_table_name_template": "filter_publishers",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:30",  # 8:30 AM CST (14:30 UTC)
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
        logger.error(f"Error setting up scheduled query for filter_publishers table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        return False


def get_filter_categories_query(project_id, dataset):
    """Get the SQL query for filter_categories table."""
    return f"""
    SELECT
        c.category_id,
        c.name,
        COUNT(DISTINCT gc.game_id) as game_count
    FROM `{project_id}.{dataset}.categories` c
    JOIN `{project_id}.{dataset}.game_categories` gc
        ON c.category_id = gc.category_id
    JOIN `{project_id}.{dataset}.games_active_table` g
        ON gc.game_id = g.game_id
    WHERE g.bayes_average IS NOT NULL
        AND g.bayes_average > 0
    GROUP BY c.category_id, c.name
    ORDER BY game_count DESC, c.name ASC
    LIMIT 500
    """


def create_filter_categories_table(environment=None):
    """Create a table for filter_categories populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        logger.info(
            f"Creating filter_categories table in project: {project_id}, dataset: {dataset}"
        )

        # Execute the query to create the table
        query = get_filter_categories_query(project_id, dataset)
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {project_id}.{dataset}.filter_categories")

        # Set up a scheduled query to refresh the table
        setup_filter_categories_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating filter_categories table: {e}")
        raise


def setup_filter_categories_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the filter_categories table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for filter_categories table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh filter_categories table",
            data_source_id="scheduled_query",
            params={
                "query": get_filter_categories_query(project_id, dataset),
                "destination_table_name_template": "filter_categories",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:35",  # 8:35 AM CST (14:35 UTC)
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
        logger.error(f"Error setting up scheduled query for filter_categories table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        return False


def get_filter_mechanics_query(project_id, dataset):
    """Get the SQL query for filter_mechanics table."""
    return f"""
    SELECT
        m.mechanic_id,
        m.name,
        COUNT(DISTINCT gm.game_id) as game_count
    FROM `{project_id}.{dataset}.mechanics` m
    JOIN `{project_id}.{dataset}.game_mechanics` gm
        ON m.mechanic_id = gm.mechanic_id
    JOIN `{project_id}.{dataset}.games_active_table` g
        ON gm.game_id = g.game_id
    WHERE g.bayes_average IS NOT NULL
        AND g.bayes_average > 0
    GROUP BY m.mechanic_id, m.name
    ORDER BY game_count DESC, m.name ASC
    LIMIT 500
    """


def get_filter_designers_query(project_id, dataset):
    """Get the SQL query for filter_designers table."""
    return f"""
    SELECT
        d.designer_id,
        d.name,
        COUNT(DISTINCT gd.game_id) as game_count
    FROM `{project_id}.{dataset}.designers` d
    JOIN `{project_id}.{dataset}.game_designers` gd
        ON d.designer_id = gd.designer_id
    JOIN `{project_id}.{dataset}.games_active_table` g
        ON gd.game_id = g.game_id
    WHERE g.bayes_average IS NOT NULL
        AND g.bayes_average > 0
    GROUP BY d.designer_id, d.name
    ORDER BY game_count DESC, d.name ASC
    LIMIT 1000
    """


def get_filter_options_combined_query(project_id, dataset):
    """Get the SQL query for filter_options_combined table."""
    return f"""
    SELECT
        'publisher' as entity_type,
        publisher_id as entity_id,
        name,
        game_count
    FROM `{project_id}.{dataset}.filter_publishers`
    UNION ALL
    SELECT
        'category' as entity_type,
        category_id as entity_id,
        name,
        game_count
    FROM `{project_id}.{dataset}.filter_categories`
    UNION ALL
    SELECT
        'mechanic' as entity_type,
        mechanic_id as entity_id,
        name,
        game_count
    FROM `{project_id}.{dataset}.filter_mechanics`
    UNION ALL
    SELECT
        'designer' as entity_type,
        designer_id as entity_id,
        name,
        game_count
    FROM `{project_id}.{dataset}.filter_designers`
    """


def create_filter_mechanics_table(environment=None):
    """Create a table for filter_mechanics populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        logger.info(f"Creating filter_mechanics table in project: {project_id}, dataset: {dataset}")

        # Execute the query to create the table
        query = get_filter_mechanics_query(project_id, dataset)
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {project_id}.{dataset}.filter_mechanics")

        # Set up a scheduled query to refresh the table
        setup_filter_mechanics_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating filter_mechanics table: {e}")
        raise


def setup_filter_mechanics_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the filter_mechanics table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for filter_mechanics table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh filter_mechanics table",
            data_source_id="scheduled_query",
            params={
                "query": get_filter_mechanics_query(project_id, dataset),
                "destination_table_name_template": "filter_mechanics",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:40",  # 8:40 AM CST (14:40 UTC)
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
        logger.error(f"Error setting up scheduled query for filter_mechanics table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        return False


def create_filter_designers_table(environment=None):
    """Create a table for filter_designers populated by a scheduled query."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        logger.info(f"Creating filter_designers table in project: {project_id}, dataset: {dataset}")

        # Execute the query to create the table
        query = get_filter_designers_query(project_id, dataset)
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {project_id}.{dataset}.filter_designers")

        # Set up a scheduled query to refresh the table
        setup_filter_designers_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating filter_designers table: {e}")
        raise


def setup_filter_designers_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the filter_designers table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for filter_designers table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh filter_designers table",
            data_source_id="scheduled_query",
            params={
                "query": get_filter_designers_query(project_id, dataset),
                "destination_table_name_template": "filter_designers",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:45",  # 8:45 AM CST (14:45 UTC)
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
        logger.error(f"Error setting up scheduled query for filter_designers table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        return False


def create_filter_options_combined_table(environment=None):
    """Create a combined table that unions all the individual filter tables."""
    try:
        config = get_bigquery_config(environment)
        client = bigquery.Client()

        project_id = config["project"]["id"]
        dataset = config["project"]["dataset"]
        location = config["project"]["location"]

        logger.info(
            f"Creating filter_options_combined table in project: {project_id}, dataset: {dataset}"
        )

        # Execute the query to create the table
        query = get_filter_options_combined_query(project_id, dataset)
        query_job = client.query(query)
        query_job.result()

        logger.info(f"Successfully created table: {project_id}.{dataset}.filter_options_combined")

        # Set up a scheduled query to refresh the table
        setup_filter_options_combined_scheduled_query(project_id, dataset, location)

        return True

    except Exception as e:
        logger.error(f"Error creating filter_options_combined table: {e}")
        raise


def setup_filter_options_combined_scheduled_query(project_id, dataset, location):
    """Set up a scheduled query to refresh the filter_options_combined table using BigQuery Data Transfer Service."""
    try:
        logger.info("Setting up scheduled query for filter_options_combined table...")

        # Create a Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Get the parent resource path
        parent = transfer_client.common_location_path(project_id, location)

        # Create the transfer config
        transfer_config = bigquery_datatransfer.TransferConfig(
            display_name="Refresh filter_options_combined table",
            data_source_id="scheduled_query",
            params={
                "query": get_filter_options_combined_query(project_id, dataset),
                "destination_table_name_template": "filter_options_combined",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every day 14:50",  # 8:50 AM CST (14:50 UTC) - runs after all individual tables
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
        logger.error(f"Error setting up scheduled query for filter_options_combined table: {e}")
        logger.error("You may need to enable the BigQuery Data Transfer Service API.")
        return False


if __name__ == "__main__":
    # Create games_active_table
    create_games_active_table()

    # Create best_player_counts_table
    create_best_player_counts_table()

    # Create individual filter tables
    create_filter_publishers_table()
    create_filter_categories_table()
    create_filter_mechanics_table()
    create_filter_designers_table()

    # Create combined filter table (depends on individual tables)
    create_filter_options_combined_table()
