"""Create BigQuery views for monitoring refresh operations."""

from src.config import get_bigquery_client


def create_refresh_monitoring_views():
    """Create or update BigQuery views for monitoring refresh operations."""
    client = get_bigquery_client()

    # View for monitoring refresh operations
    refresh_operations_view = """
    CREATE OR REPLACE VIEW `monitoring.refresh_operations` AS
    WITH daily_stats AS (
        SELECT
            DATE(last_refresh_timestamp) as refresh_date,
            COUNT(*) as games_refreshed,
            AVG(refresh_count) as avg_refresh_count,
            AVG(TIMESTAMP_DIFF(last_refresh_timestamp, 
                LAG(last_refresh_timestamp) OVER(PARTITION BY game_id ORDER BY last_refresh_timestamp),
                HOUR)) as avg_refresh_interval_hours
        FROM raw.raw_responses
        WHERE last_refresh_timestamp IS NOT NULL
        GROUP BY refresh_date
    )
    SELECT
        refresh_date,
        games_refreshed,
        ROUND(avg_refresh_count, 2) as avg_refresh_count,
        ROUND(avg_refresh_interval_hours, 2) as avg_refresh_interval_hours,
        ROUND(games_refreshed / 100.0, 2) as batches_processed
    FROM daily_stats
    ORDER BY refresh_date DESC
    """

    # View for monitoring refresh queue
    refresh_queue_view = """
    CREATE OR REPLACE VIEW `monitoring.refresh_queue` AS
    WITH game_stats AS (
        SELECT
            g.year_published,
            COUNT(*) as total_games,
            COUNT(CASE WHEN r.next_refresh_due < CURRENT_TIMESTAMP() THEN 1 END) as games_due_for_refresh,
            AVG(TIMESTAMP_DIFF(r.next_refresh_due, r.last_refresh_timestamp, HOUR)) as avg_refresh_interval_hours,
            AVG(CASE 
                WHEN r.next_refresh_due < CURRENT_TIMESTAMP() 
                THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.next_refresh_due, HOUR)
                ELSE 0 
            END) as avg_hours_overdue
        FROM bgg_data.games g
        JOIN raw.raw_responses r ON g.game_id = r.game_id
        GROUP BY g.year_published
    )
    SELECT
        year_published,
        total_games,
        games_due_for_refresh,
        ROUND(avg_refresh_interval_hours / 24.0, 1) as avg_refresh_interval_days,
        ROUND(avg_hours_overdue, 1) as avg_hours_overdue,
        ROUND(games_due_for_refresh / 100.0, 1) as estimated_batches_needed
    FROM game_stats
    ORDER BY year_published DESC
    """

    # Execute view creation
    client.query(refresh_operations_view).result()
    client.query(refresh_queue_view).result()

    print("Successfully created refresh monitoring views")


if __name__ == "__main__":
    create_refresh_monitoring_views()
