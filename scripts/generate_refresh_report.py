"""Generate a report on refresh strategy test results."""

import argparse
import logging
from google.cloud import bigquery
import pandas as pd


def generate_refresh_report(dataset, output_file):
    """
    Generate a markdown report on refresh strategy test results.

    Args:
        dataset (str): BigQuery dataset to analyze
        output_file (str): Path to save the markdown report
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create BigQuery client
    client = bigquery.Client()

    # Queries for different aspects of the refresh test
    queries = {
        "refresh_distribution": f"""
        SELECT 
            year_published,
            COUNT(*) as total_games,
            COUNTIF(last_refresh_timestamp IS NOT NULL) as refreshed_games,
            COUNTIF(last_refresh_timestamp IS NOT NULL) / COUNT(*) * 100 as refresh_percentage
        FROM `{dataset}.raw_responses` r
        JOIN `{dataset}.games` g ON r.game_id = g.game_id
        GROUP BY year_published
        ORDER BY year_published DESC
        """,
        "refresh_intervals": f"""
        WITH refresh_intervals AS (
            SELECT 
                g.year_published,
                TIMESTAMP_DIFF(
                    r.last_refresh_timestamp, 
                    TIMESTAMP_SUB(r.last_refresh_timestamp, INTERVAL 1 DAY), 
                    DAY
                ) as refresh_interval
            FROM `{dataset}.raw_responses` r
            JOIN `{dataset}.games` g ON r.game_id = g.game_id
            WHERE r.last_refresh_timestamp IS NOT NULL
        )
        SELECT 
            year_published,
            AVG(refresh_interval) as avg_refresh_interval,
            MIN(refresh_interval) as min_refresh_interval,
            MAX(refresh_interval) as max_refresh_interval
        FROM refresh_intervals
        GROUP BY year_published
        ORDER BY year_published DESC
        """,
        "overdue_games": f"""
        SELECT 
            g.year_published,
            COUNT(*) as total_overdue_games,
            AVG(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_refresh_timestamp, HOUR)) as avg_hours_overdue
        FROM `{dataset}.raw_responses` r
        JOIN `{dataset}.games` g ON r.game_id = g.game_id
        WHERE 
            r.last_refresh_timestamp IS NOT NULL AND
            TIMESTAMP_ADD(r.last_refresh_timestamp, INTERVAL 7 DAY) < CURRENT_TIMESTAMP()
        GROUP BY year_published
        ORDER BY year_published DESC
        """,
    }

    # Execute queries and collect results
    results = {}
    for name, query in queries.items():
        df = client.query(query).to_dataframe()
        results[name] = df

    # Generate markdown report
    with open(output_file, "w") as f:
        f.write("# Refresh Strategy Test Report\n\n")

        # Refresh Distribution
        f.write("## Refresh Distribution by Year\n\n")
        f.write("| Year | Total Games | Refreshed Games | Refresh Percentage |\n")
        f.write("|------|-------------|----------------|-------------------|\n")
        for _, row in results["refresh_distribution"].iterrows():
            f.write(
                f"| {row['year_published']} | {row['total_games']} | {row['refreshed_games']} | {row['refresh_percentage']:.2f}% |\n"
            )

        # Refresh Intervals
        f.write("\n## Refresh Intervals by Year\n\n")
        f.write("| Year | Avg Refresh Interval | Min Interval | Max Interval |\n")
        f.write("|------|---------------------|--------------|-------------|\n")
        for _, row in results["refresh_intervals"].iterrows():
            f.write(
                f"| {row['year_published']} | {row['avg_refresh_interval']:.2f} | {row['min_refresh_interval']} | {row['max_refresh_interval']} |\n"
            )

        # Overdue Games
        f.write("\n## Overdue Games by Year\n\n")
        f.write("| Year | Total Overdue Games | Avg Hours Overdue |\n")
        f.write("|------|---------------------|-------------------|\n")
        for _, row in results["overdue_games"].iterrows():
            f.write(
                f"| {row['year_published']} | {row['total_overdue_games']} | {row['avg_hours_overdue']:.2f} |\n"
            )

        # Additional insights
        f.write("\n## Additional Insights\n\n")
        f.write("### Observations\n")
        f.write("- This report provides an overview of the refresh strategy performance\n")
        f.write(
            "- Refresh distribution shows how games are being updated across different publication years\n"
        )
        f.write(
            "- Refresh intervals indicate the frequency of updates for different game age groups\n"
        )
        f.write(
            "- Overdue games highlight potential areas for optimization in the refresh strategy\n"
        )

    logger.info(f"Refresh report generated: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate refresh strategy test report")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset to analyze")
    parser.add_argument(
        "--output-file", default="refresh_test_report.md", help="Path to save the markdown report"
    )

    args = parser.parse_args()

    generate_refresh_report(args.dataset, args.output_file)


if __name__ == "__main__":
    main()
