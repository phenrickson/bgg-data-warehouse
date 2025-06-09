"""Streamlit dashboard for visualizing BGG data."""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

from ..config import get_bigquery_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BGGDashboard:
    """Dashboard for visualizing BGG data."""

    def __init__(self) -> None:
        """Initialize the dashboard."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client(project=self.config["project"]["id"])

    def get_top_games(self, limit: int = 10) -> pd.DataFrame:
        """Get top rated games.
        
        Args:
            limit: Number of games to return
            
        Returns:
            DataFrame of top games
            
        Raises:
            ValueError: If limit is less than 1
        """
        if limit < 1:
            raise ValueError("Limit must be at least 1")
        query = f"""
        SELECT
            name,
            year_published,
            average as rating,
            num_ratings,
            weight
        FROM `{self.config["project"]["id"]}.{self.config["datasets"]["raw"]}.games`
        WHERE num_ratings >= 100
        ORDER BY rating DESC
        LIMIT {limit}
        """
        
        return self.client.query(query).to_dataframe()

    def get_games_by_year(self) -> pd.DataFrame:
        """Get game counts and ratings by year.
        
        Returns:
            DataFrame of game statistics by year
        """
        query = f"""
        SELECT
            year_published,
            COUNT(*) as game_count,
            AVG(average) as avg_rating,
            AVG(weight) as avg_weight
        FROM `{self.config["project"]["id"]}.{self.config["datasets"]["raw"]}.games`
        WHERE year_published IS NOT NULL
        GROUP BY year_published
        ORDER BY year_published
        """
        
        return self.client.query(query).to_dataframe()

    def get_popular_mechanics(self, limit: int = 10) -> pd.DataFrame:
        """Get most popular game mechanics.
        
        Args:
            limit: Number of mechanics to return
            
        Returns:
            DataFrame of popular mechanics
            
        Raises:
            ValueError: If limit is less than 1
        """
        if limit < 1:
            raise ValueError("Limit must be at least 1")
        query = f"""
        WITH mechanic_counts AS (
            SELECT
                mechanic,
                COUNT(*) as game_count,
                AVG(average) as avg_rating
            FROM `{self.config["project"]["id"]}.{self.config["datasets"]["raw"]}.games`
            CROSS JOIN UNNEST(mechanics) as mechanic
            GROUP BY mechanic
            HAVING game_count >= 10
        )
        SELECT *
        FROM mechanic_counts
        ORDER BY game_count DESC
        LIMIT {limit}
        """
        
        return self.client.query(query).to_dataframe()

    def get_data_quality_metrics(self) -> pd.DataFrame:
        """Get recent data quality metrics.
        
        Returns:
            DataFrame of quality metrics
        """
        query = f"""
        SELECT
            check_name,
            table_name,
            check_status,
            records_checked,
            failed_records,
            check_timestamp
        FROM `{self.config["project"]["id"]}.{self.config["datasets"]["monitoring"]}.data_quality`
        WHERE check_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY check_timestamp DESC
        """
        
        return self.client.query(query).to_dataframe()

def main() -> None:
    """Run the Streamlit dashboard."""
    st.set_page_config(
        page_title="BGG Data Warehouse Dashboard",
        page_icon="🎲",
        layout="wide"
    )

    st.title("BoardGameGeek Data Warehouse Dashboard")
    
    try:
        dashboard = BGGDashboard()
        
        # Top Games
        st.header("Top Rated Games")
        top_n = st.slider("Number of games to show", 5, 50, 10)
        top_games = dashboard.get_top_games(limit=top_n)
        
        fig = px.bar(
            top_games,
            x="name",
            y="rating",
            hover_data=["year_published", "num_ratings", "weight"],
            title="Top Rated Games"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Games by Year
        st.header("Games by Year")
        yearly_stats = dashboard.get_games_by_year()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(
                yearly_stats,
                x="year_published",
                y="game_count",
                title="Number of Games Published"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(
                yearly_stats,
                x="year_published",
                y=["avg_rating", "avg_weight"],
                title="Average Rating and Weight by Year"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Popular Mechanics
        st.header("Popular Game Mechanics")
        mechanics = dashboard.get_popular_mechanics()
        
        fig = px.scatter(
            mechanics,
            x="game_count",
            y="avg_rating",
            text="mechanic",
            title="Game Mechanics by Popularity and Rating",
            labels={
                "game_count": "Number of Games",
                "avg_rating": "Average Rating"
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Data Quality
        st.header("Data Quality Metrics")
        quality_metrics = dashboard.get_data_quality_metrics()
        
        # Summary metrics
        total_checks = len(quality_metrics)
        passed_checks = len(quality_metrics[quality_metrics["check_status"] == "PASSED"])
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Checks", total_checks)
        col2.metric("Passed Checks", passed_checks)
        col3.metric("Success Rate", f"{(passed_checks/total_checks)*100:.1f}%")
        
        # Detailed metrics table
        st.dataframe(
            quality_metrics[[
                "check_name",
                "table_name",
                "check_status",
                "records_checked",
                "failed_records",
                "check_timestamp"
            ]],
            hide_index=True
        )

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        logger.error("Dashboard error: %s", e)

if __name__ == "__main__":
    main()
