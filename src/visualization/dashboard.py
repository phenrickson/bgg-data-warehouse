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
        logger.info(f"Config: {self.config}")
        env_config = self.config["environments"]["dev"]  # Using dev environment
        self.project_id = env_config["project_id"]
        self.dataset = env_config["dataset"]  # Using the dev dataset
        logger.info(f"Project ID: {self.project_id}, Dataset: {self.dataset}")
        self.client = bigquery.Client(project=self.project_id)

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
        SELECT *
        FROM `{self.project_id}.{self.dataset}.games`
        LIMIT 1
        """
        
        # First, get the schema
        schema_query = self.client.query(query)
        schema_df = schema_query.to_dataframe()
        logger.info(f"Table columns: {list(schema_df.columns)}")
        
        # Now the actual query
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset}.games`
        ORDER BY average DESC
        LIMIT {limit}
        """
        
        return self.client.query(query).to_dataframe()

    def get_games_by_year(self, min_ratings: int = 0) -> pd.DataFrame:
        """Get game counts and ratings by year.
        
        Args:
            min_ratings: Minimum number of ratings to include
            
        Returns:
            DataFrame of game statistics by year
        """
        query = f"""
        SELECT
            year_published,
            COUNT(*) as game_count,
            AVG(average) as avg_rating,
            AVG(weight) as avg_weight,
            AVG(num_ratings) as avg_num_ratings
        FROM `{self.project_id}.{self.dataset}.games`
        WHERE year_published IS NOT NULL
        AND num_ratings >= {min_ratings}
        GROUP BY year_published
        ORDER BY year_published
        """
        
        return self.client.query(query).to_dataframe()

    def get_weight_vs_rating(self, min_ratings: int = 0, min_year: int = 1900) -> pd.DataFrame:
        """Get weight vs rating data.
        
        Args:
            min_ratings: Minimum number of ratings to include
            min_year: Minimum year published
            
        Returns:
            DataFrame of weight vs rating data
        """
        query = f"""
        SELECT 
            name,
            year_published,
            weight as average_weight,
            average as average_rating,
            num_ratings
        FROM `{self.project_id}.{self.dataset}.games`
        WHERE year_published >= {min_year}
        AND num_ratings >= {min_ratings}
        AND weight IS NOT NULL
        AND average IS NOT NULL
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
        FROM `{self.project_id}.{self.dataset}.games`
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
        FROM `{self.project_id}.{self.config["datasets"]["monitoring"]}.data_quality`
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
    st.markdown("""
    Explore and analyze BoardGameGeek data through interactive visualizations.
    Use the sidebar filters to customize the analysis.
    """)
    
    try:
        dashboard = BGGDashboard()
        
        # Sidebar filters
        st.sidebar.header("Filters")
        min_year = st.sidebar.slider("Minimum Year Published", 1900, 2024, 1900)
        min_ratings = st.sidebar.slider("Minimum Number of Ratings", 0, 10000, 100)
        
        # Weight vs Rating Analysis
        st.header("Game Weight vs Rating Analysis")
        weight_rating_data = dashboard.get_weight_vs_rating(min_ratings=min_ratings, min_year=min_year)
        
        fig = px.scatter(
            weight_rating_data,
            x="average_weight",
            y="average_rating",
            hover_data=["name", "year_published", "num_ratings"],
            title="Game Complexity (Weight) vs Rating",
            labels={
                "average_weight": "Game Weight (Complexity)",
                "average_rating": "Average Rating"
            },
            trendline="ols",
            color="num_ratings",
            size="num_ratings",
            size_max=30
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Correlation Analysis
        correlation = weight_rating_data["average_weight"].corr(weight_rating_data["average_rating"])
        st.metric("Correlation between Weight and Rating", f"{correlation:.3f}")
        
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
        yearly_stats = dashboard.get_games_by_year(min_ratings=min_ratings)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(
                yearly_stats,
                x="year_published",
                y="game_count",
                title="Number of Games Published by Year",
                labels={
                    "year_published": "Year",
                    "game_count": "Number of Games"
                }
            )
            fig.add_scatter(
                x=yearly_stats["year_published"],
                y=yearly_stats["avg_num_ratings"],
                name="Avg Ratings per Game",
                yaxis="y2"
            )
            fig.update_layout(
                yaxis2=dict(
                    title="Average Number of Ratings",
                    overlaying="y",
                    side="right"
                )
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
