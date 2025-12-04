"""Reusable components for the monitoring dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


def create_metric_card(label: str, value: str | int | float, delta: str | int | float | None = None) -> None:
    """Create a metric card with optional delta value."""
    st.metric(label=label, value=value, delta=delta)


def create_time_series(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    title: str,
    color: str = "#1f77b4"
) -> None:
    """Create a time series line chart."""
    fig = px.line(
        df,
        x=date_col,
        y=value_col,
        title=title,
        template="plotly_white"
    )
    fig.update_traces(line_color=color)
    st.plotly_chart(fig, use_container_width=True)


def create_error_table(df: pd.DataFrame) -> None:
    """Create a styled table for error display."""
    st.dataframe(
        df,
        column_config={
            "game_id": "Game ID",
            "error": "Error Status",
            "error_message": "Error Details",
            "process_attempt": "Attempt #",
            "fetch_timestamp": st.column_config.DatetimeColumn(
                "Fetch Time",
                format="MM/DD/YY HH:mm:ss"
            ),
            "process_timestamp": st.column_config.DatetimeColumn(
                "Process Time",
                format="MM/DD/YY HH:mm:ss"
            )
        },
        hide_index=True
    )


def create_latest_games_table(df: pd.DataFrame) -> None:
    """Create a styled table for latest games."""
    # Select and prepare columns for display
    display_df = df.copy()

    # Convert game_id to string for centered display
    display_df['game_id'] = display_df['game_id'].astype(str)

    # Create BGG link column
    display_df['bgg_link'] = display_df['game_id'].apply(
        lambda x: f"https://boardgamegeek.com/boardgame/{x}/"
    )

    # Reorder columns
    display_df = display_df[['game_id', 'bgg_link', 'name', 'year_published', 'users_rated', 'load_timestamp']]

    # Display with Streamlit dataframe
    st.dataframe(
        display_df,
        column_config={
            "game_id": st.column_config.TextColumn(
                "Game ID",
                help="BoardGameGeek Game ID"
            ),
            "bgg_link": st.column_config.LinkColumn(
                "Link",
                help="Link to BoardGameGeek page",
                display_text="BGG"
            ),
            "name": st.column_config.TextColumn(
                "Name",
                help="Game name"
            ),
            "year_published": st.column_config.NumberColumn(
                "Year Published",
                help="Year the game was published"
            ),
            "users_rated": st.column_config.NumberColumn(
                "Ratings",
                help="Number of user ratings"
            ),
            "load_timestamp": st.column_config.DatetimeColumn(
                "Added",
                format="MM/DD/YY HH:mm:ss",
                help="When the game was added to the warehouse"
            )
        },
        hide_index=True,
        height=900
    )


def create_processing_gauge(success_rate: float) -> None:
    """Create a gauge chart for processing success rate."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=success_rate,
        title={"text": "Processing Success Rate"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#1f77b4"},
            "steps": [
                {"range": [0, 60], "color": "#ff7f7f"},
                {"range": [60, 80], "color": "#ffd700"},
                {"range": [80, 100], "color": "#90ee90"}
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": 95
            }
        }
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)
