"""Reusable components for the monitoring dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


def create_metric_card(
    label: str, value: str | int | float, delta: str | int | float | None = None
) -> None:
    """Create a metric card with optional delta value."""
    st.metric(label=label, value=value, delta=delta)


def create_time_series(
    df: pd.DataFrame, date_col: str, value_col: str, title: str, color: str = "#1f77b4"
) -> None:
    """Create a time series line chart."""
    fig = px.line(df, x=date_col, y=value_col, title=title, template="plotly_white")
    fig.update_traces(line_color=color)
    st.plotly_chart(fig, use_container_width=True)


def create_error_table(df: pd.DataFrame) -> None:
    """Create a styled table for error display."""
    st.dataframe(
        df,
        column_config={
            "game_id": "Game ID",
            "error": "Error Message",
            "process_attempt": "Attempt #",
            "fetch_timestamp": st.column_config.DatetimeColumn(
                "Fetch Time", format="MM/DD/YY HH:mm:ss"
            ),
            "process_timestamp": st.column_config.DatetimeColumn(
                "Process Time", format="MM/DD/YY HH:mm:ss"
            ),
        },
        hide_index=True,
    )


def create_latest_games_table(df: pd.DataFrame) -> None:
    """Create a styled table for latest games."""
    # Create a new column with BGG URLs
    df["bgg_url"] = df["game_id"].apply(lambda x: f"https://boardgamegeek.com/boardgame/{x}/")

    # Use HTML to create clickable links
    html = "<table>"
    html += "<tr><th>Game ID</th><th>Game Name</th><th>Year</th><th>Avg Rating</th><th># Ratings</th><th>Added</th></tr>"

    for _, row in df.iterrows():
        html += f"""
        <tr>
            <td>{row["game_id"]}</td>
            <td><a href="{row["bgg_url"]}" target="_blank">{row["name"]}</a></td>
            <td>{row["year_published"]}</td>
            <td>{row["average_rating"]:.2f}</td>
            <td>{row["users_rated"]}</td>
            <td>{row["load_timestamp"].strftime("%m/%d/%y %H:%M:%S")}</td>
        </tr>
        """

    html += "</table>"

    # Apply some CSS to make it look like a Streamlit table
    st.markdown(
        f"""
        <style>
        .stMarkdown {{
            margin-top: -20px;  /* Remove top margin from markdown container */
            padding-top: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 0;
            padding-top: 0;
        }}
        th, td {{
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #0e1117;  /* Match Streamlit dark theme */
            color: white;
        }}
        tr:hover {{
            background-color: #1e1e1e;  /* Darker hover for dark theme */
        }}
        a {{
            color: #4da6ff;  /* Brighter blue for dark theme */
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        </style>
        {html}
        """,
        unsafe_allow_html=True,
    )


def create_processing_gauge(success_rate: float) -> None:
    """Create a gauge chart for processing success rate."""
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=success_rate,
            title={"text": "Processing Success Rate"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#1f77b4"},
                "steps": [
                    {"range": [0, 60], "color": "#ff7f7f"},
                    {"range": [60, 80], "color": "#ffd700"},
                    {"range": [80, 100], "color": "#90ee90"},
                ],
                "threshold": {"line": {"color": "red", "width": 4}, "thickness": 0.75, "value": 95},
            },
        )
    )
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)
