"""Dash application callbacks."""

from datetime import datetime, timezone
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from . import components
from src.visualization import queries
from src.visualization.dashboard import format_project_dataset, run_query


def register_callbacks(app, client):
    """Register all callbacks for the Dash application."""

    @app.callback(Output("last-updated", "children"), Input("interval-component", "n_intervals"))
    def update_timestamp(n):
        current_time = datetime.now(timezone.utc)
        return f"Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"

    @app.callback(
        [
            Output("total-games-card", "children"),
            Output("ranked-games-card", "children"),
            Output("responses-card", "children"),
            Output("success-rate-card", "children"),
            Output("unprocessed-card", "children"),
        ],
        Input("interval-component", "n_intervals"),
    )
    def update_top_metrics(n):
        total_games = run_query(client, queries.TOTAL_GAMES_QUERY)
        games_with_bayes = run_query(client, queries.GAMES_WITH_BAYESAVERAGE_QUERY)
        processing_status = run_query(client, queries.PROCESSING_STATUS)
        unprocessed = run_query(client, queries.UNPROCESSED_RESPONSES_QUERY)

        return [
            components.create_metric_card("Total Games", total_games.iloc[0]["total_games"]),
            components.create_metric_card(
                "Ranked Games", games_with_bayes.iloc[0]["games_with_bayesaverage"]
            ),
            components.create_metric_card(
                "Responses (7d)", processing_status.iloc[0]["total_responses"]
            ),
            components.create_metric_card(
                "Success Rate", f"{processing_status.iloc[0]['success_rate']}%"
            ),
            components.create_metric_card("Unprocessed", unprocessed.iloc[0]["unprocessed_count"]),
        ]

    @app.callback(
        [
            Output("categories-card", "children"),
            Output("mechanics-card", "children"),
            Output("families-card", "children"),
            Output("designers-card", "children"),
            Output("artists-card", "children"),
            Output("publishers-card", "children"),
        ],
        Input("interval-component", "n_intervals"),
    )
    def update_entity_counts(n):
        entity_counts = run_query(client, queries.ALL_ENTITY_COUNTS_QUERY)
        data = entity_counts.iloc[0]

        return [
            components.create_metric_card("Categories", data["category_count"]),
            components.create_metric_card("Mechanics", data["mechanic_count"]),
            components.create_metric_card("Families", data["family_count"]),
            components.create_metric_card("Designers", data["designer_count"]),
            components.create_metric_card("Artists", data["artist_count"]),
            components.create_metric_card("Publishers", data["publisher_count"]),
        ]

    @app.callback(
        Output("fetch-activity-chart", "figure"), Input("interval-component", "n_intervals")
    )
    def update_fetch_activity(n):
        df = run_query(client, queries.RECENT_FETCH_ACTIVITY)
        fig = px.line(df, x="date", y="responses_fetched", title="Daily Fetch Counts")
        fig.update_layout(template="plotly_dark")
        return fig

    @app.callback(
        Output("processing-activity-chart", "figure"), Input("interval-component", "n_intervals")
    )
    def update_processing_activity(n):
        df = run_query(client, queries.DAILY_PROCESSING_COUNTS)
        fig = px.line(df, x="date", y="processed_count", title="Daily Processing Counts")
        fig.update_layout(template="plotly_dark")
        return fig

    @app.callback(
        Output("latest-games-table", "children"), Input("interval-component", "n_intervals")
    )
    def update_latest_games(n):
        df = run_query(client, queries.LATEST_GAMES)
        return components.create_games_table(df)

    @app.callback(
        Output("error-trends-chart", "figure"), Input("interval-component", "n_intervals")
    )
    def update_error_trends(n):
        df = run_query(client, queries.PROCESSING_ERROR_TRENDS)
        fig = px.line(df, x="date", y="error_count", title="Daily Error Counts")
        fig.update_layout(template="plotly_dark")
        return fig

    @app.callback(
        Output("recent-errors-table", "children"), Input("interval-component", "n_intervals")
    )
    def update_recent_errors(n):
        df = run_query(client, queries.RECENT_ERRORS)
        if df.empty:
            return "No recent processing errors! 🎉"
        return components.create_error_table(df)
