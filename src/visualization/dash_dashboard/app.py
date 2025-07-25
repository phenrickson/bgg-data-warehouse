"""Main Dash application # Initialize Dash app
# Layout
app.layout = dbc.Container(["""

import os
from pathlib import Path
import sys

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from google.cloud import bigquery
from google.auth import default
from dotenv import load_dotenv

# Add project root to Python path
root_dir = Path(__file__).parent.parent.parent.parent
sys.path.append(str(root_dir))

# Load environment variables
load_dotenv()

# Initialize Dash app with dark theme
app = dash.Dash(
    __name__, external_stylesheets=[dbc.themes.DARKLY], title="BGG Data Warehouse Monitor"
)

# Initialize BigQuery client
credentials, _ = default()
project_id = os.getenv("GCP_PROJECT_ID")
client = bigquery.Client(credentials=credentials, project=project_id)

# Layout
app.layout = dbc.Container(
    [
        # Header
        html.H1("🎲 BGG Data Warehouse Monitoring", className="my-4"),
        html.P("Real-time monitoring of the BGG data pipeline"),
        html.Div(id="last-updated", className="text-muted mb-4"),
        # Top Metrics Row
        dbc.Row(
            [
                dbc.Col(dcc.Loading(id="total-games-card"), width=2),
                dbc.Col(dcc.Loading(id="ranked-games-card"), width=2),
                dbc.Col(dcc.Loading(id="responses-card"), width=2),
                dbc.Col(dcc.Loading(id="success-rate-card"), width=2),
                dbc.Col(dcc.Loading(id="unprocessed-card"), width=2),
            ],
            className="mb-4",
        ),
        # Entity Counts Row
        html.H2("Game Metadata Counts", className="mb-3"),
        dbc.Row(
            [
                dbc.Col(dcc.Loading(id="categories-card"), width=2),
                dbc.Col(dcc.Loading(id="mechanics-card"), width=2),
                dbc.Col(dcc.Loading(id="families-card"), width=2),
                dbc.Col(dcc.Loading(id="designers-card"), width=2),
                dbc.Col(dcc.Loading(id="artists-card"), width=2),
                dbc.Col(dcc.Loading(id="publishers-card"), width=2),
            ],
            className="mb-4",
        ),
        # Activity Charts Row
        dbc.Row(
            [
                dbc.Col(
                    [html.H3("Fetch Activity"), dcc.Loading(dcc.Graph(id="fetch-activity-chart"))],
                    width=6,
                ),
                dbc.Col(
                    [
                        html.H3("Processing Activity"),
                        dcc.Loading(dcc.Graph(id="processing-activity-chart")),
                    ],
                    width=6,
                ),
            ],
            className="mb-4",
        ),
        # Latest Games Table
        dbc.Row(
            [dbc.Col([html.H3("Latest Games Added"), dcc.Loading(id="latest-games-table")])],
            className="mb-4",
        ),
        # Error Trends and Recent Errors
        dbc.Row(
            [
                dbc.Col(
                    [html.H3("Error Trends"), dcc.Loading(dcc.Graph(id="error-trends-chart"))],
                    width=12,
                ),
            ],
            className="mb-4",
        ),
        dbc.Row(
            [dbc.Col([html.H3("Recent Processing Errors"), dcc.Loading(id="recent-errors-table")])]
        ),
        # Auto-refresh
        dcc.Interval(
            id="interval-component", interval=60 * 1000, n_intervals=0  # refresh every minute
        ),
    ],
    fluid=True,
)
