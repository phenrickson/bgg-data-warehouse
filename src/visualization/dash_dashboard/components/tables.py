"""Table components."""

import dash_bootstrap_components as dbc
from dash import html
import pandas as pd


def create_games_table(df: pd.DataFrame):
    """Create a table displaying game information.

    Args:
        df (pd.DataFrame): DataFrame containing game data

    Returns:
        dash_bootstrap_components.Table: A styled table component
    """
    # Format DataFrame
    df = df.copy()
    df["bgg_url"] = df["game_id"].apply(lambda x: f"https://boardgamegeek.com/boardgame/{x}/")
    df["average_rating"] = df["average_rating"].round(2)
    df["load_timestamp"] = pd.to_datetime(df["load_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Create table with links
    table_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("Game ID"),
                    html.Th("Name"),
                    html.Th("Year"),
                    html.Th("Avg Rating"),
                    html.Th("# Ratings"),
                    html.Th("Added"),
                ]
            )
        )
    ]

    rows = []
    for _, row in df.iterrows():
        rows.append(
            html.Tr(
                [
                    html.Td(row["game_id"]),
                    html.Td(html.A(row["name"], href=row["bgg_url"], target="_blank")),
                    html.Td(row["year_published"]),
                    html.Td(row["average_rating"]),
                    html.Td(row["users_rated"]),
                    html.Td(row["load_timestamp"]),
                ]
            )
        )

    table_body = [html.Tbody(rows)]

    return dbc.Table(
        table_header + table_body,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="align-middle",
    )


def create_error_table(df: pd.DataFrame):
    """Create a table displaying error information.

    Args:
        df (pd.DataFrame): DataFrame containing error data

    Returns:
        dash_bootstrap_components.Table: A styled table component
    """
    # Format DataFrame
    df = df.copy()
    df["fetch_timestamp"] = pd.to_datetime(df["fetch_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["process_timestamp"] = pd.to_datetime(df["process_timestamp"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    df["bgg_url"] = df["game_id"].apply(lambda x: f"https://boardgamegeek.com/boardgame/{x}/")

    # Create table with links
    table_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("Game ID"),
                    html.Th("Error"),
                    html.Th("Attempt"),
                    html.Th("Fetch Time"),
                    html.Th("Process Time"),
                ]
            )
        )
    ]

    rows = []
    for _, row in df.iterrows():
        rows.append(
            html.Tr(
                [
                    html.Td(html.A(row["game_id"], href=row["bgg_url"], target="_blank")),
                    html.Td(row["error"]),
                    html.Td(row["process_attempt"]),
                    html.Td(row["fetch_timestamp"]),
                    html.Td(row["process_timestamp"]),
                ]
            )
        )

    table_body = [html.Tbody(rows)]

    return dbc.Table(
        table_header + table_body,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="align-middle",
    )
