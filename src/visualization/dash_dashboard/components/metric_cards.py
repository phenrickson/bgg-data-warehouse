"""Metric card components."""

import dash_bootstrap_components as dbc
from dash import html


def create_metric_card(title, value):
    """Create a metric card component.

    Args:
        title (str): The title of the metric
        value (Any): The value to display

    Returns:
        dash_bootstrap_components.Card: A styled card component
    """
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H4(title, className="card-title text-center"),
                    html.H2(str(value), className="card-text text-center"),
                ]
            )
        ],
        className="h-100 shadow-sm",
    )
