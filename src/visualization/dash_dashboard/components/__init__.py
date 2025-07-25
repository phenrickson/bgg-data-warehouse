"""Dashboard components module."""

from . import metric_cards
from . import tables

# Re-export components
from .metric_cards import create_metric_card
from .tables import create_games_table, create_error_table
