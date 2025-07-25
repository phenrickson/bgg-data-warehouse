"""Script to run the Dash dashboard."""

from src.visualization.dash_dashboard.app import app
from src.visualization.dash_dashboard.callbacks import register_callbacks


def main():
    """Run the Dash application."""
    # Import here to ensure state is initialized after environment is loaded
    from src.visualization.dash_dashboard.state import state
    
    # Register all callbacks
    register_callbacks(app, state.bq_client)
    
    # Start the server
    app.run(debug=True, port=8050)
if __name__ == "__main__":
    main()
