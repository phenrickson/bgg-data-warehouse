.PHONY: install test lint clean fetch load update quality create-datasets

install:
	uv pip install -e .

test:
	uv run -m pytest

lint:
	black .
	ruff check .
	mypy .

clean:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf **/__pycache__
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

# Data pipeline tasks
fetch-ids:
	uv run -m src.id_fetcher.fetcher

fetch-games:
	uv run -m src.pipeline.fetch_data

# Utility tasks
examine-game:
	@if [ -z "$(GAME)" ]; then \
		echo "Usage: make examine-game GAME=1234"; \
		exit 1; \
	fi
	uv run -m src.scripts.examine_game $(GAME)

check-duplicates:
	uv run -m src.scripts.check_duplicates

# Load data tasks
load-unprocessed:
	uv run -m src.scripts.load_games

load-games:
	@if [ -z "$(GAMES)" ]; then \
		echo "Usage: make load-games GAMES='1234 5678 9012'"; \
		exit 1; \
	fi
	uv run -m src.scripts.load_games $(GAMES)

load: load-unprocessed
	@echo "Loaded all unprocessed games"

update:
	uv run -m src.pipeline.update_data

quality:
	uv run -m src.quality_monitor.monitor

# BigQuery setup tasks
create-datasets:
	uv run -m src.warehouse.setup_bigquery

# Development tasks
dev-setup: install create-datasets
	@echo "Development environment setup complete"

# Visualization
dashboard:
	uv streamlit run src/visualization/dashboard.py

.DEFAULT_GOAL := help
help:
	@echo "Available commands:"
	@echo "  install         Install project dependencies"
	@echo "  test           Run tests"
	@echo "  lint           Run code quality checks"
	@echo "  clean          Clean temporary files"
	@echo "  fetch-ids      Fetch game IDs from BGG"
	@echo "  fetch-games    Fetch game data from BGG API"
	@echo "  examine-game   Examine a specific game (Usage: make examine-game GAME=1234)"
	@echo "  check-duplicates Check for duplicate game entries"
	@echo "  load           Load all unprocessed games to BigQuery"
	@echo "  load-unprocessed Load all unprocessed games to BigQuery"
	@echo "  load-games     Load specific games (Usage: make load-games GAMES='1234 5678')"
	@echo "  update         Update data in BigQuery"
	@echo "  quality        Run data quality checks"
	@echo "  create-datasets Setup BigQuery datasets"
	@echo "  dev-setup      Complete development environment setup"
	@echo "  dashboard      Run the visualization dashboard"
