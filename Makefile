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

fetch-responses:
	uv run -m src.pipeline.fetch_responses

process-responses:
	uv run -m src.pipeline.process_responses

# Environment-specific tasks
create-datasets:
	uv run -m src.warehouse.setup_bigquery

# Utility tasks
examine-game:
	@if [ -z "$(GAME)" ]; then \
		echo "Usage: make examine-game GAME=1234"; \
		exit 1; \
	fi
	uv run -m src.scripts.examine_game $(GAME)

check-duplicates:
	uv run -m src.scripts.check_duplicates

# Load data tasks (default to dev environment)
ENV ?= dev

load-unprocessed:
	ENVIRONMENT=$(ENV) uv run -m src.scripts.load_games

load-games:
	@if [ -z "$(GAMES)" ]; then \
		echo "Usage: make load-games GAMES='1234 5678 9012' [ENV=prod|dev|test]"; \
		exit 1; \
	fi
	ENVIRONMENT=$(ENV) uv run -m src.scripts.load_games $(GAMES)

load: load-unprocessed
	@echo "Loaded all unprocessed games to $(ENV) environment"

update:
	ENVIRONMENT=$(ENV) uv run -m src.pipeline.update_data

quality:
	ENVIRONMENT=$(ENV) uv run -m src.quality_monitor.monitor

# Development tasks
dev-setup: install create-datasets-dev
	@echo "Development environment setup complete"

test-setup: install create-datasets-test
	@echo "Test environment setup complete"

prod-setup: install create-datasets-prod
	@echo "Production environment setup complete"

# Visualization
dashboard:
	uv streamlit run src/visualization/dashboard.py

.DEFAULT_GOAL := help
help:
	@echo "Available commands:"
	@echo "  install           Install project dependencies"
	@echo "  test             Run tests"
	@echo "  lint             Run code quality checks"
	@echo "  clean            Clean temporary files"
	@echo ""
	@echo "Environment Setup:"
	@echo "  dev-setup        Setup development environment"
	@echo "  test-setup       Setup test environment"
	@echo "  prod-setup       Setup production environment"
	@echo "  create-datasets  Create datasets in all environments"
	@echo ""
	@echo "Data Pipeline Tasks:"
	@echo "  fetch-ids        Fetch game IDs from BGG (prod only)"
	@echo "  fetch-games      Fetch game data from BGG API (prod only)"
	@echo "  load            Load all unprocessed games (ENV=prod|dev|test, default: dev)"
	@echo "  load-unprocessed Load all unprocessed games (ENV=prod|dev|test, default: dev)"
	@echo "  load-games      Load specific games (Usage: make load-games GAMES='1234 5678' [ENV=prod|dev|test])"
	@echo "  update          Update data (ENV=prod|dev|test, default: dev)"
	@echo "  quality         Run data quality checks (ENV=prod|dev|test, default: dev)"
	@echo ""
	@echo "Utility Tasks:"
	@echo "  examine-game     Examine a specific game (Usage: make examine-game GAME=1234)"
	@echo "  check-duplicates Check for duplicate game entries"
	@echo "  dashboard        Run the visualization dashboard"
	@echo ""
	@echo "Examples:"
	@echo "  make load ENV=dev                    # Load unprocessed games to dev environment"
	@echo "  make load-games GAMES='1234' ENV=test # Load specific game to test environment"
	@echo "  make quality ENV=prod                # Run quality checks on production data"
