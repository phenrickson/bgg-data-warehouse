.PHONY: install test lint clean fetch load update quality create-datasets

install:
	uv pip install -e ".[dev]"

test:
	pytest

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
fetch:
	python -m src.pipeline.fetch_data

load:
	python -m src.pipeline.load_data

update:
	python -m src.pipeline.update_data

quality:
	python -m src.pipeline.quality_checks

# BigQuery setup tasks
create-datasets:
	python -m src.warehouse.setup_bigquery

# Development tasks
dev-setup: install create-datasets
	@echo "Development environment setup complete"

# Visualization
dashboard:
	streamlit run src/visualization/dashboard.py

.DEFAULT_GOAL := help
help:
	@echo "Available commands:"
	@echo "  install         Install project dependencies"
	@echo "  test           Run tests"
	@echo "  lint           Run code quality checks"
	@echo "  clean          Clean temporary files"
	@echo "  fetch          Fetch data from BGG API"
	@echo "  load           Load data to BigQuery"
	@echo "  update         Update data in BigQuery"
	@echo "  quality        Run data quality checks"
	@echo "  create-datasets Setup BigQuery datasets"
	@echo "  dev-setup      Complete development environment setup"
	@echo "  dashboard      Run the visualization dashboard"
