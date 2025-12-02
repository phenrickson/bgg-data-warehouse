.PHONY: requirements test lint clean fetch load update quality create-datasets

requirements:
	uv sync

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

monitor:
	uv run streamlit run src/visualization/dashboard.py --server.port 8501

search:
	uv run streamlit run src/visualization/game_search_dashboard.py --server.port 8502

# migrate
project-id ?= gcp-demos-411520
source-dataset ?= bgg_data_prod
target-dataset ?= bgg_data_test
source-raw ?= bgg_raw_prod
target-raw ?= bgg_raw_test
TARGET_ENV ?= test

migrate-bgg-data:
	uv run -m src.warehouse.migrate_datasets \
	--source-dataset $(source-dataset) \
	--dest-dataset $(target-dataset) \
	--project-id gcp-demos-411520

migrate-bgg-raw:
	uv run -m src.warehouse.migrate_datasets \
	--source-dataset $(source-raw) \
	--dest-dataset ${target-raw} \
	--project-id gcp-demos-411520

create-views:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.create_views

create-scheduled-tables:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.create_scheduled_tables

add-record-id:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.migration_scripts.add_record_id

.PHONY: migrate-bgg-data migrate-bgg-raw create-views create-scheduled-tables add-record-id
migrate-dataset: migrate-bgg-data migrate-bgg-raw create-views