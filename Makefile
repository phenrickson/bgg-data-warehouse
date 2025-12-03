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
target-dataset ?= bgg_data_dev
source-raw ?= bgg_raw_prod
target-raw ?= bgg_raw_dev
TARGET_ENV ?= dev

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
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.migration_scripts.add_record_id_to_raw_responses

create-tracking-tables:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.migration_scripts.create_tracking_tables

backfill-tracking-tables:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.migration_scripts.backfill_tracking_tables

remove-processed-columns:
	set ENVIRONMENT=$(TARGET_ENV) && uv run -m src.warehouse.migration_scripts.remove_processed_columns

.PHONY: migrate-bgg-data migrate-bgg-raw create-views create-scheduled-tables add-record-id create-tracking-tables backfill-tracking-tables remove-processed-columns
migrate-dataset: migrate-bgg-data migrate-bgg-raw create-views

# Complete migration workflow: copy prod to target env and apply all migrations
migrate-full: migrate-bgg-data migrate-bgg-raw create-views create-scheduled-tables add-record-id create-tracking-tables backfill-tracking-tables remove-processed-columns