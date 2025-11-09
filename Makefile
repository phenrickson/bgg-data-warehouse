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
source-dataset ?= bgg_data_dev
target-dataset ?= bgg_data_prod
source-raw ?= bgg_raw_dev
target-raw ?= bgg_raw_prod
TARGET_ENV ?= prod

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
	uv run -m src.warehouse.create_views \
	--environment $(TARGET_ENV)

add-record-id:
	uv run -m src.warehouse.migration_scripts.add_record_id_to_raw_responses \
	--environment $(TARGET_ENV)

migrate-data: migrate-bgg-data migrate-bgg-raw create-views

# run steps of pipeline
.PHONY: fetch-ids fetch-responses process-responses
fetch-ids:
	uv run -m src.pipeline.fetch_ids

fetch-responses:
	uv run -m src.pipeline.fetch_responses

process-responses:
	uv run -m src.pipeline.process_responses

pipeline:
	fetch-ids fetch-responses process-responses