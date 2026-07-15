---
name: explain-codebase
description: Explain how part of this repo works and how the pieces fit together. Use when onboarding, answering "where does X happen / how does the data flow / what calls this", or building a mental model before making a change. Read-first; grounds every claim in actual files.
---

# Explain codebase

Give an accurate, grounded explanation of how something works here — trace the real
code and data flow, cite concrete files, and don't guess. Read before you explain.

## How to explain

1. **Find the entry point**, then follow the calls. Cite `path:line` so the user can
   click through. If a claim isn't backed by a file you read, don't make it.

2. **Situate it in the pipeline.** Most things here are a stage in the daily flow:
   **scrape IDs → fetch game data → land in BigQuery `raw` → Dataform transforms →
   models/predictions**. Say where the piece sits and what triggers it.

3. **Name the boundary it crosses** — a BigQuery table, a BGG HTTP call, a
   `repository_dispatch` event, a cross-project Dataform source. That's usually where
   the interesting behavior (and the bugs) live.

4. **Match depth to the question.** "Where does X happen" → point to the file/function.
   "How does the whole thing work" → a short flow with the key hops, not a file dump.

## Map of the repo

- **`src/pipeline/`** — orchestration entry points, run as `uv run python -m src.pipeline.<name>`
  (e.g. `fetch_thing_ids`, `fetch_new_games`, `fetch_games`).
- **`src/modules/`, `src/id_fetcher/`, `src/api_client/`, `src/data_processor/`,
  `src/warehouse/`** — the building blocks: BGG scraping/ID discovery, the BGG API
  client, response processing, and BigQuery load/merge logic.
- **`definitions/*.sqlx` + `sources.js`** — Dataform: SQL transformations and source
  declarations. Config in `workflow_settings.yaml`.
- **`.github/workflows/`** — orchestration. The chain `Fetch Thing IDs → Fetch New Games
  → Run Dataform` is stitched with `repository_dispatch`; `Scrape Heartbeat` watches it.
- **`scripts/box/`** — the residential-IP home-box scrape wrapper (Cloudflare blocks
  datacenter egress) and its setup docs.
- **`config/`** — `bigquery.yaml` (project/datasets, refresh policy), `cloudbuild.yaml`.
- **`docs/architecture/diagrams/`** — architecture and lineage diagrams; good for the
  big picture.

## Reminders

- Prefer `Grep`/`Glob` to locate things over assuming a path.
- The BigQuery datasets are `raw` (landed), `core`, `predictions`, `analytics` (Dataform).
- Timestamps in `logs/` are local time despite the trailing `Z`.
