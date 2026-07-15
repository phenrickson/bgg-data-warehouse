# BGG Data Warehouse

A data pipeline that collects BoardGameGeek (BGG) game data, lands it in BigQuery,
and transforms it into a normalized warehouse plus analytics and ML-prediction
tables for downstream consumers.

## Overview

1. **Discover IDs** — new game IDs are found by scraping BGG's sitemaps (which sit
   behind Cloudflare) and upserted into `raw.thing_ids`.
2. **Fetch** — game data is fetched from BGG's public XML API2 and stored as raw XML
   in BigQuery.
3. **Process** — raw responses are parsed into normalized `core` tables.
4. **Transform** — [Dataform](https://cloud.google.com/dataform) models build the
   `analytics` and `predictions` datasets.
5. **Enrich** — ML predictions and embeddings from the sibling
   [`bgg-predictive-models`](https://github.com/phenrickson/bgg-predictive-models)
   project flow in via cross-project Dataform sources, coordinated by a bidirectional
   `repository_dispatch` event chain.
6. **Consume** — the `analytics` and `predictions` datasets are read by downstream
   apps, notably the separate
   [`bgg-dash-viewer`](https://github.com/phenrickson/bgg-dash-viewer) project. This
   repo is the warehouse/back end; it does not serve a UI.

For the full picture see [docs/architecture.md](docs/architecture.md) and the
diagrams under [docs/architecture/diagrams/](docs/architecture/diagrams/).

## Architecture

### Pipelines (`src/pipeline/`)

| Pipeline | What it does | How it runs |
|----------|--------------|-------------|
| `fetch_thing_ids` | Discovers new game IDs by scraping BGG sitemaps (a stealth browser bypasses Cloudflare); MERGEs them into `raw.thing_ids`. | **Scheduled off-platform on a residential-IP home box** — datacenter egress is Cloudflare-blocked. On success the box fires a `thing_ids_fetched` `repository_dispatch`. The `Fetch Thing IDs` GitHub Actions workflow remains as a manual fallback. See [scripts/box/README.md](scripts/box/README.md). |
| `fetch_new_games` | Fetches API responses for unfetched IDs in `raw.thing_ids` and processes them into `core` tables. | Triggered by the home box's `thing_ids_fetched` dispatch (and after `Fetch Thing IDs`). |
| `refresh_old_games` | Re-fetches stale games based on a publication-year policy (see `config/bigquery.yaml`). | Scheduled daily at **07:00 UTC**. |
| `fetch_games` | On-demand fetch/refresh of specific game IDs. | Manual `workflow_dispatch` with a comma-separated `game_ids` input. |

### Orchestration (GitHub Actions + `repository_dispatch`)

The daily flow is event-driven rather than a fixed schedule:

```text
home box (~06:00 UTC)
  └─ repository_dispatch: thing_ids_fetched
       └─ Run Fetch New Games
            └─ (workflow_run) Run Dataform ──> analytics + predictions
                 └─ repository_dispatch: dataform_complete ──> bgg-predictive-models
                        (ML scores complexity → text embeddings → game embeddings)
                 ┌───────────────────────────────────────────────┘
                 └─ complexity_complete / text_embeddings_complete / embeddings_complete
                      └─ Run Dataform (re-run to publish the new ML outputs)
```

`Scrape Heartbeat` runs daily at 12:00 UTC and fails loudly if no home-box dispatch
has landed in ~26h (box offline, scrape error, etc.).

Key workflows in `.github/workflows/`:

| Workflow | Trigger |
|----------|---------|
| `fetch_new_games.yml` | `repository_dispatch: thing_ids_fetched`, after `Fetch Thing IDs`, or manual |
| `refresh.yml` | daily `0 7 * * *`, or manual |
| `fetch_games.yml` | manual `workflow_dispatch` (`game_ids` input) |
| `dataform.yml` | after a fetch/refresh, `repository_dispatch` from the ML repo, push to `definitions/**`, or manual |
| `fetch_thing_ids.yml` | manual fallback only |
| `scrape_heartbeat.yml` | daily `0 12 * * *` |
| `deploy.yml` | push to `main` (builds & deploys the Cloud Run jobs) |
| `terraform.yml` | push/PR to `terraform/**` |
| `tag-release.yml` | push to `main` touching `pyproject.toml` |

### Data model (BigQuery)

| Dataset | Managed by | Contents |
|---------|-----------|----------|
| `raw` | pipeline code | `thing_ids`, `raw_responses`, `fetched_responses`, `processed_responses`, `request_log`, `fetch_in_progress` |
| `core` | pipeline code | `games` plus dimension (`categories`, `mechanics`, `families`, …), creator (`designers`, `artists`, `publishers`) and association (`game_categories`, …) tables |
| `analytics` | Dataform | `games_active`, `games_features`, `best_player_counts`, `game_dropdown_options`, `game_similarity_search`, `filter_*` |
| `predictions` | Dataform (from `bgg-predictive-models`) | `bgg_predictions`, `bgg_complexity_predictions`, `bgg_game_embeddings`, `bgg_description_embeddings`, `bgg_game_coordinates`, `user_collection_predictions`, `game_first_prediction` |
| `staging`, `monitoring` | Dataform | internal (feature hashes, deployed-model registry) |

Dataform sources and cross-project declarations live in `definitions/sources.js`;
the model lineage is in [docs/lineage.md](docs/lineage.md). Dataform operational
notes (incremental refreshes, schema drift) are in
[docs/dataform_operations.md](docs/dataform_operations.md).

### Infrastructure

- **Cloud Run jobs** run the pipelines; they are built and deployed by **Cloud Build**
  (`config/cloudbuild.yaml`), not Terraform:
  `bgg-fetch-thing-ids` (8Gi / 2 vCPU), `bgg-fetch-new-games`, `bgg-refresh-old-games`,
  `bgg-fetch-games`.
- **Terraform** (`terraform/`) manages the artifact registry, service accounts, and
  Secret Manager — not the Cloud Run jobs.
- **Dataform** transformations run via the `dataform.yml` workflow.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- A Google Cloud project with BigQuery, Cloud Run, Cloud Build, and Dataform enabled
- A service account with BigQuery Data Editor and Cloud Run Invoker roles

## Setup

```bash
git clone https://github.com/phenrickson/bgg-data-warehouse.git
cd bgg-data-warehouse

# Install dependencies
uv sync

# Only needed to run the sitemap scraper (fetch_thing_ids) locally:
uv run playwright install chromium

# Configure environment
cp .env.example .env
# Set GOOGLE_APPLICATION_CREDENTIALS (path to your GCP service-account key) and,
# optionally, BGG_API_TOKEN. BGG's XML API2 is public — no token is required.
```

GitHub repository secrets used by the workflows:

- `GCP_SA_KEY_BGG_DW` — GCP service-account key JSON (**required**)
- `BGG_API_TOKEN` — optional; BGG's API needs no auth
- `CROSS_REPO_PAT` — PAT used to dispatch events to `bgg-predictive-models`

## Usage

### Local development

```bash
# Run a pipeline locally
uv run python -m src.pipeline.fetch_new_games
uv run python -m src.pipeline.refresh_old_games
uv run python -m src.pipeline.fetch_thing_ids      # requires playwright chromium
uv run python -m src.pipeline.fetch_games          # reads GAME_IDS env var

# Run the tests (pytest lives in the `test` extra)
uv run --extra test python -m pytest
```

### Manual Cloud Run job execution

```bash
gcloud run jobs execute bgg-fetch-new-games   --region us-central1 --wait
gcloud run jobs execute bgg-refresh-old-games --region us-central1 --wait
```

## Consumers

This repo is the warehouse itself and does not serve a UI. The consumer-facing app
is the separate [`bgg-dash-viewer`](https://github.com/phenrickson/bgg-dash-viewer)
project, which reads the `analytics` and `predictions` datasets. See the ecosystem
diagram in [docs/architecture/diagrams/](docs/architecture/diagrams/).

## Documentation

- [docs/architecture.md](docs/architecture.md) — system architecture and data flow
- [docs/dataform_operations.md](docs/dataform_operations.md) — Dataform incremental/refresh operations
- [docs/bgg_api.md](docs/bgg_api.md) — BGG XML API2 usage notes
- [docs/lineage.md](docs/lineage.md) — Dataform model lineage
- [scripts/box/README.md](scripts/box/README.md) — home-box scrape setup
- [.claude/skills/README.md](.claude/skills/README.md) — Claude Code skills for this repo

## Versioning

Semantic versioning. Bumping `version` in `pyproject.toml` on `main` triggers the
`Tag Release` workflow to create the matching `vX.Y.Z` tag. See
[CHANGELOG.md](CHANGELOG.md).

## License

MIT License
