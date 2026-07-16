# Games Resource — First Slice of the Warehouse Read APIs

> Part of the warehouse services architecture — see
> [2026-07-16-warehouse-services-architecture-design.md](2026-07-16-warehouse-services-architecture-design.md).
> "Games" is the **`games` router inside the single `services/warehouse_api/` app**,
> not a standalone service. This is rollout step 2 (the first real resource slice).

## Problem

The front-end (`bgg-dash-viewer`) builds its `/app/game/<id>` page — and its game
search, ratings, and new-games pages — by reaching directly into BigQuery through
hand-written SQL in `src/data/bigquery_client.py` and inline in Dash callbacks. This
entangles back-end data access with the front-end: the UI holds BigQuery credentials
and owns the query logic, and any other consumer wanting game data re-implements the
same joins. There is no reusable, decoupled way to ask the warehouse for a game.

## Solution

The `games` router of `services/warehouse_api/` (the modular-monolith read API defined
in the architecture spec), reading the warehouse's already-materialized per-game
tables. The query logic lives in `src/warehouse/readers/games.py` (pure,
unit-testable); the router is a thin FastAPI shell. The front-end deletes its game SQL
and becomes a pure HTTP consumer.

This is the **live-query** approach: a handful of small point-lookups per request,
composed. It deliberately does *not* pre-materialize a `game_profile` table — see
*Deferred*.

## Endpoints (games router)

- `GET /games/{game_id}` — full profile: features + current/historical predictions +
  embedding coordinates + provenance.
- `GET /games/{game_id}/predictions | /features | /similar | /embedding | /players | /provenance`
  — per-block, so the front-end can lazy-load.
- `GET /games` — list / filter / sort / paginate.
- `GET /games/search?q=` , `GET /games/new` , `GET /games/summary`.

## Components

### 1. Reader — `src/warehouse/readers/games.py`

One function per block plus a `get_game(game_id)` aggregator, reusing the shared
BigQuery client factory and dataset config:

- `get_features(game_id)` — game record + player-count recommendations
- `get_predictions(game_id)` — current prediction + history + `first_prediction_ts`
- `get_embedding(game_id)` — embedding coordinates
- `get_similar(game_id, n)` — nearest neighbours computed **in-warehouse** from
  `analytics.game_similarity_search` (`ML.DISTANCE`); no call-out to
  `bgg-predictive-models` (the table already lives in this project)
- `get_provenance(game_id)` — fetch/load timestamps ("when we pulled this")
- `list_games(...)`, `search_games(q)`, `new_games(...)`, `summary()` — the list surface

### 2. Router — `services/warehouse_api/routers/games.py`

FastAPI `APIRouter` mounting the endpoints above; calls the reader, shapes pydantic
responses, `404` on missing id. Mounted by `services/warehouse_api/main.py`.

### 3. Shared plumbing (from the architecture skeleton, not games-specific)

Config extension (`predictions`/`analytics` datasets), `auth.py` (Cloud Run IAM + ID
token), `Dockerfile` (uv-based, modeled on `docker/Dockerfile.pipeline`),
`cloudbuild.yaml`, and `deploy-warehouse-api.yml`.

### 4. Front-end consumer — separate PR in `bgg-dash-viewer`

- `src/data/warehouse_api_client.py` — thin `requests` wrapper (same shape as the
  existing `ServiceSimilarityClient`), attaches the ID token.
- `WAREHOUSE_API_URL` in `src/config.py`.
- Rewrite `src/layouts/game_details.py` and the game-search/new-games callbacks to
  call the API; delete the game SQL from `src/data/bigquery_client.py` and the inline
  callback SQL it replaces.

## Data contract

`GET /games/{game_id}` composes blocks sourced from:

| Block | Source (dataset.table) |
|---|---|
| `features` | `analytics.games_features`, `analytics.player_count_recommendations`, `analytics.best_player_counts` |
| `predictions` (current) | `predictions.bgg_predictions` |
| `predictions.history` + `first_prediction_ts` | `predictions.game_first_prediction` |
| `embedding` / `coordinates` | `predictions.bgg_game_coordinates` |
| `similar` | `analytics.game_similarity_search` (`ML.DISTANCE`) |
| `provenance` | `raw.fetched_responses` / load-timestamp tables |

## Validation

- `tests/test_game_reader.py` — mocked `bigquery.Client`; each block function targets
  the right table and shapes its payload.
- Local `uvicorn`: `curl /health` → 200; `curl /games/13` (Catan) → populated doc.
- Deployed: `/health` → 200; `/games/13` → real data; unauthenticated call → 403.
- Front-end: `/app/game/<id>` renders identically off the API; no game SQL remains in
  `bigquery_client.py`.

## What this doesn't change

- **No live inference** — serves already-materialized results only.
- **No new Dataform models, no schema migration, no backfill** — reads existing tables
  (adding `clusterBy game_id` to serving tables is the architecture-level perf change,
  tracked there, not here).
- **Other dash-viewer pages** (similarity, collections, experiments, monitoring) move
  in later slices — out of scope here.
- **No write endpoints.**

## Deferred

If the per-request fan-out proves too costly/slow, introduce a Dataform incremental
`analytics.game_profile` model — one pre-joined row per game with top-N neighbours as
an array — and collapse `GET /games/{id}` to a single clustered lookup. Documented,
not built in this iteration.
