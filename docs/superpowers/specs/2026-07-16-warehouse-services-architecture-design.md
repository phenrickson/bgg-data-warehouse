# Warehouse Read APIs — Services Architecture

## Problem

`bgg-dash-viewer` reads warehouse data through hand-written BigQuery SQL — spread
across its `src/data/` layer *and* inline inside Dash callbacks (ratings scatter,
similarity dropdown/name-search/feature-cards, all monitoring/catalog queries).
Every consumer that wants warehouse data re-implements the joins and holds BigQuery
credentials. There is no coherent, reusable way to ask the warehouse for data.

We want a set of **read APIs owned by `bgg-data-warehouse`** — the project that owns
the data — so front-ends and other consumers become pure HTTP clients. This document
defines the architecture for that services layer. Individual endpoints (games,
publishers, …) are *slices* of it, each specced and shipped separately.

## The shape of the data (why the design is what it is)

The warehouse is a **game-centric star schema**:

- **`games` is the aggregate root.** Nearly every serving table is one row per
  `game_id`: `analytics.games_active`, `analytics.games_features`,
  `predictions.bgg_predictions`, `analytics.game_similarity_search`,
  `predictions.bgg_game_coordinates`, …
- **Publishers / designers / artists / families / categories / mechanics** are
  `core` **dimension** tables (`id` + `name`) plus `core` **bridge** tables
  (`game_id` ↔ `entity_id`). There are **no independent per-entity fact tables**.
  Their primary operation is therefore *"list the games for this entity"* (a bridge
  join), plus a `game_count` facet. The only per-entity aggregates that exist are the
  truncated `analytics.filter_*` tables — and those cover only publishers, designers,
  categories, mechanics (**not** artists or families).
- **ML outputs** (predictions, embeddings, similarity, coordinates) are materialized
  per-game in `predictions.*` / `analytics.*` — thin dedup wrappers over
  `bgg-predictive-models`'s outputs.
- **Experiments** live in GCS (`bgg-predictive-models` bucket); **monitoring/catalog**
  come from `INFORMATION_SCHEMA`.

This shape dictates the resource model: `games` is primary; entity endpoints are
mostly `→ games` projections.

## Design principles

1. **Warehouse owns the API.** Data lives here; the front-end becomes a consumer.
2. **Readers over routers.** The reusable asset is a query/repository layer, not the
   HTTP shell.
3. **One deployable, many routers** (modular monolith). Uniform light deps, one
   auth/config/deploy.
4. **Serve materialized results only** — no live inference (that stays in
   `bgg-predictive-models`).
5. **Consistent conventions** (URLs, pagination, auth, errors) so a new resource is
   cheap to add.
6. **Read-only.**

## Architecture

### Layering

- **`src/warehouse/readers/`** — one module per resource (`games.py`,
  `publishers.py`, `designers.py`, `predictions.py`, `collections.py`,
  `similarity.py`, `experiments.py`, `monitoring.py`). Pure functions: params in,
  dicts / pydantic models out. **No FastAPI.** Reusable by the API, notebooks, and
  pipelines. This is where dash-viewer's scattered SQL is consolidated and becomes the
  single source of truth.
- **`services/warehouse_api/`** — the FastAPI app. `main.py` mounts one `APIRouter`
  per resource; routers call readers and shape responses. Thin, disposable glue.
  Also `auth.py`, `Dockerfile`, `cloudbuild.yaml`, `pyproject.toml`.
- **Shared** — BigQuery client factory + dataset config (`src/config.py` extended),
  pydantic response models, pagination + error helpers.

### Structure decision: modular monolith, not service-per-entity

One `services/warehouse_api/` deployable with a router per resource — **not**
`services/games/`, `services/publishers/`, … The `bgg-predictive-models` repo split
into four services because each carried heavy, *different* ML dependencies (embedding
models, scoring models) with real isolation value. A read API has uniform, light
dependencies (BigQuery client + FastAPI) over one backend project; splitting would
multiply deploy / auth / cold-start / config for zero isolation benefit. Routers keep
it modular; if one resource ever needs isolation, it lifts out cleanly.

### Resource model (URLs, `/v1` prefix)

- **Games** (aggregate root):
  - `GET /games` — list / filter / sort / paginate (rating, year, complexity,
    publisher/designer/category/mechanic, player count)
  - `GET /games/{id}` — full profile (features + predictions + embedding + provenance)
  - `GET /games/{id}/predictions | /similar | /embedding | /players | /provenance`
  - `GET /games/search?q=` , `GET /games/new` , `GET /games/summary`
- **Entity resources** (dimension + bridge):
  - `GET /publishers` , `GET /publishers/{id}` , `GET /publishers/{id}/games`
  - same for `/designers`, `/artists`, `/families`, `/categories`, `/mechanics`
  - `GET /filters` — combined facet options
- **Predictions:** `GET /predictions` (filters) , `GET /predictions/summary`
- **Collections:** `GET /collections` (usernames) , `GET /collections/{username}`
- **Similarity / embeddings:** `GET /games/{id}/similar` , `POST /similar` (by set) ,
  `GET /embeddings/info`
- **Experiments** (GCS): `GET /experiments` ,
  `GET /experiments/{type}/{name}/{version}` + sub-resources
- **Meta:** `GET /health` , `GET /meta/monitoring` , `GET /meta/catalog`

### Cross-cutting conventions

- **Pagination:** `limit`/`offset` + total count, consistent across list endpoints.
- **Sorting:** `sort_by`/`sort_order` against a per-resource allowlist.
- **Responses:** `{ data, meta }` envelope with pydantic models; `404` on missing id.
- **Auth:** Cloud Run IAM + Google-signed ID token (**not** public); the dash-viewer
  backend attaches the token. Predictions are proprietary.
- **Config:** add `predictions`, `analytics`, `monitoring`, `staging` to
  `config/bigquery.yaml` + `src/config.py` (currently only `core`, `raw`).

### Backends (the reader layer hides these)

- **BigQuery:** games, entities, predictions, collections, similarity (`ML.DISTANCE`
  over `analytics.game_similarity_search`), coordinates.
- **GCS:** experiments (bucket `bgg-predictive-models`, prefix
  `prod/models/experiments`).
- **`INFORMATION_SCHEMA`:** monitoring / catalog.
- Similarity *may* optionally proxy to the predictive-models embeddings service
  (the existing BQ-vs-service abstraction) — default is in-warehouse BigQuery.

## Cost / performance

The Dataform serving tables (`games_features`, `bgg_predictions`,
`game_similarity_search`, …) are **unclustered** — a per-id lookup scans the whole
table. Add `clusterBy: ["game_id"]` to those `.sqlx` models so point-lookups are
cheap. This is a low-risk Dataform change, but a cluster change to an existing table
requires a **full-refresh** (see the dataform-incremental-schema-drift spec). The
`core` base + bridge tables are already clustered by `game_id`, so entity→games joins
are fine; per-publisher/designer lookups scan bridge tables (small, acceptable).

## What needs new modeling (honest gaps)

- **No per-publisher/designer/artist/family fact table.** "Games for entity" is a
  bridge join (fine); richer per-entity stats (avg rating, top games) need new
  Dataform models.
- **`artists` and `families`** are absent from `filter_options_combined` and have no
  list/search surface today — decide whether they're first-class resources.
- **`rankings`** table is declared but unused by any consumer; skip unless needed.

## Rollout (incremental — one router per PR)

1. **App skeleton** — `services/warehouse_api/` + `src/warehouse/readers/` + config +
   auth + deploy + `/health`.
2. **`games` router** (get-by-id + list/search) — replaces dash-viewer game detail +
   search. First real slice; detailed in the games-endpoint spec.
3. **Entity routers** — publishers/designers/categories/mechanics, then artists/families.
4. **`predictions` + `collections` routers.**
5. **`similarity` + coordinates router.**
6. **`experiments` + `monitoring` routers.**

Each PR repoints the corresponding dash-viewer page(s) to the API and deletes the SQL
it replaced (including the inline-in-callback SQL). The front-end ends as a pure
consumer.

## What this doesn't change

- **No live inference** — `bgg-predictive-models` keeps that.
- **No write endpoints.**
- **No new datasets or backfills** in the skeleton; clustering and any per-entity stat
  models are separate, opt-in changes.
- **Dashboard behavior is unchanged** — only each page's data *source* moves.
