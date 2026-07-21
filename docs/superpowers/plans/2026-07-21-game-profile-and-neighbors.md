# Game Profile & Precomputed Neighbors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `GET /games/{id}` from ~361 MB to ~20 MB per request, and fix the
unfiltered `/similar` defect, by adding two Dataform serving models and repointing the
API at them.

**Spec:** `docs/superpowers/specs/2026-07-21-game-profile-and-neighbors-design.md`

**Tech Stack:** Dataform (BigQuery), Python 3.12, FastAPI, GitHub Actions.

## Scope

**In:** `definitions/game_profile.sqlx`, `definitions/game_neighbors.sqlx`, a clustering
change to `definitions/game_similarity_search.sqlx`, the games reader + router, tests.

**Out of scope:** caching (the residual ~1 s is BigQuery per-query overhead); extra
neighbour profiles per distance metric (deferred); front-end work; any response-contract
change.

## Branching & delivery

**Never commit to `main`.** This lands as **two PRs in order**, because the API cannot
read tables that don't exist yet:

1. **PR A — `feature/game-profile-neighbors`** (Dataform only). Merging triggers
   `dataform.yml` (paths include `definitions/**`), which builds the new tables.
2. **PR B — `feature/api-profile-reads`** (API only). Merge **after** PR A's Dataform
   run has succeeded and the tables are populated. Merging triggers
   `deploy-warehouse-api.yml`.

Both squash-merge to `main`. No local `terraform`/`gcloud`/deploys — everything via
Actions.

---

## PR A — Dataform models

### Task 1: `game_neighbors` (precomputed filtered similarity)

**Files:** Create `definitions/game_neighbors.sqlx`

- [ ] **Step 1: Write the model.** `type: "table"`, `schema: "analytics"`,
  `bigquery: { clusterBy: ["profile", "game_id"] }`. Declare profiles once as a JS array:
  ```js
  const PROFILES = [
    { name: "default", min_users_rated: 100, complexity_band: 0.75,
      distance: "COSINE", dims: 64, top_k: 10 },
  ];
  ```
  Generate one `UNION ALL` branch per profile: filter candidates
  (`users_rated >= min_users_rated`), self-join on the source-relative complexity band,
  rank by `ML.DISTANCE(..., distance)`, keep `top_k`, `ARRAY_AGG(STRUCT(...))`. Carry
  the profile params as columns.
- [ ] **Step 2: Validate the generated SQL by dry-run** before committing — paste the
  resolved query (with `${ref()}` expanded) into a dry run; expect **~72 MB**.
  A large deviation means the filter/join is wrong.
- [ ] **Step 3: Verify semantics** — run it for game 13 and confirm the filtered list
  (Lords of Vegas, Chinatown, CATAN: 3D Edition …) and that **Catan Connect is absent**.
- [ ] **Step 4: Commit** — `feat(dataform): precompute filtered game neighbors`

### Task 2: `game_profile` (one row per game)

**Files:** Create `definitions/game_profile.sqlx`

- [ ] **Step 1: Write the model.** `type: "table"`, `schema: "analytics"`,
  `bigquery: { clusterBy: ["game_id"] }`. Join `games_features` +
  `player_count_recommendations` (as `ARRAY_AGG(STRUCT(...))`) + `bgg_predictions` /
  `game_first_prediction` (STRUCT) + `bgg_game_coordinates` (STRUCT) +
  `fetched_responses` (STRUCT), per the spec's shape. `LEFT JOIN` every optional block so
  a game with no predictions still yields a row.
- [ ] **Step 2: Dry-run the resolved SQL** — expect ~290 MB for a full build.
- [ ] **Step 3: Verify the shape matches the current API response** for game 13
  field-for-field (names, arrays, nested structs).
- [ ] **Step 4: Commit** — `feat(dataform): add game_profile serving model`

### Task 3: Cluster `game_similarity_search`

**Files:** Modify `definitions/game_similarity_search.sqlx`

- [ ] **Step 1: Add** `bigquery: { clusterBy: ["users_rated", "complexity"] }` to the
  config. **Not `game_id`** — the live similarity query filters on `users_rated` and
  `complexity`, never on `game_id` (see spec).
- [ ] **Step 2: Note the refresh requirement in the PR body** — this is an *incremental*
  model and clustering cannot be added in place, so it needs a **full refresh** to take
  effect (see the incremental-schema-drift spec).
- [ ] **Step 3: Commit** — `perf(dataform): cluster similarity search on filter columns`

### Task 4: Open PR A

- [ ] **Step 1: Push + open PR** describing the full-refresh requirement for
  `game_similarity_search`.
- [ ] **Step 2: After merge**, confirm the `dataform.yml` run succeeded and that
  `game_profile` / `game_neighbors` are populated (row counts ≈ 127,645 and 17,258).
- [ ] **Step 3: Trigger the full refresh** for `game_similarity_search` so clustering
  takes effect, then confirm pruning by comparing a filtered query's dry-run estimate
  against bytes **billed** (dry run alone cannot show pruning).

---

## PR B — API repoint

### Task 5: Reader — `similar` from precomputed, live fallback

**Files:** `src/warehouse/readers/games.py`, `tests/test_games_reader.py`

- [ ] **Step 1: Write failing tests.** (a) `get_similar(game_id)` with no params reads
  `game_neighbors` and filters `profile='default'`; (b) passing any tuning parameter
  (band/metric/min_ratings/dims/n) routes to the **live** query against
  `game_similarity_search`; (c) the live query applies the `users_rated` floor **and**
  the source-relative complexity band — the defect regression test.
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement** — `get_similar(game_id, *, profile="default", band=None,
  metric=None, min_ratings=None, dims=None, n=None)`; precomputed path when all tuning
  params are `None`, else live filtered query.
- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Commit** — `fix(api): serve filtered neighbors; live query when tuned`

### Task 6: Reader — `get_game` from `game_profile`

**Files:** `src/warehouse/readers/games.py`, `tests/test_games_reader.py`

- [ ] **Step 1: Write failing tests** — `get_game` issues **one** query against
  `game_profile` and returns the same document shape (same top-level keys, features with
  `player_counts`).
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement** — single read; unpack nested structs/arrays into the existing
  response shape. Keep the per-block readers for the sub-resource endpoints.
- [ ] **Step 4: Run, watch pass** (the concurrency timing test becomes moot for
  `get_game` — remove or retarget it rather than leaving it asserting stale behaviour).
- [ ] **Step 5: Commit** — `perf(api): serve get_game from game_profile in one query`

### Task 7: Router + PR B

**Files:** `services/warehouse_api/routers/games.py`, `tests/test_games_router.py`

- [ ] **Step 1: Tests** — `/games/{id}/similar` accepts `band`, `metric`, `min_ratings`,
  `dims`, `n`, `profile`, and passes them through; bare call hits the precomputed path.
- [ ] **Step 2: Implement**, run full suite `-m "not integration"`.
- [ ] **Step 3: Commit + open PR B.**
- [ ] **Step 4: After merge**, re-measure live: bytes/request vs the 361 MB baseline and
  wall-clock vs ~1.2 s. **Record both.**

---

## Risks / rollback

- **`game_similarity_search` full refresh** is the only destructive-ish step: it rebuilds
  a serving table the live API reads. The API's `/similar` keeps working throughout
  (same schema, only clustering changes), but the rebuild window is real.
- **Ordering:** PR B before PR A = the API queries non-existent tables → 5xx. Enforced by
  the two-PR sequence.
- **Shape drift:** if `game_profile` doesn't reproduce the current document exactly, the
  response contract silently changes. Task 2 Step 3 is the guard.
- **Rollback:** PR B is pure code — revert and redeploy. PR A's new tables are additive
  (dropping them affects nothing until PR B lands); the clustering change reverts by
  removing `clusterBy` and refreshing again.
