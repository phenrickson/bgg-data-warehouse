# Game Profile & Precomputed Neighbors

## Problem

Two things are wrong with how the warehouse read API serves a game.

**1. Cost.** `GET /games/{id}` fans out to six queries across ~7 tables and scans
**~361 MB per request**. None of the serving tables are clustered, so every point-lookup
reads every row. At 361 MB you exhaust the 1 TiB/month free tier in **~2,900 requests**
(under 100/day); 50k requests/month would cost ~**$81**.

**2. A correctness defect.** `GET /games/{id}/similar` is **unfiltered** — raw
`ML.DISTANCE` with no `users_rated` floor and no complexity band. It does not match the
semantics the front-end actually uses, and surfaces near-unrated obscure games:

| Unfiltered (API today) | Filtered (front-end's real config) |
|---|---|
| Catan Connect (0.070) | Lords of Vegas (0.191) |
| Loot the World (0.122) | Chinatown (0.198) |
| Springsign (0.167) | CATAN: 3D Edition (0.199) |

The live front-end applies `users_rated >= 100` **and** complexity within ±0.75 of the
source game, **before** ranking. A global unfiltered neighbour list can't reproduce that.

## Measurements (all verified, not estimated)

- Today: 361 MB/request; ~1.2 s latency (after the concurrency slice).
- **BigQuery bills a 10 MB minimum per table referenced** — so a 7-table fan-out can
  never go below ~70 MB, however well clustered. Fewer tables is worth more than
  fewer columns.
- Corpus: **127,645** games with embeddings (64-d, plus 8/16/32-d reductions);
  **17,258** have `users_rated >= 100`, all with complexity.
- **Filtered precompute of neighbours for all 17,258 games: 13.1 s, 72.4 MB, $0.0003.**
- Unfiltered all-pairs (127,645² = 16.3 B) **fails** on resource limits after ~6 min.
- Clustering is real: a clustered table pruned **85.8%** (606 MB → 86 MB billed);
  unclustered tables pruned 0%.
- Latency floor is BigQuery per-query overhead (~0.9 s), *not* bytes — a clustered
  86 MB query took longer than an unclustered 216 MB one. **This work is about cost,
  not latency.**

## Solution

Three data changes plus an API change.

### 1. `analytics.game_profile` — one row per game

Collapses the four cheap-to-join blocks into a single clustered row, so `get_game`
becomes **one** query (~10 MB) instead of six (~361 MB). Multi-row blocks become
nested arrays; single-row blocks become structs.

```
analytics.game_profile                      -- clustered by game_id
  game_id INT64
  name, year_published, bayes_average, average_rating, average_weight, users_rated,
  min_players, max_players, min_playtime, max_playtime, min_age,
  image, thumbnail, description
  categories, mechanics, publishers, designers, artists, families  ARRAY<STRING>
  player_counts ARRAY<STRUCT<player_count STRING, best_votes INT64,
                             recommended_votes INT64, not_recommended_votes INT64,
                             total_votes INT64, best_percentage FLOAT64,
                             recommended_percentage FLOAT64>>
  predictions   STRUCT<predicted_hurdle_prob FLOAT64, predicted_complexity FLOAT64,
                       predicted_rating FLOAT64, predicted_users_rated FLOAT64,
                       predicted_geek_rating FLOAT64, score_ts TIMESTAMP,
                       first_prediction_ts TIMESTAMP>
  embedding     STRUCT<umap_1 FLOAT64, umap_2 FLOAT64, pca_1 FLOAT64, pca_2 FLOAT64,
                       embedding_model STRING, embedding_version INT64>
  provenance    STRUCT<fetch_timestamp TIMESTAMP, fetch_status STRING>
```

This is deliberately **the same shape as the existing API response**, so structs/arrays
serialize straight through and the response contract does not change.

`similar` is **not** in the profile: neighbour semantics are source-relative and
user-tunable (below).

### 2. `analytics.game_neighbors` — precomputed *filtered* neighbours

```
analytics.game_neighbors                    -- clustered by (profile, game_id)
  profile STRING                            -- 'default'
  game_id INT64
  similar ARRAY<STRUCT<game_id INT64, name STRING,
                       year_published FLOAT64, distance FLOAT64>>
  min_users_rated INT64, complexity_band FLOAT64,
  distance_type STRING, embedding_dims INT64, computed_ts TIMESTAMP
```

Parameters are carried **on the row** so the table is self-describing and the API never
hardcodes an assumption about what "default" means.

Profiles are declared once as a JS array in the `.sqlx` (Dataform templates JS):

```js
const PROFILES = [
  { name: "default", min_users_rated: 100, complexity_band: 0.75,
    distance: "COSINE", dims: 64, top_k: 10 },
];
```

**Tuning workflow:** add a second profile alongside the current one (~13 s each),
compare on real games, and flip the default only once satisfied — rather than mutating
the live default in place. That is what the `profile` column buys.

### 3. Clustering — on the columns actually filtered

| Table | Cluster by | Why |
|---|---|---|
| `game_profile` | `game_id` | point lookup by id |
| `game_neighbors` | `profile, game_id` | point lookup by id within a profile |
| `game_similarity_search` | **`users_rated, complexity`** | the live query filters on *these*, never on `game_id` |

The third is the non-obvious one. Clustering the similarity table by `game_id` would
buy **nothing** — the live query filters by `users_rated` and `complexity` and then
ranks every survivor. `users_rated >= 100` is applied on essentially every query and
alone cuts 127,645 → 17,258 (86%), so it is the right prefix key; `complexity` second
serves the band tweak. Expected: live similarity **70 MB → ~10–15 MB**.

### 4. API behaviour

- `GET /games/{id}` → single read of `game_profile`.
- `GET /games/{id}/similar` (no params) → `game_neighbors`, `profile='default'` —
  **fixes the unfiltered defect**.
- `GET /games/{id}/similar?band=…&metric=…&min_ratings=…&dims=…&n=…` → any parameter
  present falls through to the **live** filtered query against
  `game_similarity_search`.

Same semantics either way: the precomputed table is a materialized cache of one
parameter set, not a different feature. This matches the intended front-end UX —
precomputed default on page load, live endpoint when the user tweaks.

**What is and isn't precomputable:** `distance_type` is discrete (3 values) and could
become extra profiles; the **complexity band is a continuous slider** and can never be
fully precomputed — it must stay live, which is exactly why clustering on `complexity`
matters. Dimension tweaks are already cheap via `embedding_8/16/32` (8× less data).

## Refresh strategy

Both new models are built as **full-rebuild tables**, not incremental — they join
several upstream tables whose change-detection would otherwise need bespoke logic, and
the rebuild is trivially cheap:

- `game_profile`: ~290 MB per build
- `game_neighbors`: ~72 MB per build (13 s)

At a few Dataform runs/day that's well inside the free tier — and it replaces **361 MB
on every request**. One rebuild costs about as much as one request does today.

## Expected outcome

| | Per request | Free-tier ceiling | 50k req/mo |
|---|---:|---:|---:|
| Today | 361 MB | ~2,900 | $81 |
| **After** | **~20 MB** | **~50,000** | **~$0** |

Latency is expected to stay ~1 s (the BigQuery per-query floor), though `get_game`
drops from six concurrent queries to one.

## Validation

- Unit tests (mocked BigQuery) for the new readers.
- `game_profile` row for game 13 matches today's composed document field-for-field.
- `game_neighbors` for game 13 reproduces the filtered list (Lords of Vegas, Chinatown,
  CATAN: 3D Edition …) — and specifically **no longer** returns Catan Connect.
- Post-deploy: re-measure bytes/request against the 361 MB baseline.
- Confirm the clustered similarity table actually prunes (compare dry-run estimate vs
  bytes *billed* — dry run alone cannot show pruning).

## What this doesn't change

- **No response-contract change** — the profile is shaped to the existing JSON.
- No live inference; no write endpoints.
- The `raw`/`core` pipeline, its schedules, and Dataform's existing models are untouched
  apart from the clustering change to `game_similarity_search`.

## Risks

- **`game_similarity_search` clustering needs a full refresh** — clustering can't be
  added to an existing table in place (see the incremental-schema-drift spec). It is an
  incremental model, so this is a real rebuild of a serving table the live API reads.
- **Staleness** is bounded by the Dataform cadence, which already gates everything else.
- **Dropping to one query** removes the natural per-block isolation: a schema change in
  any source now breaks one model rather than one endpoint. Mitigated by the profile
  being a plain rebuild.

## Deferred

- Extra `game_neighbors` profiles per distance metric (cheap; add when the front-end
  wants them).
- Caching in front of the API — the remaining ~1 s is BigQuery per-query overhead, and
  only a cache removes that.
