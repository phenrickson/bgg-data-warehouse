# Pool-scoped similarity and collection neighbours — plan

**Spec:** `docs/superpowers/specs/2026-09-24-pool-similarity-and-collection-neighbors-design.md`
**Branch/PR:** each phase is its own branch off `main` and its own PR; Phil merges.
Spec + this plan ride on `docs/pool-similarity-spec`.

## Goal

Phase 1 — `/games/{id}/similar` answers **within a pool**, live, with the same profile
logic as `game_neighbors` (verified to match exactly on 2026-09-24).
Phase 2 — a precomputed per-collection neighbours artifact (parts A, C, D).
Phase 3 — collection-mode profiles (Recommend / Surprise me), tuned against Phase 2 output.

Phase 1 is planned in full. Phases 2–3 are outlined; each needs a decision first.

## What already exists

- `includes/similarity_profiles.js` — three profiles; the one source of parameters.
- `definitions/game_neighbors.sqlx` — the profile logic (`profileQuery`), one BigQuery job
  per profile because the on-demand **CPU ceiling is per job** (see
  `plans/2026-09-03-similarity-profiles.md`).
- `src/warehouse/readers/games.py` — `get_similar`: precomputed by default, `_similar_live`
  when any tuning param is set. `_similar_live` is distance + band + ratings floor only.
- `services/warehouse_api/routers/games.py` — `GET /games/{id}/similar`.
- Tests: `tests/test_games_reader.py` (`RoutingClient`, SQL-string assertions),
  `tests/test_games_router.py`.
- Deploy: `deploy-warehouse-api.yml` on push to `main` touching `src/warehouse/**` or
  `services/warehouse_api/**`. Actions only.
- Nobody calls the tuning params today — bgg-viewer reads `similar_profiles` from the game
  document only. They stay as they are in Phase 1.

## Phase 1 — live, pool-scoped, profile-faithful

### Step 0 — check embedding versions (read-only, needs a yes)

`SELECT embedding_version, COUNT(*) FROM analytics.game_similarity_search GROUP BY 1` —
dry-run first. If more than one version is present, stop and raise it before Phase 1:
every search, precomputed or live, would be mixing vector spaces.

### Step 1 — `analytics.similarity_profiles` table — `feat(dataform): materialize the similarity profiles`

- New `definitions/similarity_profiles.sqlx`, `type: "table"`, built in `js {}` from
  `similarity_profiles.profiles`: one row per profile with every parameter
  (`name, weight, complexity_band, max_per_family, min_similarity, min_rating_pct,
  max_rating_pct, min_users_rated, source_min_users_rated, top_k, dims, distance`).
- `game_neighbors.sqlx` unchanged — it keeps inlining the JS (no rebuild risk).

**Verify:** Dataform compile + `CREATE TABLE` dry-run of the compiled SQL (not a bare
SELECT). Rows equal the JS file.

### Step 2 — pooled live query in the reader — `feat(similar): live pooled search with the profile logic`

- New `_similar_pooled(game_id, pool, client)` in `readers/games.py`: the statement
  measured on 2026-09-24 — `profiles` (from `analytics.similarity_profiles`), global
  `rating_pct`, `src`, `cand` restricted to the pool, score → band / floors / ceiling /
  source floor → product-line cap → top `top_k` — returning **all three profiles** in one
  query, as `{profile: [rows]}` (the game document's `similar_profiles` shape).
- Pool is a small typed value, never interpolated SQL:
  - `collection` — `username` param → `game_id IN (SELECT game_id FROM collections.user_collections WHERE username = @username AND owned)`
  - `year_min` — `year_published >= @year_min`
  - `ids` — `game_id IN UNNEST(@ids)` (capped length)
  - forms combine with AND.
- `get_similar` routing: no pool → unchanged (precomputed, or the existing tuning path).
  Pool given → `_similar_pooled`. Pool + tuning params → 400 (not combined in Phase 1).

**Verify (unit, `RoutingClient`):**
- pooled SQL reads `similarity_profiles`, has the rating blend, percentile floor/ceiling,
  product-line cap, and the pool predicate; no pool → no `ML.DISTANCE`, as today.
- username / year / ids are query parameters, never in the SQL text.
- result is keyed by all three profile names, `[]` for a profile with no rows.

### Step 3 — the route — `feat(api): pool parameters on /games/{id}/similar`

- `GET /games/{id}/similar?collection=<username>&year_min=<y>&ids=<id>&ids=<id>` → pooled.
- Fix the docstring's "same filtering semantics either way" — true for pooled, not for the
  tuning path; say which.

**Verify:** router tests for param pass-through and the 400; `uv run pytest`.

### Step 4 — real-data check (billed, needs a yes)

Against a locally run API (not deployed), the three seeds from the spec with
`collection=phenrickson` and `year_min=2016`: lists equal the 2026-09-24 script output;
record API-inclusive latency. ~7 queries × ~75 MB ≈ $0.004; dry-run totals shown first.

**Phase 1 PR:** Steps 1–3 (Step 1 may go first on its own if the Dataform run should land
before the API deploy — the live query needs the table to exist).

### Risks

- **The API's service account reading `collections.user_collections`.** Recent PRs had to
  authorize that view for warehouse Dataform; the API SA may need the same. Check before
  Step 4; the fix is Terraform, Actions only.
- **Deploy order.** The API's pooled query fails until `analytics.similarity_profiles`
  exists. Land Step 1, let Dataform build it, then merge Steps 2–3.
- **Latency ~3 s.** Accepted for occasional pooled calls; collections don't use this path.
- Rollback: revert the PR; no existing response shape changes.

## Phase 2 — collection neighbours artifact (outline)

**Decide first:** where it's built and served — Dataform table keyed by username + a read
API route, or a GCS artifact on bgg-viewer's rail. And which usernames get built.

Then: one model per part, each its own BigQuery job (CPU ceiling):
- **A** owned → owned, **C** owned → catalog minus owned, **D** C's not-owned games → owned
  minus the original seed. C uses the catalog profiles; A and D use collection-mode ones.
- Measure D's real size at 162 games and at the largest collection before picking a format.

## Phase 3 — collection-mode profiles (outline)

Add Recommend / Surprise me profiles to `similarity_profiles.js`; tune on Phil's collection
against Phase 2 output (rating floors dropped or collection-relative, band loosened,
`min_users_rated` dropped, product-line cap kept). Settle what Surprise me means.

## Out of scope

The viewer UI (own spec), arbitrary-seed collection search, multi-seed queries, changing
the existing tuning path.
