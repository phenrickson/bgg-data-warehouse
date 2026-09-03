# Tuned similarity profiles for `analytics.game_neighbors` — Plan

**Date:** 2026-09-03 (rewritten — the first draft predated the product-line-cap design)
**Status:** Draft — awaiting go-ahead
**Bench:** bgg-viewer `feat/similar-games-explorer`. The profile values come from the
bench's "copy profiles" button; the product-line logic below is a port of
`bgg-viewer/src/lib/server/similar-explorer/build.ts`.

## Goal & success criteria

Serve four similarity profiles from the precomputed path so different use-cases get
different similar-games lists. Values, from the bench:

| profile | weight (sim:rating) | band | max/line | floor | sim ≥ | rating pct ≥ | top_k |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `similar` | 1.00 | ±0.75 | — | 100 | 0.50 | — | 12 |
| `sicko` | 0.70 | none | 1 | 30 | — | — | 10 |
| `recommender` | 0.54 | ±0.75 | 1 | 100 | 0.50 | 0.75 | 10 |
| `default` | 0.80 | ±1.0 | 1 | 100 | 0.50 | — | 10 |

(Values as of the re-copied `includes/similarity_profiles.js`.) All four:
`source_min_users_rated: 0`, `dims: 64`, `distance: COSINE`.

Success:
- `SELECT profile, COUNT(*) FROM analytics.game_neighbors GROUP BY profile` → 4 rows.
- `GET /games/{id}?profile=recommender` returns a list ranked by those params.
- `GET /games/{id}` (no param) is unchanged — still the `default` profile.
- Spot checks hold: Take Time → ≤1 Unlock; TI4 → ≤1 other TI; Risk → ≤1 other Risk;
  a niche game still gets a non-empty list.

## What already exists (reuse)

- [`definitions/game_neighbors.sqlx`](../../../definitions/game_neighbors.sqlx) — the
  `type: table` (full `CREATE OR REPLACE` each run), `clusterBy: ["profile","game_id"]`,
  per-profile `UNION ALL` skeleton. Currently one hard-coded `default` profile.
- `game_similarity_search` carries `embedding`, `geek_rating` (= `bayes_average`),
  `users_rated`, `complexity`, `name`, `year_published` — everything the blend, the
  floors, and the band need.
- `core.game_families` + `core.families` are already declared sources.
  `core.game_implementations` + `core.game_expansions` **exist** but are **not**
  declared — needed for product-line propagation.
- `reader.get_game(game_id, client, profile="default")` — the `profile` arg is already
  there; only the router doesn't pass it. `_similar_precomputed` already does
  `WHERE profile = @profile AND game_id = @game_id`.
- `includes/similarity_profiles.js` — the exported profile array, already committed on
  `feature/similarity-profiles` (from `9f69804`); it needs re-copying from the bench
  once the current experiments are final, then it's the single source of truth.

## Scope

**In:**
- `definitions/sources.js` — declare `core.game_implementations`, `core.game_expansions`.
- `definitions/game_product_line.sqlx` — **new** `type: table` model, one row per game.
- `definitions/game_neighbors.sqlx` — read `similarity_profiles.profiles`; add the
  rating-percentile blend, the sim/rating floors, and the per-product-line cap.
- `.github/workflows/dataform.yml` — add `includes/**` to the `push` path filter.
- `services/warehouse_api/routers/games.py` — `GET /games/{id}` accepts `?profile=`.
- `src/warehouse/readers/games.py` — a `KNOWN_PROFILES` allow-list; 400 on unknown.
- `tests/test_games_router.py`, `tests/test_games_reader.py`.

**Out:**
- Any bgg-viewer change — the `?profile=` toggle / per-user preference / how the client
  chooses. Separate follow-up.
- MMR / diversity (dropped in the bench — not SQL-expressible).
- The "exclude shared title words" knob (bench-only, no SQL equivalent — the export
  header already notes it was dropped).
- The "cap editions of the same game" idea still under discussion in the bench — not
  in these profiles, not this PR.
- Backfill — `game_neighbors` and `game_product_line` are full rebuilds.

## Branching & delivery

Two PRs to `main`, in order (the API can't read profiles Dataform hasn't built):

1. **PR 1 — `feature/similarity-profiles`** (Dataform + workflow). Merge triggers
   `dataform.yml` (now also on `includes/**`), which builds `game_product_line` then
   rebuilds `game_neighbors` with all four profiles.
2. **PR 2 — `feature/api-profile-param`** (warehouse API). Merge **after** PR 1's
   Dataform run is green and `game_neighbors` has 4 profiles. Triggers
   `deploy-warehouse-api.yml`.

Squash-merge both. No local `gcloud`/`terraform`/`bq load`. Phil merges.

## Steps

### PR 1 — Dataform

**1. `sources.js` — declare the two bridge tables.**
```js
["game_implementations", "game_expansions"].forEach((t) =>
  declare({ schema: "core", name: t })
);
```
- **Verify:** `npx @dataform/cli compile` resolves; nothing else references them yet.

**2. `definitions/game_product_line.sqlx` — one product line per game.**
`type: "table"`, `schema: "analytics"`, `clusterBy: ["game_id"]`. Port `build.ts`:
- `fam_size` — `COUNT(*)` per `family_id` over `core.game_families`.
- `gs_families` — each game's `Game:`/`Series:` families with `family_size`,
  `is_game_family`, and `name_match` (`STRPOS(LOWER(gfeat.name), LOWER(core)) > 0` where
  `core` is the family name minus the `Game: `/`Series: ` prefix and a trailing
  ` (Publisher)`, length ≥ 3; joins `${ref("games_features")}` for the title).
- `product_line_base` — `ARRAY_AGG(family_id ORDER BY name_match DESC, is_game_family
  DESC, family_size ASC, family_id ASC LIMIT 1)[OFFSET(0)]` per game.
- `product_line_inherited` — a family-less game (`game_id NOT IN product_line_base`)
  takes the line of a `${ref("game_implementations")}`/`${ref("game_expansions")}`
  neighbour that has one (one hop, tightest wins).
- Output: `game_id, product_line_id, product_line_name` (join `core.families` for the
  name — handy for debugging and a future UI).
- **Verify:** dry-run (~80 MB, per the bench). `bq query`:
  `Take Time → NULL`; all `Unlock!%` → `Series: Unlock!`; Irish/Iberian Gauge + Ride
  the Rails → `Series: Iron Rail`; TI editions + Rex → `Game: Twilight Imperium`;
  Summoner Wars incl. Second Edition → `Game: Summoner Wars`.

**3. `game_neighbors.sqlx` — read the include + the three new mechanics.**
- `js{}`: `const PROFILES = similarity_profiles.profiles;` (keep `embeddingColumn`).
- `rating_pct` CTE — `PERCENT_RANK() OVER (ORDER BY geek_rating)` over
  `game_similarity_search WHERE geek_rating > 0` (global percentile, matches the bench).
- Per profile: `pairs_*` gains `similarity = 1 - ML.DISTANCE(...)`, a LEFT JOIN to
  `rating_pct` (`nbr_geek_pct = COALESCE(geek_pct, 0)`), and a LEFT JOIN to
  `game_product_line` (`nbr_product_line`). The band clause is emitted only when
  `complexity_band` is non-null.
- `scored_*` — `score = weight*similarity + (1-weight)*nbr_geek_pct`;
  `WHERE similarity >= min_similarity AND (min_rating_pct = 0 OR nbr_geek_pct >= min_rating_pct)`.
- When `max_per_family` is non-null: `line_rn = IF(nbr_product_line IS NULL, 1,
  ROW_NUMBER() OVER (PARTITION BY src_game_id, nbr_product_line ORDER BY score DESC))`,
  keep `line_rn <= max_per_family`.
- `ranked_*` — `ROW_NUMBER() OVER (PARTITION BY src_game_id ORDER BY score DESC)`,
  `ARRAY_AGG(STRUCT(game_id, name, year_published, distance) ORDER BY score DESC)`,
  `WHERE rn <= top_k`.
- Carry `weight, min_similarity, min_rating_pct, max_per_family` as columns (all
  `CAST(... AS FLOAT64/INT64)` so the `UNION ALL` types line up; `NULL`s cast too).
- The `similar` struct is **unchanged** (`game_id, name, year_published, distance`) —
  no reader/viewer change. `geek_rating` in the struct is a separate later call.
- **Verify:** resolve the generated SQL for one profile, paste into a `CREATE TABLE`
  dry-run (not a bare `SELECT` — catches `ref()`/duplicate-field errors). For
  `weight: 1, band, no cap, no floors` the ordering must match the current
  `distance ASC` output exactly (regression on today's `default`). Dry-run the whole
  model — record bytes + wall time, especially `similar` (no band).

**4. `dataform.yml` — watch `includes/`.** Add `- 'includes/**'` under `push: paths:`.
- **Verify:** the diff; without it an includes-only edit wouldn't trigger a run.

**5. Merge PR 1 → confirm the Dataform run.**
- `SELECT profile, COUNT(*), ANY_VALUE(weight), ANY_VALUE(max_per_family)
   FROM analytics.game_neighbors GROUP BY profile` → 4 rows, right params.
- Spot-check `similar` for game 13 vs the pre-change `default` — same list (both are
  pure-distance, floor 100; `similar` just drops the band).
- `Take Time` `default`: `SELECT similar FROM game_neighbors WHERE profile='default'
   AND game_id=<take-time-id>` → at most one Unlock.

### PR 2 — Warehouse API

**6. `routers/games.py` — `GET /{game_id}` takes `?profile=`.**
```py
@router.get("/{game_id}")
def get_game(game_id: int, profile: str = "default"):
    return _require(reader.get_game(game_id, profile=profile), game_id)
```

**7. `readers/games.py` — validate the profile.**
`KNOWN_PROFILES = {"default", "recommender", "sicko", "similar"}` next to the
`DEFAULT_*` constants; `get_game` / `get_similar` raise `ValueError` on an unknown
profile (router maps `ValueError → 400`, the pattern already used for bad metric/dims).
- **Verify:** `tests/test_games_router.py` — `?profile=recommender` reaches the reader;
  `?profile=nope` → 400; no param → `default`. `tests/test_games_reader.py` —
  `_similar_precomputed` gets the profile param; unknown raises.

**8. Merge PR 2 after PR 1's tables are populated.**

## Risks / unknowns / rollback

- **`similar` build cost** — the one no-band profile: 128k sources × ~17k floor-100
  candidates ≈ 2.2e9 `ML.DISTANCE` calls. The current banded model is ~13 s / profile;
  expect `similar` ≈ 20–30 s and the 4-profile rebuild ≈ 2–3 min. Step 3's dry-run
  gates it; if it's untenable, give `similar` `dims: 16` or a loose band.
- **`default` returns < 10 for sparse games** — `min_similarity: 0.5` + `max_per_family: 1`
  is a real behaviour change from today's "always 10". Upcoming/niche games (which get
  lists because `source_min_users_rated: 0`) are the ones most likely to come back
  short. Intended per the bench tuning, but the game page should tolerate a 3-item list.
- **No schema-migration risk** — both models are `type: table`, rebuilt whole each run;
  new columns need no full-refresh (unlike the incremental models — see the
  `dataform-incremental-schema-drift` note).
- **Cross-repo drift** — `game_product_line.sqlx` is a hand-port of `build.ts`. They can
  diverge. Mitigation: the plan links them and the spot-check list is the same; a
  shared definition isn't worth a cross-repo mechanism for ~40 lines of SQL.
- **`similar` struct unchanged** — deliberately not adding `geek_rating`, so no reader
  or bgg-viewer change is forced. If the eventual card wants a rating badge that's a
  separate, additive change.
- **Rollback** — revert PR 1 → next Dataform run restores the single `default` profile
  and drops `game_product_line` (nothing else refs it). Revert PR 2 → route stops
  taking `?profile=`. Independent, both cheap.

## Resolved

1. `includes/similarity_profiles.js` re-copied — the table above is the shipping set.
2. `min_similarity` on `default` (and others) is intended — short lists for niche games
   are acceptable.
3. Unknown `?profile=` → **400** (consistent with the sub-endpoint's bad-metric path;
   fails loud where the only caller is app code).

## Extra risk found while building

- **`sicko` is the expensive build** now, not `similar`: `sicko` has no band and a
  floor of 30 (~40k candidates × 128k sources ≈ 5e9 `ML.DISTANCE`). `similar` gained a
  ±0.75 band and is cheap. Step 5's Dataform run is the real timing check; if `sicko`
  drags, give it `dims: 16` or a loose band in the include and re-copy.
- **Dataform includes-global access** — `game_neighbors.sqlx` assumes
  `includes/similarity_profiles.js` is exposed as the global `similarity_profiles`
  (the documented Dataform-3.0 pattern). The workflow's compile step verifies it
  before any execution; a failure there is a fast, cheap fix.

## Validation done (pre-merge)

- `game_product_line.sqlx` — `CREATE TABLE` dry-run OK (~9 MB); `bq query` spot-check:
  Irish/Iberian Gauge + Ride the Rails → `Series: Iron Rail`; all `Unlock!%` (incl.
  Game Adventures, Fifth Avenue) → `Series: Unlock!`; TI4 / Summoner Wars (Second Ed.)
  / Catan / Risk correct; 1830 → `Series: 18xx`; Take Time → no row.
- `game_neighbors.sqlx` — resolves; combined `CREATE TABLE` dry-run (product-line
  inlined as a CTE) validates, ~83 MB read estimate. Real wall time is the Dataform run.
