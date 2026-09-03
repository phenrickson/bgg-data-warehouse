# Tuned similarity profiles for `analytics.game_neighbors` — Plan

**Date:** 2026-09-03
**Status:** Draft — awaiting answers to the open questions
**Related:** the bgg-viewer `/dev/similar` tuning bench
(`feat/similar-games-explorer`) and its spec
`bgg-viewer/docs/superpowers/specs/2026-09-02-similar-neighbor-diversity-design.md`

## Goal & success criteria

Add the bench's tuning parameters to the precomputed neighbour path so different
use-cases get different similar-games lists, served the way the default one already is
(one clustered `game_neighbors` lookup, no request-time embedding math).

Three profiles, matching what was tuned in the bench:

| profile | sim:rating | band | max/family | floor | sim ≥ | rating pct ≥ |
| --- | --- | --- | --- | --- | --- | --- |
| `default` | 80/20 | ±1.0 | 2 | 100 | — | — |
| `recommended` | 60/40 | none | 1 | 100 | 0.50 | 0.50 |
| `sicko` | 100/0 | none | 1 | 25 *(see Q3)* | — | — |

Success: `GET /games/{id}?profile=recommended` returns a list ranked by the recommended
params; `SELECT profile, COUNT(*) FROM analytics.game_neighbors GROUP BY profile` shows
three; each row carries its own params; with `weight = 1.0` and no cap/floors a profile
reproduces today's `distance ASC` ordering exactly.

## What already exists (reuse, don't rebuild)

- [`definitions/game_neighbors.sqlx`](../../../definitions/game_neighbors.sqlx) — already a
  `PROFILES = [...]` array, `type: "table"` (full `CREATE OR REPLACE` every run),
  `clusterBy: ["profile", "game_id"]`, params carried as row columns. Its own header
  comment: *"add a NEW profile alongside the existing one… extra profiles are effectively
  free (~13s / 72MB per profile)."*
- `game_similarity_search` already carries `geek_rating` (= `bayes_average`),
  `users_rated`, `complexity`, and the 8/16/32/64-d embeddings — everything the blend and
  floors need.
- `core.game_families` + `core.families` are **already declared sources** — the family
  cap needs no `sources.js` change.
- `reader.get_game(game_id, client, profile="default")` — the `profile` arg **already
  exists**; only the router doesn't pass it.
- `_similar_live` (the tuning path) stays exactly as-is and unexposed.

## Scope

**In:** `definitions/game_neighbors.sqlx` (ranking CTEs + profiles),
`services/warehouse_api/routers/games.py` (accept `?profile=`), maybe
`src/warehouse/readers/games.py` (profile allowlist), the two `tests/test_games_*` files.

**Out:**
- Any bgg-viewer change — the `?profile=` toggle, per-user preference, how the client
  chooses. Separate work, explicitly deferred.
- MMR / diversity in production — no profile uses it; it's a sequential greedy pick that
  isn't SQL, and would need a Python pipeline step.
- A reimplementation/expansion cap bucket (`core.game_implementations` /
  `game_expansions`) — family-only for v1; reskins almost always share a `Game:` family.
- Backfill — `game_neighbors` is a full rebuild; nothing to backfill.

## Branching & delivery

Two PRs to `main`, in order (the API can't read profiles the Dataform run hasn't built):

1. **PR A — `feature/similarity-profiles`** (Dataform only). Merge triggers `dataform.yml`
   (`definitions/**`), which rebuilds `game_neighbors` with all three profiles.
2. **PR B — `feature/api-profile-param`** (API only). Merge **after** PR A's Dataform run
   is green and the table is populated. Triggers `deploy-warehouse-api.yml`.

Squash-merge both. No local `gcloud`/`terraform`/deploys. Phil merges.

## Steps

### PR A — Dataform

**1. Rating-percentile CTE + sim:rating blend.**
- `rated_pct AS (SELECT game_id, PERCENT_RANK() OVER (ORDER BY geek_rating) AS geek_pct
  FROM ${ref("game_similarity_search")} WHERE geek_rating > 0)` — global percentile, matches
  the bench.
- In `pairs_*`: `similarity = 1 - ML.DISTANCE(...)`, LEFT JOIN `rated_pct` on the candidate,
  `score = w * similarity + (1 - w) * COALESCE(geek_pct, 0)`.
- Rank by `score DESC` (was `distance ASC`).
- New profile param `weight`. `weight: 1.0` ⇒ `score = similarity` ⇒ identical order to today.
- **Verify:** dry-run cost unchanged for the `weight:1.0` case; resolve the generated SQL
  for game 13 and diff the ordering against the current model — must be identical.

**2. Hard floors.**
- `WHERE similarity >= @min_similarity` and `(@min_rating_pct = 0 OR geek_pct >= @min_rating_pct)`.
- New params `min_similarity` (0–1), `min_rating_pct` (0–1); 0 = off.
- **Verify:** `recommended` (`min_similarity: 0.5`) — a spot-check game's list contains no
  entry with cosine similarity < 0.5; `min_rating_pct: 0.5` — none below the catalogue
  median geek rating.

**3. Per-family cap.**
- `src_fam` / `cand_fam` CTEs — `Game:` / `Series:` family ids per game
  (`${ref("core","game_families")}` ⋈ `${ref("core","families")}`,
  `STARTS_WITH(name,'Game: ') OR STARTS_WITH(name,'Series: ')`).
- Explode ranked pairs to `(src, cand, score, shared_family)` per shared family.
- `fam_rank = ROW_NUMBER() OVER (PARTITION BY src, shared_family ORDER BY score DESC)`.
- Keep a candidate iff `MAX(fam_rank) OVER (PARTITION BY src, cand) <= @max_per_family`;
  candidates sharing no family with the source are always kept.
- Re-rank survivors by `score DESC`, take `top_k`.
- **Known deviation:** this is the *conservative* form of the bench's greedy cap — it drops
  a candidate when ≥ N higher-scoring candidates share one of its families, even if some of
  those were themselves capped out elsewhere. Exact parity needs the Python step (Q2).
- **Verify:** Risk (181) with `max_per_family: 2` — ≤ 2 `Game: Risk` entries, list still
  fills to `top_k`; a no-family game (e.g. a standalone euro) is unaffected.

**4. The three profiles.**
```js
const PROFILES = [
  { name: "default",     weight: 0.8, complexity_band: 1.0,  min_users_rated: 100,
    source_min_users_rated: 0,  min_similarity: 0,   min_rating_pct: 0,
    max_per_family: 2, distance: "COSINE", dims: 64, top_k: 10 },
  { name: "recommended", weight: 0.6, complexity_band: null, min_users_rated: 100,
    source_min_users_rated: 0,  min_similarity: 0.5, min_rating_pct: 0.5,
    max_per_family: 1, distance: "COSINE", dims: 64, top_k: 10 },
  { name: "sicko",       weight: 1.0, complexity_band: null, min_users_rated: 25,
    source_min_users_rated: 25, min_similarity: 0,   min_rating_pct: 0,
    max_per_family: 1, distance: "COSINE", dims: 64, top_k: 10 },
];
```
- `complexity_band: null` ⇒ the template omits the `BETWEEN` clause for that profile.
- Carry every param as a row column (the table stays self-describing).
- **Verify:** `npx @dataform/cli compile` clean; post-run
  `SELECT profile, COUNT(*), ANY_VALUE(weight), ANY_VALUE(max_per_family)
   FROM analytics.game_neighbors GROUP BY profile`.

**5. (Q4) Add `geek_rating` to the `similar` struct.** `STRUCT(game_id, name,
year_published, distance, geek_rating)`. `type: table` ⇒ no full-refresh, picked up next
run. Reader passes it straight through (`[dict(s) for s in row["similar"]]`).

**6. Dry-run the whole generated model — `recommended` and `sicko` especially.** Record
bytes scanned + wall time per profile. If `sicko` is untenable: drop it to `dims: 16`,
raise `source_min_users_rated`, or give it a loose band. (Note: the bench only ever ran
against its floor-30 dev set, so `sicko` at floor ~25 is *closer* to what was actually
tuned than a true floor-0 would be.)

### PR B — Warehouse API

**7. `GET /games/{game_id}` accepts `?profile=`.**
```py
@router.get("/{game_id}")
def get_game(game_id: int, profile: str = "default"):
    return _require(reader.get_game(game_id, profile=profile), game_id)
```
- **Verify:** `tests/test_games_router.py` — `?profile=recommended` reaches
  `reader.get_game`; default still `"default"`.

**8. Profile handling in the reader.** Define `KNOWN_PROFILES` next to `DEFAULT_*` in
`games.py`. Decide unknown-profile behaviour per Q5 (400 vs empty `similar`).
- **Verify:** `tests/test_games_reader.py` — known profile threads to
  `_similar_precomputed`; unknown handled per decision.

## Risks / unknowns / rollback

- **`sicko` build cost** — no band + low floor is the one expensive branch; step 6 gates
  it. Not a one-way door.
- **Family-cap approximation** — differs from the bench's greedy only when the source
  shares *several* large families with a candidate; effect is an occasionally shorter
  list, never a cap violation. Exact parity = Python step (Q2).
- **No schema-migration risk** — `game_neighbors` is `type: table`, rebuilt whole each
  run; new columns / struct fields need no full-refresh (contrast the incremental models).
- **`default` changes for everyone the moment PR A lands** (if we update in place, Q1) —
  before any UI work. Revert = one-line PR; next Dataform run restores it.
- **Rollback:** revert PR A → single `default` profile back. Revert PR B → route stops
  taking `profile`. Independent, both cheap.

## Open questions (need answers before Step 1)

1. **`default`:** update in place, or add the new default params as `default_v2` and
   compare before flipping? (`game_neighbors.sqlx`'s comment prefers alongside; but it's a
   full-rebuild table and the bench is the way back.)
2. **Family cap:** accept the conservative SQL approximation for v1, or hold out for exact
   greedy parity via a Python pipeline step (which is also where MMR would eventually go)?
3. **`sicko` floor:** true 0 (full 128k-candidate cross join — needs step 6 to prove it's
   affordable), or ~25 (matches what the bench actually tuned against)?
4. **`similar` struct:** add `geek_rating` now, or wait until the UI needs it?
5. **Unknown `?profile=`:** 400, or fall through to an empty `similar` list?
