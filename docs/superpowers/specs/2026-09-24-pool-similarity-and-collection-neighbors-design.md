# Pool-scoped similarity and collection neighbours — design

**Date:** 2026-09-24
**Status:** Design agreed in outline; collection-mode parameters to be tuned
**For:** the bgg-viewer "Recommend" tool (Tools menu). The viewer UI gets its own spec.
**Builds on:** `2026-07-21-game-profile-and-neighbors-design.md`

## Problem

`analytics.game_neighbors` answers "games like X" across the whole catalog, in three
styles (`similar`, `recommender`, `sicko`), precomputed. It cannot answer the same
question **inside a pool**: "of the games in my collection, which are like X", or "like X,
among games from the last ten years". A pool's best matches are rarely in X's global top
10, so filtering the stored list after the fact comes back short or empty.

The read API's live path (`_similar_live`) does not fill the gap. It ranks by distance with
a complexity band and a ratings floor only — it ignores `profile`, so it has no rating
blend, no percentile floors or ceilings, no similarity floor and no product-line cap. The
`/games/{id}/similar` docstring's "same filtering semantics either way" is not true today.

## What was measured (2026-09-24)

A live query carrying the full `game_neighbors.sqlx` profile logic for one seed, with the
candidate set limited to a pool, all three profiles in one query. Seeds: Wingspan, Brass:
Birmingham, Take 5. Pools: unrestricted, phenrickson's owned collection (162 games), and
`year_published >= 2016`.

- **Faithful.** Unrestricted, all 9 (seed × profile) lists matched `game_neighbors`
  exactly — same games, same order.
- **Cost.** 75.5 MB billed per query regardless of seed, pool or `top_k` (the table is
  unclustered by design and the global rating percentile reads all of it): ~$0.0005/call.
- **Speed.** 2.8–3.3 s per query in BigQuery alone (one 4.7 s first-run outlier), before
  any API hop or Cloud Run cold start.
- **Profiles degrade on a collection pool.** `sicko` returned 0 for every seed (its
  below-80th-percentile ceiling excludes a well-rated shelf); `recommender` returned the
  same list as `similar`; Wingspan and Take 5 came back short (7 and 4) from the complexity
  band and similarity floor. On the last-ten-years pool all three filled to 10.

Script: kept out of the repo; the query shape is reproduced in *Live search* below.

## Decision

Three pieces.

### 1. One similarity endpoint: precomputed by default, live when the request leaves it

`/games/{id}/similar` keeps serving `game_neighbors` for the requests it covers — a seed, a
named profile, the whole catalog. It goes live only when the request moves outside what is
precomputed; the first such parameter is a **pool**.

The live search runs the **same profile logic** as `game_neighbors` (verified above),
not the current distance-and-band approximation. Profile is a parameter in both modes, so
the style can change live too.

Pool forms, first cut:
- `pool=collection:<username>` — owned games in `collections.user_collections`
- scope-style filters (e.g. `year_min`), mirroring the fields the viewer's `Scope` already has
- an explicit ID list, for anything the viewer resolves client-side

Live calls cost ~$0.0005 and take ~3 s in BigQuery. That is acceptable for an occasional
"change the pool" request and is the reason collections do **not** go through it.

### 2. Collection neighbours: a precomputed per-collection artifact

For collections the tool never searches live. A build produces, per username, the profile
results the tool needs, and the browser loads it once and answers every step locally.

It holds three parts:

| Part | Seeds | Pool | Answers | Rough size |
| --- | --- | --- | --- | --- |
| **A. owned → owned** | the collection | the collection | "what else on my shelf is like this game I own" | ~5k entries |
| **C. owned → not owned** | the collection | catalog minus the collection | "what's out there like it, that I don't have" | ~5k entries |
| **D. near-miss → owned** | the not-owned games appearing in C | the collection, minus the original seed | "the closest thing I don't own — what on my shelf is most like *that*" | ≤ ~150k entries |

Sizes are estimates for a 162-game collection (× styles × top 10), not measurements. D is
bounded because its seeds come from C, not from the whole catalog — that is what makes the
Brass → Brass: Pittsburgh → back-to-my-shelf walk precomputable.

**Not covered:** an arbitrary seed unrelated to the collection ("I played X at a friend's —
what do I own like it?"). That goes to the live endpoint with `pool=collection:<username>`,
or is left unsupported for now.

C uses the existing profiles (it is the ordinary search with owned games removed). A and D
use **collection-mode** parameters (below).

### 3. Collection-mode parameters

On a collection pool the catalog profiles collapse (see measurements), so A and D get their
own. The tool presents two modes:

- **Recommend** — the best-match style. Likely absorbs the similar / recommender split,
  which is one list on a shelf.
- **Surprise me** — good matches you would not have picked. May mean Dark-Horses-like
  (lesser-known, or further in weight) and/or randomized from a good-enough set. To be
  settled against real output.

Directions to tune, not decisions:
- rating floors/ceilings: drop, or compute the percentile **within the collection**
- complexity band: loosen or drop so a small pool does not empty
- similarity floor: lower, or "always the closest N"
- `min_users_rated`: drop — an owned game belongs in the pool however obscure
- product-line cap: probably keep

The collection-mode profiles live in `includes/similarity_profiles.js` alongside the
catalog ones, so the precomputed table, the collection build and the live endpoint all read
one source.

## Shared profile definition

`similarity_profiles.js` is read by Dataform; the read API is Python. So the live endpoint
does not copy the parameters — Dataform also materializes them as a small table
(`analytics.similarity_profiles`, one row per profile) and the live query joins it. That is
also how the measured query carried all three profiles in one statement.

## Live search (shape)

Per request, one statement:

- `profiles` — rows from `analytics.similarity_profiles`
- `rating_pct` — global geek-rating `PERCENT_RANK` (unchanged from `game_neighbors`)
- `src` — the one seed
- `cand` — `game_similarity_search` restricted to the pool
- pairs → score (`weight · similarity + (1 − weight) · rating percentile`) → band,
  similarity floor, percentile floor/ceiling, source ratings floor → product-line cap →
  top `top_k` per profile

## Open questions

1. **Where the collection artifact is built and served.** A Dataform table keyed by username
   plus a read-API route, or a GCS artifact on the rail bgg-viewer already uses for the
   catalog. Rebuild trigger: the collection sync.
2. **Which collections get built.** Every username in `user_collections`, or only those
   that ask.
3. **D's actual size** at 162 games and at a large collection — measure before choosing a
   format.
4. **Surprise me** — which of its readings, settled against real output.
5. **Stale embeddings outlive the null-year exclusion** (cleanup, not blocking). Checked
   2026-09-24: `game_similarity_search` holds 129,261 rows on `embedding_version` 6 and 11
   on version 5. Example: 398331 (a real game, "Pond", 152 ratings) now has
   `year_published = NULL` in `games_features`; the embedding pipeline's change detection
   and loader both require a year, so it is never re-embedded, and its v5 row persists
   through the incremental merges into `game_neighbors`. The catalog also still includes it
   (its working set has no year condition). Fix to scope later — e.g. drop embeddings for
   games the pipeline now excludes, and/or filter candidates to the latest version.

## Not doing

- Serving collections from the live search.
- Arbitrary-seed collection search in the artifact.
- The viewer UI — its own spec, once the endpoint and artifact shapes are settled.
- Multi-seed ("I like these three") — the live path could average vectors later.
