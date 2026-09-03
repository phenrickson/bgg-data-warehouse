# Game document carries all three similar-profile lists — Plan

**Date:** 2026-09-03
**Driver:** bgg-viewer spec `docs/superpowers/specs/2026-09-03-similar-profile-switcher-design.md`
(in the bgg-viewer repo). That feature needs the game detail page to switch between the
`similar` / `recommender` / `sicko` neighbour lists client-side, with no refetch — so the
document has to carry all three, not one selected by `?profile=`.
**Branch:** `feat/similar-doc-all-profiles`. PR to `main`; merge auto-deploys the
warehouse API. The `game_neighbors` re-cluster lands on the next scheduled Run Dataform.

## Goal & success criteria

`GET /games/{id}` returns `similar_profiles: { similar: [...], recommender: [...],
sicko: [...] }` — every key always present, `[]` when a profile has no row for that game.
Top-level `similar` stays (mirrors the `?profile=`-selected list) for back-compat. The
all-profiles read is no more expensive than the old single-profile read.

## Changes

| File | Change |
| --- | --- |
| `src/warehouse/readers/games.py` | `PROFILE_NAMES` tuple (ordered) → `KNOWN_PROFILES`. New `_similar_all_profiles(game_id, client)` — one `WHERE game_id =` lookup, `{profile: rows}` with `[]` for missing. `get_game` runs it (concurrently with `_profile_row`, as before), returns `similar_profiles` + `similar = similar_profiles[profile]`. |
| `services/warehouse_api/routers/games.py` | Docstring only — response flows through untyped. |
| `tests/test_games_reader.py` | `NEIGHBOR_ROWS` fixture (sicko absent); shape test gains `similar_profiles`; two new tests (every key present with `[]` for absent; top-level `similar` mirrors `?profile=`). |
| `definitions/game_neighbors.sqlx` | `CLUSTER BY profile, game_id` → `game_id, profile` + a `DROP TABLE IF EXISTS` before the `CREATE OR REPLACE` (BigQuery won't re-cluster in place). |

## Why the re-cluster

Measured on `game_id = 13` (bq dry-run + real run):

| Read | Before | After (expected) |
| --- | --- | --- |
| one profile (`profile = 'similar' AND game_id = 13`) | 58 MB billed | ~same (game_id still selective) |
| all profiles (`game_id = 13`) | **127 MB billed** | ~one block, well under the 10 MB floor |

With `profile` as the leading cluster key, a filter on `game_id` alone can't prune, so
the all-profiles read scans essentially the whole `similar` column (127 MB). `game_id`
first fixes that and doesn't hurt the single-profile read. The table is rebuilt whole
every Dataform run, so the `DROP` + re-cluster is a one-time free transition.

## Verification

- `uv run --extra test python -m pytest tests/test_games_reader.py tests/test_games_router.py` — 34 pass.
- `npx @dataform/cli compile` — 24 actions, `game_neighbors` compiles to 4 statements (DROP, CREATE, INSERT, INSERT).
- `bq query --dry_run` on the compiled CREATE + both INSERTs (retargeted to a scratch name) — all validate.
- Post-merge: confirm `GET /games/13` returns `similar_profiles`; after the next Run Dataform, re-check the all-profiles scan bytes.

## Risks / rollback

- **Deploy skew:** API deploys on merge; the re-cluster waits for the next Run Dataform,
  so all-profiles reads stay at 127 MB until then. Temporary, cheap, self-heals. Trigger
  Run Dataform manually after merge to close the window.
- **`DROP TABLE IF EXISTS` left in permanently:** intentional — the operations model
  already does a full rebuild each run; the DROP just makes future clustering changes
  a non-event. No downstream model `ref()`s `game_neighbors` (grep-checked); only the
  read API reads it.
- **Rollback:** revert the branch. The reader change is additive (`similar_profiles`
  alongside `similar`); reverting restores the exact old contract. The clustering revert
  needs its own `DROP` + rebuild, same as this change.
