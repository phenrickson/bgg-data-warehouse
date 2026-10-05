# SQL response parsing — proof design

## Goal

Prove that BGG thing responses can be parsed in BigQuery SQL with nothing lost
compared to the Python processor. That is the prerequisite for two later goals:

1. **Snapshots:** rebuild `core.*` from the responses as they stood at any past date.
2. **Time series:** per-game history (ratings, users rated, ranks, owned counts, links)
   built from every fetch.

Both are queries over parsed responses, not Python replays. Today a rebuild means
extracting every response, parsing it in memory, and loading a dozen tables back.

The refresh policy (`config/bigquery.yaml`: recent games weekly, unpublished
bi-weekly, 2–5 years monthly, 5–10 years quarterly, 10+ years bi-annually) is
deliberate. Each game's history is as dense as its refresh schedule.

This spec covers **the proof only**. Cutover (the fetcher writing JSON, converting
history in place, Dataform owning `core.*`, retiring the processor) gets its own spec
and happens only if the proof succeeds.

## Constraint: response_data is a Python repr

`response_fetcher.py` stores `str({"items": {"item": item}})`, the Python repr of
the `xmltodict` output (single quotes, `None`, `True`). BigQuery cannot parse it. The
processor reads it with `json.loads`, falling back to `ast.literal_eval`. So the proof
starts with a one-off Python conversion to JSON. That conversion is the only Python
round trip in the design.

## Approach

Standalone SQL in a scratch dataset. Nothing in the proof touches Dataform or
production code. If it works, the SQL moves into Dataform during cutover.

Rejected alternatives:

- **Proof as Dataform models:** puts unproven work inside the production project.
- **Parsing the repr with a JavaScript UDF:** a fragile Python-repr parser, and
  production still needs JSON eventually.

### Faithful conversion

The JSON is exactly the `xmltodict` output, with no normalisation. In particular,
link and poll fields stay "a single object when there is one, a list when there are
several". The SQL handles both shapes with `JSON_TYPE`. Faithful conversion keeps the
round-trip check a strict equality.

## Components

All work lives in `analysis/sql-response-parsing/` on branch
`feat/sql-response-parsing`.

| Unit | Does | Depends on |
|---|---|---|
| `convert.py` | Reads `raw.raw_responses` and writes `scratch_parsing.responses_json` (`record_id`, `game_id`, `fetch_timestamp`, `response_json JSON`). Verifies each row as it goes. | `raw.raw_responses` (read-only) |
| `sql/parse_*.sql` | One file per target table. Each parses **every** response into rows tagged with `record_id` and `fetch_timestamp`. | `responses_json` |
| `sql/snapshot.sql` | Picks the latest successful response per game where `fetch_timestamp <= @as_of`, and filters the parsed tables to it. | parsed tables, `raw.fetched_responses` |
| `sql/compare_*.sql` | Diffs the snapshot for today against `core.*`, table by table and row by row. | snapshot, `core.*` (read-only) |
| `FINDINGS.md` | Every difference found, and how it was classified. | — |

Snapshots and time series are both filters on the per-response layer. Parsing all
~500k responses in SQL costs about the same as parsing the latest ~130k.

### Target tables

These are the tables `prepare_for_bigquery` produces:

- `games`
- `alternate_names`
- the dimension tables: `categories`, `mechanics`, `families`, `designers`,
  `artists`, `publishers`
- the bridge tables: `game_categories`, `game_mechanics`, `game_families`,
  `game_designers`, `game_artists`, `game_publishers`, `game_expansions`,
  `game_implementations`
- `player_counts`, `language_dependence`, `suggested_ages`
- `rankings`

## Verification

Layered. Each layer catches what the earlier ones cannot.

1. **Conversion round trip, every row:** `json.loads(response_json) ==
   ast.literal_eval(response_data)`. Any mismatch is reported and stops the
   conversion.
2. **Random sample:** a few thousand games, to get the parsing working and iterate
   quickly.
3. **Targeted edge cases:** each picked from what the processor handles specially.
   - A single link or poll result (an object) versus several (a list).
   - No polls, empty polls, or a missing link type.
   - Expansions. Note that `process_batch` passes `game_type="boardgame"` for every
     game, expansions included, so `core.games.type` may not reflect `@type`.
   - Responses whose `@id` does not match the game; empty responses; records marked
     `no_response`, `parse_error` or `failed`.
   - The `_safe_int` and `_safe_float` inputs: blanks, `"Not Ranked"`, `"N/A"`.
   - Null or zero `year_published`, and negative years.
   - Primary versus alternate names; unicode and HTML entities in descriptions.
   - Several ranks per game; zero or missing stats.
   - **Fetch era:** responses from the oldest to the newest fetches, since older
     responses may come from earlier versions of the fetcher.
4. **Full population:** the snapshot for today compared against all of `core.*`. It
   is cheap because it runs inside BigQuery.
5. **An older date works:** a snapshot for a past date differs from today's, and in
   the expected ways.
   - Games first fetched after that date are absent.
   - Stats and ranks match the responses that were current at that date, spot-checked
     against the raw responses directly.

Every difference is classified as either a **parsing gap**, which gets fixed, or a
**processor behaviour** that the SQL reproduces or deliberately corrects. Both are
recorded in `FINDINGS.md`. None are waved away.

**The proof succeeds when** layers 1–4 show no unexplained differences and layer 5
behaves as expected.

### Comparing against core

`core.games` and `core.rankings` are append-only, so the comparison uses their latest
row per game. The other `core.*` tables hold current state per game only. Games whose
latest response is not `success` in `fetched_responses`, or was never processed, are
reconciled separately and not counted as mismatches.

## Production safety

- **Dataset:** run `bq ls` to confirm `scratch_parsing` does not exist, then:

  ```
  bq mk --location=US --default_table_expiration=2592000 bgg-data-warehouse:scratch_parsing
  ```

  Every table expires after 30 days. The dataset is not managed by Terraform, and
  Terraform ignores datasets it does not manage. Dataform writes only to the
  datasets it declares. The only code that lists datasets is a read-only integration
  test and bgg-dash-viewer's monitoring page (`INFORMATION_SCHEMA.SCHEMATA`), which
  will display the dataset.
- **Write guard:** `convert.py` refuses any destination outside
  `bgg-data-warehouse.scratch_parsing`.
- **SQL:** creates or replaces tables in `scratch_parsing` only. No DML. Production
  datasets appear only in `FROM` clauses.
- **Cost:** every query is dry-run first and run with `--maximum_bytes_billed`.
  Billed runs are approved one at a time.
- **Branch:** nothing in `src/`, `definitions/`, `.github/` or `terraform/` changes.
- **Cleanup:** run `bq rm -r scratch_parsing` when the proof ends, or let the tables
  expire.

The guards are in the code, not in permissions. User credentials can write anywhere.
A restricted service account was judged unnecessary.

## Out of scope

- Changing the fetcher, converting history in place, Dataform models, and retiring
  the processor (cutover spec).
- Time-series tables and snapshot tables for consumers. The proof only shows that
  both are filters over the parsed layer.
- Fixing processor bugs. They are recorded only.
