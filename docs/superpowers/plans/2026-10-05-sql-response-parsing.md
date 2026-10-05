# SQL Response Parsing Proof — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that BGG thing responses can be parsed in BigQuery SQL with nothing lost
compared to the Python processor, and that a snapshot for an older date works.

**Architecture:**
1. A one-off Python conversion copies every `raw.raw_responses` row into
   `scratch_parsing.responses_json` as a BigQuery `JSON` value. It proves each row
   unchanged, via an in-process round trip plus a stored content hash.
2. Standalone SQL files parse **every** response into per-response tables tagged with
   `record_id` and `fetch_timestamp`.
3. Snapshots are filters over that layer. Comparison queries diff today's snapshot
   against `core.*`. An as-of check covers an older date.
4. Nothing outside `analysis/sql-response-parsing/` and `docs/` changes.

**Tech Stack:** Python 3.12 (`uv`), `google-cloud-bigquery` 3.34, BigQuery GoogleSQL
(JSON type, `JSON_TYPE`/`JSON_QUERY_ARRAY`/`JSON_VALUE`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-05-sql-response-parsing-design.md`

## Global Constraints

- **Scratch only:** all writes go to `bgg-data-warehouse.scratch_parsing`, created with
  `bq mk --location=US --default_table_expiration=2592000`.
- **No DML:** SQL creates or replaces tables, views or functions in `scratch_parsing`
  only. Production datasets (`raw`, `core`, `analytics`, `predictions`) appear only as
  read sources.
- **Billed queries need approval:** every billed query is dry-run first, then run with
  `--maximum_bytes_billed` (or `--max-gb` in `run_sql.py`). **Stop and get Phil's
  approval before each billed run**, quoting the dry-run bytes. Table reads through
  `list_rows` are free and need no approval.
- **Files:** nothing in `src/`, `definitions/`, `.github/` or `terraform/` changes.
  Everything goes in `analysis/sql-response-parsing/` and `docs/`.
- **Faithful conversion:** the JSON is exactly the `xmltodict` output. No
  normalisation.
- **Classify every difference:** each one goes in `FINDINGS.md` as a **parsing gap**
  (fix it) or a **processor behaviour** (reproduce it, or deliberately correct it).
  None are dropped.
- **Delivery:** branch `feat/sql-response-parsing`, conventional commits
  (`feat(analysis): …`, `docs(…): …`), one PR. Phil merges.
- **Testing:** `uv run --extra test python -m pytest analysis/sql-response-parsing -q`
  runs the analysis tests. The repo's `testpaths` stays `["tests"]`.

## Review Focus

- **Non-string JSON shapes from `xmltodict`.** `name` can be a bare string, and
  `description`, `poll` or `ranks` can be `null` (empty elements). Expected: parsing
  matches Python's defaults (`"Unknown"`, `""`, empty lists). Pinned by the edge-tag
  query and the compare queries (Tasks 4–8).
- **A key that is missing vs. a key that is present but `null`.** Python uses `""` for
  a missing `description` but `None` for an empty one. Expected: the SQL keeps the
  distinction. Pinned in Task 4's `parse_games.sql` and its compare.
- **Integer conversions that would throw in Python** (`int(@numvotes)`,
  `int(@sortindex)`, `int(link @id)`). Python fails the whole game. Expected: the SQL
  flags these, never silently zero or null. Pinned by `checks_strict_ints.sql`
  (Task 9).
- **Expansions.** `process_batch` hard-codes `type="boardgame"`. Expected: the SQL
  reproduces `type`, and records `item_type` from `@type` separately. Pinned in Task 4.
- **Responses that core never used** (newest response not yet processed; an empty, `no_response` or
  unparseable response). Expected: they're excluded from today's comparison by
  construction (`core_inputs`), not counted as mismatches. Pinned in Task 4's
  `core_inputs.sql`.

---

## File Structure

```
analysis/sql-response-parsing/
  README.md                 how to run each step, safety rules
  sqlguard.py               refuses SQL that writes outside scratch_parsing or uses DML
  run_sql.py                guard + dry-run + capped run for one .sql file
  convert.py                raw_responses -> scratch_parsing.responses_json
  verify_conversion.py      re-hashes stored JSON against content_hash
  check_asof.py             spot-checks an as-of snapshot against original repr responses
  test_sqlguard.py
  test_convert.py
  test_verify_conversion.py
  test_check_asof.py
  sql/
    udf_as_array.sql        JSON -> ARRAY<JSON> (array as-is, null -> [], else [x])
    udf_safe_int.sql        mirrors processor._safe_int for string input
    udf_safe_float.sql      mirrors processor._safe_float for string input
    input_sample.sql        responses_input view over a 5,000-game sample
    input_full.sql          responses_input view over every converted response
    core_inputs.sql         the exact record core.* was built from, per game
    parse_items.sql         one row per response: the matched <item> JSON
    parse_games.sql
    parse_names.sql         alternate names + primary name per response
    parse_links.sql         every <link>, all 8 types
    parse_player_counts.sql
    parse_language_dependence.sql
    parse_suggested_ages.sql
    parse_rankings.sql
    compare_games.sql
    compare_alternate_names.sql
    compare_bridges.sql
    compare_dimensions.sql
    compare_player_counts.sql
    compare_language_dependence.sql
    compare_suggested_ages.sql
    compare_rankings.sql
    edge_tags.sql           tags each response with its edge-case features
    checks_strict_ints.sql  values Python's int() would have thrown on
    asof_snapshot.sql       latest successful response per game <= @as_of
    asof_checks.sql         counts showing the older date differs, sensibly
  FINDINGS.md
```

---

### Task 1: Scaffold, SQL guard and runner

**Files:**
- Create: `analysis/sql-response-parsing/README.md`
- Create: `analysis/sql-response-parsing/sqlguard.py`
- Create: `analysis/sql-response-parsing/run_sql.py`
- Test: `analysis/sql-response-parsing/test_sqlguard.py`

**Interfaces:**
- Produces: `sqlguard.check_sql(sql: str) -> None`, which raises `ValueError` with a
  reason. `run_sql.py <file> [--dry-run | --max-gb N] [--param name:TYPE:value ...]`.

- [ ] **Step 1: Write the failing tests**

```python
# analysis/sql-response-parsing/test_sqlguard.py
import pytest

from sqlguard import check_sql

OK = """
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_games` AS
SELECT * FROM `bgg-data-warehouse.raw.raw_responses`
"""


def test_scratch_create_reading_raw_is_allowed():
    check_sql(OK)


def test_create_outside_scratch_is_refused():
    with pytest.raises(ValueError, match="outside scratch_parsing"):
        check_sql("CREATE OR REPLACE TABLE `bgg-data-warehouse.core.games` AS SELECT 1")


def test_unqualified_create_is_refused():
    with pytest.raises(ValueError, match="outside scratch_parsing"):
        check_sql("CREATE TABLE games AS SELECT 1")


@pytest.mark.parametrize("stmt", ["INSERT INTO", "UPDATE ", "DELETE FROM", "MERGE ", "TRUNCATE TABLE", "DROP TABLE", "ALTER TABLE"])
def test_dml_and_drops_are_refused(stmt):
    with pytest.raises(ValueError, match="not allowed"):
        check_sql(f"{stmt} `bgg-data-warehouse.scratch_parsing.x` SELECT 1")


def test_keywords_inside_comments_and_strings_do_not_count():
    check_sql(OK + "\n-- we never DELETE FROM anything\nSELECT 'MERGE ' AS s")


def test_functions_and_views_in_scratch_are_allowed():
    check_sql("CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.as_array`(j JSON) AS (j)")
    check_sql("CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS SELECT 1")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_sqlguard.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'sqlguard'`.

- [ ] **Step 3: Implement `sqlguard.py`**

```python
"""Refuse SQL that could touch anything outside the scratch dataset.

The proof's safety comes from code, not permissions (user credentials can write
anywhere), so every .sql file goes through check_sql before it is sent.
"""

import re

SCRATCH = "bgg-data-warehouse.scratch_parsing"
FORBIDDEN = ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "DROP", "ALTER")
CREATE_TARGET = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+)?(?:TABLE|VIEW|FUNCTION)\s+(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|[\w.\-]+)",
    re.IGNORECASE,
)


def _strip_comments_and_strings(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", "''", sql)


def check_sql(sql: str) -> None:
    code = _strip_comments_and_strings(sql)
    for word in FORBIDDEN:
        if re.search(rf"\b{word}\b", code, re.IGNORECASE):
            raise ValueError(f"{word} is not allowed in proof SQL")
    for target in CREATE_TARGET.findall(code):
        name = target.strip("`")
        if not name.startswith(SCRATCH + "."):
            raise ValueError(f"CREATE target {name} is outside scratch_parsing")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_sqlguard.py -q`
Expected: all PASS.

- [ ] **Step 5: Write `run_sql.py`**

```python
"""Run one proof .sql file: guard, then dry-run or a capped run.

  uv run python analysis/sql-response-parsing/run_sql.py sql/parse_games.sql --dry-run
  uv run python analysis/sql-response-parsing/run_sql.py sql/parse_games.sql --max-gb 4
  ... --param as_of:TIMESTAMP:2026-04-05T00:00:00Z
"""

import argparse
from pathlib import Path

from google.cloud import bigquery

from sqlguard import check_sql

PROJECT = "bgg-data-warehouse"


def _param(spec: str) -> bigquery.ScalarQueryParameter:
    name, type_, value = spec.split(":", 2)
    return bigquery.ScalarQueryParameter(name, type_, value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--max-gb", type=float)
    parser.add_argument("--param", action="append", default=[])
    args = parser.parse_args()

    path = Path(args.file)
    if not path.is_absolute():
        path = Path(__file__).parent / path
    sql = path.read_text(encoding="utf-8")
    check_sql(sql)

    client = bigquery.Client(project=PROJECT)
    params = [_param(p) for p in args.param]
    if args.dry_run:
        config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, query_parameters=params)
        job = client.query(sql, job_config=config)
        print(f"{path.name}: {job.total_bytes_processed / 1e9:.3f} GB")
        return

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=int(args.max_gb * 1e9), query_parameters=params
    )
    job = client.query(sql, job_config=config)
    rows = list(job.result())
    print(f"{path.name}: billed {(job.total_bytes_billed or 0) / 1e9:.3f} GB")
    for row in rows[:200]:
        print(dict(row.items()))


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Write `README.md`**

````markdown
# SQL response parsing proof

Throwaway analysis for `docs/superpowers/specs/2026-10-05-sql-response-parsing-design.md`.
Nothing here is imported by the pipeline.

**Safety:** all writes go to `bgg-data-warehouse.scratch_parsing`. Every `.sql` file
goes through `sqlguard.check_sql` (scratch-only CREATE, no DML). Every billed query is
dry-run first, run with a cap, and approved one at a time.

    uv run python analysis/sql-response-parsing/run_sql.py sql/<file>.sql --dry-run
    uv run python analysis/sql-response-parsing/run_sql.py sql/<file>.sql --max-gb <cap>

Order: UDFs → `convert.py` → `verify_conversion.py` → input view → `core_inputs` →
`parse_items` → `parse_*` → `compare_*` → `edge_tags` / `checks_*` → `asof_*` →
`check_asof.py`.

Cleanup: `bq rm -r -f bgg-data-warehouse:scratch_parsing` (tables also expire after
30 days).
````

- [ ] **Step 7: Commit**

```bash
git add analysis/sql-response-parsing/README.md analysis/sql-response-parsing/sqlguard.py analysis/sql-response-parsing/run_sql.py analysis/sql-response-parsing/test_sqlguard.py
git commit -m "feat(analysis): scratch-only SQL guard and runner for the parsing proof"
```

---

### Task 2: Conversion, with an in-process round trip and a content hash

**Files:**
- Create: `analysis/sql-response-parsing/convert.py`
- Test: `analysis/sql-response-parsing/test_convert.py`

**Interfaces:**
- Produces:
  - `convert.assert_scratch(table_id: str) -> None`
  - `convert.convert_response(response_data: str | None) -> tuple[str, dict | None]`.
    The status is `"repr"`, `"json"`, `"empty"` or `"unparseable"`. It raises
    `ValueError` if the round trip is lossy.
  - `convert.content_hash(parsed: dict) -> str`
  - Table `scratch_parsing.responses_json`: `record_id STRING`, `game_id INT64`,
    `fetch_timestamp TIMESTAMP`, `convert_status STRING`, `content_hash STRING`,
    `response_json JSON`. Partitioned by `DATE(fetch_timestamp)`, clustered by
    `game_id, record_id`.

- [ ] **Step 1: Write the failing tests**

```python
# analysis/sql-response-parsing/test_convert.py
import pytest

from convert import assert_scratch, content_hash, convert_response

REPR = "{'items': {'item': {'@id': '13', 'name': {'@type': 'primary', '@value': \"Catan's\"}, 'description': None, 'flag': True}}}"


def test_repr_converts_and_round_trips():
    status, parsed = convert_response(REPR)
    assert status == "repr"
    assert parsed["items"]["item"]["name"]["@value"] == "Catan's"
    assert parsed["items"]["item"]["description"] is None
    assert parsed["items"]["item"]["flag"] is True


def test_json_input_is_accepted_as_json():
    status, parsed = convert_response('{"items": {"item": {"@id": "13"}}}')
    assert (status, parsed) == ("json", {"items": {"item": {"@id": "13"}}})


@pytest.mark.parametrize("blank", [None, "", "   \n"])
def test_blank_is_empty(blank):
    assert convert_response(blank) == ("empty", None)


def test_garbage_is_unparseable():
    assert convert_response("{'items': ") == ("unparseable", None)


def test_non_dict_is_unparseable():
    assert convert_response("[1, 2]") == ("unparseable", None)


def test_lossy_round_trip_raises():
    # A tuple becomes a JSON list and would not compare equal on the way back
    with pytest.raises(ValueError, match="round trip"):
        convert_response("{'items': ('a', 'b')}")


def test_unicode_and_entities_survive():
    status, parsed = convert_response("{'d': 'Café &amp; ☃ &#10;'}")
    assert parsed == {"d": "Café &amp; ☃ &#10;"}


def test_content_hash_ignores_key_order():
    assert content_hash({"a": "1", "b": ["x"]}) == content_hash({"b": ["x"], "a": "1"})
    assert content_hash({"a": "1"}) != content_hash({"a": "2"})


def test_assert_scratch():
    assert_scratch("bgg-data-warehouse.scratch_parsing.responses_json")
    with pytest.raises(ValueError):
        assert_scratch("bgg-data-warehouse.raw.raw_responses")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_convert.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'convert'`.

- [ ] **Step 3: Implement `convert.py`**

```python
"""Copy raw.raw_responses into scratch_parsing.responses_json as faithful JSON.

response_data is the Python repr of the xmltodict output. Each row is
literal_eval'd, checked to survive a JSON round trip unchanged, and stored as a
BigQuery JSON value with a content hash so verify_conversion.py can prove
BigQuery stored it unchanged too. Reads use list_rows (free).

  uv run python analysis/sql-response-parsing/convert.py
"""

import ast
import hashlib
import json
from typing import Any, Optional

from google.cloud import bigquery

PROJECT = "bgg-data-warehouse"
SCRATCH = f"{PROJECT}.scratch_parsing"
SOURCE = f"{PROJECT}.raw.raw_responses"
DEST = f"{SCRATCH}.responses_json"
CHUNK = 20_000

SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("game_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("fetch_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("convert_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("content_hash", "STRING"),
    bigquery.SchemaField("response_json", "JSON"),
]


def assert_scratch(table_id: str) -> None:
    if not table_id.startswith(SCRATCH + "."):
        raise ValueError(f"refusing to write {table_id}: outside {SCRATCH}")


def content_hash(parsed: Any) -> str:
    canonical = json.dumps(parsed, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def convert_response(response_data: Optional[str]) -> tuple[str, Optional[dict]]:
    if response_data is None or not response_data.strip():
        return "empty", None
    try:
        parsed = json.loads(response_data)
        status = "json"
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(response_data)
        except (ValueError, SyntaxError, MemoryError, RecursionError):
            return "unparseable", None
        status = "repr"
    if not isinstance(parsed, dict):
        return "unparseable", None
    if json.loads(json.dumps(parsed, ensure_ascii=False)) != parsed:
        raise ValueError("JSON round trip changed the response")
    return status, parsed


def _flush(client: bigquery.Client, rows: list, first: bool) -> None:
    assert_scratch(DEST)
    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE" if first else "WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(field="fetch_timestamp"),
        clustering_fields=["game_id", "record_id"],
    )
    client.load_table_from_json(rows, DEST, job_config=config).result()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="trial run on the first N rows")
    args = parser.parse_args()
    assert_scratch(DEST)
    client = bigquery.Client(project=PROJECT)
    source = client.get_table(SOURCE)
    fields = [f for f in source.schema if f.name in ("record_id", "game_id", "fetch_timestamp", "response_data")]
    counts: dict[str, int] = {}
    rows, first = [], True
    for row in client.list_rows(source, selected_fields=fields, page_size=5_000, max_results=args.limit):
        status, parsed = convert_response(row["response_data"])
        counts[status] = counts.get(status, 0) + 1
        rows.append({
            "record_id": row["record_id"],
            "game_id": row["game_id"],
            "fetch_timestamp": row["fetch_timestamp"].isoformat(),
            "convert_status": status,
            "content_hash": content_hash(parsed) if parsed is not None else None,
            "response_json": parsed,
        })
        if len(rows) >= CHUNK:
            _flush(client, rows, first)
            first, rows = False, []
            print(counts, flush=True)
    if rows:
        _flush(client, rows, first)
    print("done", counts)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_convert.py -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add analysis/sql-response-parsing/convert.py analysis/sql-response-parsing/test_convert.py
git commit -m "feat(analysis): faithful repr-to-JSON conversion with round-trip check"
```

---

### Task 3: Create the dataset, run the conversion, verify what was stored

**Files:**
- Create: `analysis/sql-response-parsing/verify_conversion.py`
- Test: `analysis/sql-response-parsing/test_verify_conversion.py`
- Create: `analysis/sql-response-parsing/sql/udf_as_array.sql`,
  `sql/udf_safe_int.sql`, `sql/udf_safe_float.sql`
- Create: `analysis/sql-response-parsing/FINDINGS.md`

**Interfaces:**
- Consumes: `convert.content_hash`, the `responses_json` table.
- Produces:
  - `verify_conversion.check_row(stored_json, expected_hash: str | None) -> bool`
  - UDFs `scratch_parsing.as_array(JSON) -> ARRAY<JSON>`,
    `scratch_parsing.safe_int(STRING) -> INT64` and
    `scratch_parsing.safe_float(STRING) -> FLOAT64`

- [ ] **Step 1: Write the failing test**

```python
# analysis/sql-response-parsing/test_verify_conversion.py
from convert import content_hash
from verify_conversion import check_row


def test_dict_matching_hash_passes():
    parsed = {"items": {"item": {"@id": "13"}}}
    assert check_row(parsed, content_hash(parsed))


def test_json_string_value_is_parsed_first():
    assert check_row('{"a": "1"}', content_hash({"a": "1"}))


def test_changed_value_fails():
    assert not check_row({"a": "2"}, content_hash({"a": "1"}))


def test_null_with_no_hash_passes_and_mismatched_null_fails():
    assert check_row(None, None)
    assert not check_row(None, content_hash({"a": "1"}))
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_verify_conversion.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'verify_conversion'`.

- [ ] **Step 3: Implement `verify_conversion.py`**

```python
"""Prove BigQuery stored every converted response unchanged.

Re-hashes each stored response_json (read with list_rows, free) and compares it
to the content_hash computed from the literal_eval'd original in convert.py.

  uv run python analysis/sql-response-parsing/verify_conversion.py
"""

import json
from typing import Any, Optional

from google.cloud import bigquery

from convert import DEST, PROJECT, content_hash


def check_row(stored: Any, expected_hash: Optional[str]) -> bool:
    if isinstance(stored, str):
        stored = json.loads(stored)
    if stored is None:
        return expected_hash is None
    return content_hash(stored) == expected_hash


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    table = client.get_table(DEST)
    checked = mismatched = 0
    for row in client.list_rows(table, page_size=5_000):
        checked += 1
        if not check_row(row["response_json"], row["content_hash"]):
            mismatched += 1
            print("MISMATCH", row["record_id"])
    print(f"checked {checked}, mismatched {mismatched}")
    if mismatched:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_verify_conversion.py -q`
Expected: all PASS.

- [ ] **Step 5: Write the three UDF files**

```sql
-- sql/udf_as_array.sql
-- xmltodict gives one child as an object and several as a list; null/missing -> [].
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.as_array`(j JSON)
RETURNS ARRAY<JSON> AS (
  CASE
    WHEN j IS NULL OR JSON_TYPE(j) = 'null' THEN []
    WHEN JSON_TYPE(j) = 'array' THEN JSON_QUERY_ARRAY(j)
    ELSE [j]
  END
)
```

```sql
-- sql/udf_safe_int.sql
-- processor._safe_int on a string: int(), negatives -> 0, failure -> 0.
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.safe_int`(s STRING)
RETURNS INT64 AS (
  IFNULL(GREATEST(SAFE_CAST(TRIM(s) AS INT64), 0), 0)
)
```

```sql
-- sql/udf_safe_float.sql
-- processor._safe_float on a string: float(), failure -> 0.0.
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.safe_float`(s STRING)
RETURNS FLOAT64 AS (
  IFNULL(SAFE_CAST(TRIM(s) AS FLOAT64), 0.0)
)
```

- [ ] **Step 6: Create the dataset (approved in the spec)**

Run: `bq ls --project_id=bgg-data-warehouse | grep -w scratch_parsing || echo "absent"`
Expected: `absent`. If it is present, **stop and ask Phil.**

Run: `bq mk --location=US --default_table_expiration=2592000 --description "Throwaway: SQL response parsing proof" bgg-data-warehouse:scratch_parsing`
Expected: `Dataset 'bgg-data-warehouse:scratch_parsing' successfully created.`

- [ ] **Step 7: Create the UDFs**

Dry-run each with `run_sql.py sql/udf_*.sql --dry-run`. Expected: 0 GB. Then run each
with `--max-gb 0.01`.

- [ ] **Step 8: Trial the conversion on 1,000 rows**

Run: `uv run python analysis/sql-response-parsing/convert.py --limit 1000`
Then write `sql/check_json_type.sql`:

```sql
SELECT convert_status, JSON_TYPE(response_json) AS json_type, COUNT(*) AS n
FROM `bgg-data-warehouse.scratch_parsing.responses_json`
GROUP BY 1, 2
```

Dry-run it, get approval, and run it (it costs pennies).
Expected: `repr`/`json` rows have `json_type = 'object'`, and `empty`/`unparseable`
rows have a NULL `json_type`. If the values were stored as JSON strings, change `_flush`
to load through a DataFrame with the column typed JSON, and re-run the trial.
Then run `verify_conversion.py`. Expected: `checked 1000, mismatched 0`.

- [ ] **Step 8b: Run the full conversion**

`list_rows` reads are free and load jobs are free. Tell Phil it is starting, then run:
`uv run python analysis/sql-response-parsing/convert.py`
Expected: it finishes with `done {...}`, and the status counts add up to the
`raw_responses` row count (about 498k). A `ValueError` (lossy round trip) **stops the
run**. Record the record and the reason in `FINDINGS.md`, then ask Phil.

- [ ] **Step 9: Verify what was stored**

Run: `uv run python analysis/sql-response-parsing/verify_conversion.py`
Expected: `checked N, mismatched 0`, where N equals the conversion total.

- [ ] **Step 10: Check that every record was copied**

Write `sql/check_conversion_counts.sql`:

```sql
SELECT
  (SELECT COUNT(*) FROM `bgg-data-warehouse.raw.raw_responses`) AS raw_rows,
  (SELECT COUNT(DISTINCT record_id) FROM `bgg-data-warehouse.raw.raw_responses`) AS raw_ids,
  (SELECT COUNT(*) FROM `bgg-data-warehouse.scratch_parsing.responses_json`) AS json_rows,
  (SELECT COUNT(*) FROM (
     SELECT record_id FROM `bgg-data-warehouse.raw.raw_responses`
     EXCEPT DISTINCT
     SELECT record_id FROM `bgg-data-warehouse.scratch_parsing.responses_json`)) AS missing_ids
```

Dry-run it, then get approval and run it (expected ≈ 0.05 GB).
Expected: `raw_rows = json_rows`, `missing_ids = 0`.

- [ ] **Step 11: Start `FINDINGS.md`**

```markdown
# Findings — SQL response parsing proof

## Conversion
- Rows: <raw_rows>; status counts: <counts from convert.py>
- Stored-hash verification: <checked> checked, <mismatched> mismatched
- Notes: <anything unparseable / empty, with counts>

## Differences
| Table | Difference | Count | Classification | Resolution |
|---|---|---|---|---|
```

- [ ] **Step 12: Commit**

```bash
git add analysis/sql-response-parsing/verify_conversion.py analysis/sql-response-parsing/test_verify_conversion.py analysis/sql-response-parsing/sql/udf_*.sql analysis/sql-response-parsing/sql/check_conversion_counts.sql analysis/sql-response-parsing/sql/check_json_type.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): verify stored JSON and add parsing UDFs"
```

---

### Task 4: Sample input, core inputs, items and games

**Files:**
- Create: `sql/input_sample.sql`, `sql/input_full.sql`, `sql/core_inputs.sql`,
  `sql/parse_items.sql`, `sql/parse_games.sql`, `sql/compare_games.sql`
  (all under `analysis/sql-response-parsing/`)

**Interfaces:**
- Consumes: `responses_json` and the UDFs.
- Produces:
  - View `responses_input` (`record_id`, `game_id`, `fetch_timestamp`,
    `convert_status`, `response_json`)
  - Table `core_inputs` (`game_id`, `record_id`, `fetch_timestamp`)
  - Table `parsed_items` (`record_id`, `game_id`, `fetch_timestamp`, `item JSON`)
  - Table `parsed_games`, with `core.games` columns plus `record_id` and `item_type`

- [ ] **Step 1: Write the input views**

```sql
-- sql/input_sample.sql  (5,000 random games, every response for each)
CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS
SELECT r.*
FROM `bgg-data-warehouse.scratch_parsing.responses_json` r
WHERE r.game_id IN (
  SELECT game_id FROM `bgg-data-warehouse.core.games`
  GROUP BY game_id
  ORDER BY FARM_FINGERPRINT(CAST(game_id AS STRING))
  LIMIT 5000
)
```

```sql
-- sql/input_full.sql
CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS
SELECT * FROM `bgg-data-warehouse.scratch_parsing.responses_json`
```

- [ ] **Step 2: Write `core_inputs.sql`**

`core.games` is append-only, and its `load_timestamp` is the `fetch_timestamp` of the
response it came from. The latest row per game therefore identifies the exact response
current `core.*` was built from.

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.core_inputs` AS
WITH latest AS (
  SELECT game_id, MAX(load_timestamp) AS fetch_timestamp
  FROM `bgg-data-warehouse.core.games`
  GROUP BY game_id
)
SELECT l.game_id, r.record_id, l.fetch_timestamp
FROM latest l
JOIN `bgg-data-warehouse.raw.raw_responses` r
  ON r.game_id = l.game_id AND r.fetch_timestamp = l.fetch_timestamp
JOIN `bgg-data-warehouse.raw.processed_responses` p
  ON p.record_id = r.record_id AND p.process_status = 'success'
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.game_id ORDER BY r.record_id DESC) = 1
```

- [ ] **Step 3: Write `parse_items.sql`**

This mirrors `process_game`: it takes the first item whose `@id` equals the game id.

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_items`
PARTITION BY DATE(fetch_timestamp)
CLUSTER BY game_id, record_id AS
SELECT r.record_id, r.game_id, r.fetch_timestamp, item
FROM `bgg-data-warehouse.scratch_parsing.responses_input` r,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(r.response_json, '$.items.item'))) AS item WITH OFFSET o
WHERE r.convert_status IN ('repr', 'json')
  AND JSON_TYPE(item) = 'object'
  AND JSON_VALUE(item, '$."@id"') = CAST(r.game_id AS STRING)
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.record_id ORDER BY o) = 1
```

- [ ] **Step 4: Write `parse_games.sql`**

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_games`
PARTITION BY DATE(load_timestamp)
CLUSTER BY game_id AS
WITH named AS (
  SELECT
    i.record_id,
    -- processor._extract_names: last primary wins; default 'Unknown'
    IFNULL((
      SELECT JSON_VALUE(n, '$."@value"')
      FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.name'))) n WITH OFFSET o
      WHERE JSON_TYPE(n) = 'object' AND JSON_VALUE(n, '$."@type"') = 'primary'
      ORDER BY o DESC LIMIT 1
    ), 'Unknown') AS primary_name
  FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i
),
yr AS (
  SELECT record_id,
    CASE JSON_TYPE(JSON_QUERY(item, '$.yearpublished'))
      WHEN 'string' THEN JSON_VALUE(item, '$.yearpublished')
      ELSE JSON_VALUE(item, '$.yearpublished."@value"')
    END AS year_str
  FROM `bgg-data-warehouse.scratch_parsing.parsed_items`
)
SELECT
  i.game_id,
  'boardgame' AS type,                              -- process_batch hard-codes this
  JSON_VALUE(i.item, '$."@type"') AS item_type,     -- what BGG actually says
  n.primary_name,
  CAST(NULLIF(SAFE_CAST(TRIM(y.year_str) AS INT64), 0) AS FLOAT64) AS year_published,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minplayers."@value"')) AS min_players,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.maxplayers."@value"')) AS max_players,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.playingtime."@value"')) AS playing_time,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minplaytime."@value"')) AS min_playtime,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.maxplaytime."@value"')) AS max_playtime,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minage."@value"')) AS min_age,
  -- item.get(key, ""): missing key -> '', present-but-null -> NULL
  IF(JSON_QUERY(i.item, '$.description') IS NULL, '', JSON_VALUE(i.item, '$.description')) AS description,
  IF(JSON_QUERY(i.item, '$.thumbnail') IS NULL, '', JSON_VALUE(i.item, '$.thumbnail')) AS thumbnail,
  IF(JSON_QUERY(i.item, '$.image') IS NULL, '', JSON_VALUE(i.item, '$.image')) AS image,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.usersrated."@value"')) AS users_rated,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.average."@value"')) AS average_rating,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.bayesaverage."@value"')) AS bayes_average,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.stddev."@value"')) AS standard_deviation,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.median."@value"')) AS median_rating,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.owned."@value"')) AS owned_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.trading."@value"')) AS trading_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.wanting."@value"')) AS wanting_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.wishing."@value"')) AS wishing_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.numcomments."@value"')) AS num_comments,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.numweights."@value"')) AS num_weights,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.averageweight."@value"')) AS average_weight,
  i.fetch_timestamp AS load_timestamp,
  i.record_id
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i
JOIN named n USING (record_id)
JOIN yr y USING (record_id)
```

- [ ] **Step 5: Write `compare_games.sql`**

```sql
-- Today's snapshot (core_inputs records) vs the latest core.games row per game.
WITH core_latest AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT g.*, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_timestamp DESC) rn
    FROM `bgg-data-warehouse.core.games` g
    WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_games`)
  ) WHERE rn = 1
),
parsed AS (
  SELECT p.* EXCEPT(record_id, item_type)
  FROM `bgg-data-warehouse.scratch_parsing.parsed_games` p
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id)
),
only_core AS (SELECT * FROM core_latest EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_latest)
SELECT 'games' AS tbl,
  (SELECT COUNT(*) FROM core_latest) AS core_rows,
  (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core,
  (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

`p.* EXCEPT(record_id, item_type)` leaves `core.games`' columns in `core.games`' order
(game_id … load_timestamp). `EXCEPT DISTINCT` compares by position, so keep
`parse_games.sql`'s SELECT in that order.

- [ ] **Step 6: Run the sample**

For each of `input_sample`, `core_inputs`, `parse_items`, `parse_games` and
`compare_games`: dry-run, report the bytes, **get approval**, then run with a cap of
1.5× the dry run.
Expected: `parsed_rows` ≈ `core_rows`, and `only_in_core = only_in_parsed = 0`.

- [ ] **Step 7: Fix or classify every difference**

For each sample row, decide whether it's a parsing gap (fix the SQL and re-run Step 6)
or a processor behaviour (add a row to `FINDINGS.md`). Repeat until the remaining
counts are all classified.

- [ ] **Step 8: Commit**

```bash
git add analysis/sql-response-parsing/sql/input_*.sql analysis/sql-response-parsing/sql/core_inputs.sql analysis/sql-response-parsing/sql/parse_items.sql analysis/sql-response-parsing/sql/parse_games.sql analysis/sql-response-parsing/sql/compare_games.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): parse games from JSON responses; compare with core.games"
```

---

### Task 5: Alternate names

**Files:**
- Create: `sql/parse_names.sql`, `sql/compare_alternate_names.sql`

**Interfaces:**
- Consumes: `parsed_items`, `core_inputs`
- Produces: table `parsed_alternate_names` (`record_id`, `game_id`, `fetch_timestamp`,
  `name`, `sort_index`)

- [ ] **Step 1: Write `parse_names.sql`**

```sql
-- processor._extract_names: non-primary objects and bare strings become alternates.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_alternate_names`
CLUSTER BY game_id, record_id AS
SELECT
  i.record_id, i.game_id, i.fetch_timestamp,
  CASE JSON_TYPE(n)
    WHEN 'string' THEN JSON_VALUE(n)
    ELSE IFNULL(JSON_VALUE(n, '$."@value"'), 'Unknown')
  END AS name,
  CASE JSON_TYPE(n)
    WHEN 'string' THEN 1
    ELSE IFNULL(SAFE_CAST(JSON_VALUE(n, '$."@sortindex"') AS INT64), 1)
  END AS sort_index
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.name'))) n
WHERE JSON_TYPE(n) = 'string'
   OR (JSON_TYPE(n) = 'object' AND IFNULL(JSON_VALUE(n, '$."@type"'), '') != 'primary')
```

- [ ] **Step 2: Write `compare_alternate_names.sql`**

```sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT a.game_id, a.name, a.sort_index FROM `bgg-data-warehouse.core.alternate_names` a
              WHERE a.game_id IN (SELECT game_id FROM games)),
parsed AS (SELECT p.game_id, p.name, p.sort_index FROM `bgg-data-warehouse.scratch_parsing.parsed_alternate_names` p
           JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'alternate_names' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

- [ ] **Step 3: Run on the sample (dry-run, approval, capped run), then fix or
  classify every difference into `FINDINGS.md`**

Expected: counts close, and remaining differences classified. Note: the loader only
deletes a game's old rows when the batch's DataFrame for that table is non-empty
(`_load_dataframe` returns early on an empty DataFrame). So `core` can hold stale
rows. That is a processor behaviour.

- [ ] **Step 4: Commit**

```bash
git add analysis/sql-response-parsing/sql/parse_names.sql analysis/sql-response-parsing/sql/compare_alternate_names.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): parse alternate names; compare with core"
```

---

### Task 6: Links, the eight bridge tables and six dimension tables

**Files:**
- Create: `sql/parse_links.sql`, `sql/compare_bridges.sql`, `sql/compare_dimensions.sql`

**Interfaces:**
- Consumes: `parsed_items`, `core_inputs`
- Produces: table `parsed_links` (`record_id`, `game_id`, `fetch_timestamp`,
  `link_type`, `entity_id`, `name`, `inbound`, `id_raw`)

- [ ] **Step 1: Write `parse_links.sql`**

```sql
-- processor._extract_links + prepare_for_bigquery: every link of the 8 mapped types.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_links`
CLUSTER BY link_type, game_id AS
SELECT
  i.record_id, i.game_id, i.fetch_timestamp,
  JSON_VALUE(l, '$."@type"') AS link_type,
  IFNULL(SAFE_CAST(JSON_VALUE(l, '$."@id"') AS INT64), 0) AS entity_id,
  JSON_VALUE(l, '$."@id"') AS id_raw,               -- kept for checks_strict_ints
  IFNULL(JSON_VALUE(l, '$."@value"'), 'Unknown') AS name,
  IFNULL(JSON_VALUE(l, '$."@inbound"'), 'false') = 'true' AS inbound
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.link'))) l
WHERE JSON_TYPE(l) = 'object'
  AND JSON_VALUE(l, '$."@type"') IN (
    'boardgamecategory', 'boardgamemechanic', 'boardgamefamily', 'boardgameexpansion',
    'boardgameimplementation', 'boardgamedesigner', 'boardgameartist', 'boardgamepublisher')
```

- [ ] **Step 2: Write `compare_bridges.sql`**

Bridge rule: distinct `(game_id, entity_id)`. Implementations count only when
`NOT inbound`. Expansions count in both directions, as the processor does.

```sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
parsed AS (
  SELECT DISTINCT
    CASE p.link_type
      WHEN 'boardgamecategory' THEN 'game_categories'
      WHEN 'boardgamemechanic' THEN 'game_mechanics'
      WHEN 'boardgamefamily' THEN 'game_families'
      WHEN 'boardgameexpansion' THEN 'game_expansions'
      WHEN 'boardgameimplementation' THEN 'game_implementations'
      WHEN 'boardgamedesigner' THEN 'game_designers'
      WHEN 'boardgameartist' THEN 'game_artists'
      WHEN 'boardgamepublisher' THEN 'game_publishers'
    END AS tbl, p.game_id, p.entity_id
  FROM `bgg-data-warehouse.scratch_parsing.parsed_links` p
  JOIN games g USING (record_id)
  WHERE NOT (p.link_type = 'boardgameimplementation' AND p.inbound)
),
core_rows AS (
  SELECT 'game_categories' tbl, game_id, category_id entity_id FROM `bgg-data-warehouse.core.game_categories`
  UNION ALL SELECT 'game_mechanics', game_id, mechanic_id FROM `bgg-data-warehouse.core.game_mechanics`
  UNION ALL SELECT 'game_families', game_id, family_id FROM `bgg-data-warehouse.core.game_families`
  UNION ALL SELECT 'game_expansions', game_id, expansion_id FROM `bgg-data-warehouse.core.game_expansions`
  UNION ALL SELECT 'game_implementations', game_id, implementation_id FROM `bgg-data-warehouse.core.game_implementations`
  UNION ALL SELECT 'game_designers', game_id, designer_id FROM `bgg-data-warehouse.core.game_designers`
  UNION ALL SELECT 'game_artists', game_id, artist_id FROM `bgg-data-warehouse.core.game_artists`
  UNION ALL SELECT 'game_publishers', game_id, publisher_id FROM `bgg-data-warehouse.core.game_publishers`
),
core_scoped AS (SELECT DISTINCT * FROM core_rows WHERE game_id IN (SELECT game_id FROM games)),
only_core AS (SELECT * FROM core_scoped EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_scoped)
SELECT tbl,
  COUNTIF(side = 'core') AS only_in_core, COUNTIF(side = 'parsed') AS only_in_parsed,
  ARRAY_AGG(STRUCT(side, game_id, entity_id) ORDER BY game_id LIMIT 20) AS sample
FROM (SELECT 'core' side, * FROM only_core UNION ALL SELECT 'parsed', * FROM only_parsed)
GROUP BY tbl
ORDER BY tbl
```

Also record `SELECT tbl, COUNT(*) FROM core_rows GROUP BY tbl` for duplicate rows in
core (`COUNT(*)` vs `COUNT(DISTINCT …)`). Duplicates are a processor behaviour.

- [ ] **Step 3: Write `compare_dimensions.sql`**

The dimension tables are insert-only (MERGE `WHEN NOT MATCHED`), so the first name ever
loaded wins. The compare uses every response that was processed successfully, and the
earliest name seen per id.

```sql
WITH processed AS (
  SELECT DISTINCT record_id FROM `bgg-data-warehouse.raw.processed_responses` WHERE process_status = 'success'
),
parsed AS (
  SELECT
    CASE link_type
      WHEN 'boardgamecategory' THEN 'categories' WHEN 'boardgamemechanic' THEN 'mechanics'
      WHEN 'boardgamefamily' THEN 'families' WHEN 'boardgamedesigner' THEN 'designers'
      WHEN 'boardgameartist' THEN 'artists' WHEN 'boardgamepublisher' THEN 'publishers'
    END AS tbl,
    entity_id,
    ARRAY_AGG(name ORDER BY fetch_timestamp, record_id LIMIT 1)[OFFSET(0)] AS first_name
  FROM `bgg-data-warehouse.scratch_parsing.parsed_links`
  WHERE record_id IN (SELECT record_id FROM processed)
    AND link_type NOT IN ('boardgameexpansion', 'boardgameimplementation')
  GROUP BY 1, 2
),
core_rows AS (
  SELECT 'categories' tbl, category_id entity_id, name FROM `bgg-data-warehouse.core.categories`
  UNION ALL SELECT 'mechanics', mechanic_id, name FROM `bgg-data-warehouse.core.mechanics`
  UNION ALL SELECT 'families', family_id, name FROM `bgg-data-warehouse.core.families`
  UNION ALL SELECT 'designers', designer_id, name FROM `bgg-data-warehouse.core.designers`
  UNION ALL SELECT 'artists', artist_id, name FROM `bgg-data-warehouse.core.artists`
  UNION ALL SELECT 'publishers', publisher_id, name FROM `bgg-data-warehouse.core.publishers`
)
SELECT
  COALESCE(c.tbl, p.tbl) AS tbl,
  COUNTIF(p.entity_id IS NULL) AS id_only_in_core,
  COUNTIF(c.entity_id IS NULL) AS id_only_in_parsed,
  COUNTIF(c.entity_id IS NOT NULL AND p.entity_id IS NOT NULL AND c.name != p.first_name) AS name_differs,
  ARRAY_AGG(IF(c.name != p.first_name, STRUCT(c.entity_id, c.name, p.first_name), NULL) IGNORE NULLS LIMIT 20) AS sample_name_diffs
FROM core_rows c
FULL JOIN parsed p ON c.tbl = p.tbl AND c.entity_id = p.entity_id
GROUP BY 1
ORDER BY 1
```

This compare is only meaningful on full input (Task 9). On the sample, expect many
`id_only_in_core` rows. Check that it runs, and leave the counts for Task 9.

- [ ] **Step 4: Run on the sample (dry-run, approval, capped run). Fix or classify
  every bridge difference.**

- [ ] **Step 5: Commit**

```bash
git add analysis/sql-response-parsing/sql/parse_links.sql analysis/sql-response-parsing/sql/compare_bridges.sql analysis/sql-response-parsing/sql/compare_dimensions.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): parse links into bridges and dimensions; compare with core"
```

---

### Task 7: Polls (player counts, language dependence, suggested ages)

**Files:**
- Create: `sql/parse_player_counts.sql`, `sql/parse_language_dependence.sql`,
  `sql/parse_suggested_ages.sql`, `sql/compare_player_counts.sql`,
  `sql/compare_language_dependence.sql`, `sql/compare_suggested_ages.sql`

**Interfaces:**
- Consumes: `parsed_items`, `core_inputs`
- Produces: tables
  - `parsed_player_counts` (`record_id`, `game_id`, `fetch_timestamp`,
    `player_count`, `best_votes`, `recommended_votes`, `not_recommended_votes`)
  - `parsed_language_dependence` (… `level`, `description`, `votes`, `level_raw`,
    `votes_raw`)
  - `parsed_suggested_ages` (… `age`, `votes`, `votes_raw`)

- [ ] **Step 1: Write `parse_player_counts.sql`**

```sql
-- processor._extract_poll_results, suggested_numplayers: first vote per value, else 0.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_player_counts`
CLUSTER BY game_id, record_id AS
SELECT
  i.record_id, i.game_id, i.fetch_timestamp,
  JSON_VALUE(res, '$."@numplayers"') AS player_count,
  IFNULL((SELECT SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@numvotes"'), '0') AS INT64)
          FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(res, '$.result'))) v WITH OFFSET o
          WHERE JSON_VALUE(v, '$."@value"') = 'Best' ORDER BY o LIMIT 1), 0) AS best_votes,
  IFNULL((SELECT SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@numvotes"'), '0') AS INT64)
          FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(res, '$.result'))) v WITH OFFSET o
          WHERE JSON_VALUE(v, '$."@value"') = 'Recommended' ORDER BY o LIMIT 1), 0) AS recommended_votes,
  IFNULL((SELECT SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@numvotes"'), '0') AS INT64)
          FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(res, '$.result'))) v WITH OFFSET o
          WHERE JSON_VALUE(v, '$."@value"') = 'Not Recommended' ORDER BY o LIMIT 1), 0) AS not_recommended_votes
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.poll'))) p,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(p, '$.results'))) res
WHERE JSON_VALUE(p, '$."@name"') = 'suggested_numplayers'
```

- [ ] **Step 2: Write `parse_language_dependence.sql`**

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_language_dependence`
CLUSTER BY game_id, record_id AS
SELECT
  i.record_id, i.game_id, i.fetch_timestamp,
  SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@level"'), '0') AS INT64) AS level,
  IFNULL(JSON_VALUE(v, '$."@value"'), '') AS description,
  SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@numvotes"'), '0') AS INT64) AS votes,
  JSON_VALUE(v, '$."@level"') AS level_raw,
  JSON_VALUE(v, '$."@numvotes"') AS votes_raw
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.poll'))) p,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(p, '$.results.result'))) v
WHERE JSON_VALUE(p, '$."@name"') = 'language_dependence'
  AND JSON_TYPE(v) = 'object'
```

- [ ] **Step 3: Write `parse_suggested_ages.sql`**

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_suggested_ages`
CLUSTER BY game_id, record_id AS
SELECT
  i.record_id, i.game_id, i.fetch_timestamp,
  IFNULL(JSON_VALUE(v, '$."@value"'), '') AS age,
  SAFE_CAST(IFNULL(JSON_VALUE(v, '$."@numvotes"'), '0') AS INT64) AS votes,
  JSON_VALUE(v, '$."@numvotes"') AS votes_raw
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.poll'))) p,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(p, '$.results.result'))) v
WHERE JSON_VALUE(p, '$."@name"') = 'suggested_playerage'
```

- [ ] **Step 4: Write the three compares**

```sql
-- sql/compare_player_counts.sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT game_id, player_count, best_votes, recommended_votes, not_recommended_votes
              FROM `bgg-data-warehouse.core.player_counts` WHERE game_id IN (SELECT game_id FROM games)),
parsed AS (SELECT p.game_id, p.player_count, p.best_votes, p.recommended_votes, p.not_recommended_votes
           FROM `bgg-data-warehouse.scratch_parsing.parsed_player_counts` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'player_counts' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

```sql
-- sql/compare_language_dependence.sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT game_id, level, description, votes
              FROM `bgg-data-warehouse.core.language_dependence` WHERE game_id IN (SELECT game_id FROM games)),
parsed AS (SELECT p.game_id, p.level, p.description, p.votes
           FROM `bgg-data-warehouse.scratch_parsing.parsed_language_dependence` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'language_dependence' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

```sql
-- sql/compare_suggested_ages.sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT game_id, age, votes
              FROM `bgg-data-warehouse.core.suggested_ages` WHERE game_id IN (SELECT game_id FROM games)),
parsed AS (SELECT p.game_id, p.age, p.votes
           FROM `bgg-data-warehouse.scratch_parsing.parsed_suggested_ages` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'suggested_ages' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

- [ ] **Step 5: Run on the sample (dry-run, approval, capped run). Fix or classify
  every difference into `FINDINGS.md`.**

The loader quirk from Task 5 applies here too: when a refresh produces an empty poll,
the game's old rows are left in place.

- [ ] **Step 6: Commit**

```bash
git add analysis/sql-response-parsing/sql/parse_player_counts.sql analysis/sql-response-parsing/sql/parse_language_dependence.sql analysis/sql-response-parsing/sql/parse_suggested_ages.sql analysis/sql-response-parsing/sql/compare_player_counts.sql analysis/sql-response-parsing/sql/compare_language_dependence.sql analysis/sql-response-parsing/sql/compare_suggested_ages.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): parse polls; compare with core"
```

---

### Task 8: Rankings

**Files:**
- Create: `sql/parse_rankings.sql`, `sql/compare_rankings.sql`

**Interfaces:**
- Consumes: `parsed_items`, `core_inputs`
- Produces: table `parsed_rankings` (`record_id`, `game_id`, `ranking_type`,
  `ranking_name`, `friendly_name`, `value`, `bayes_average`, `load_timestamp`)

- [ ] **Step 1: Write `parse_rankings.sql`**

```sql
-- processor.GameRanks: skip @value == 'Not Ranked' (a missing @value is kept).
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_rankings`
CLUSTER BY game_id, record_id AS
SELECT
  i.record_id, i.game_id,
  IFNULL(JSON_VALUE(r, '$."@type"'), '') AS ranking_type,
  IFNULL(JSON_VALUE(r, '$."@name"'), '') AS ranking_name,
  IFNULL(JSON_VALUE(r, '$."@friendlyname"'), '') AS friendly_name,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(r, '$."@value"')) AS value,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(r, '$."@bayesaverage"')) AS bayes_average,
  i.fetch_timestamp AS load_timestamp
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i,
  UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.statistics.ratings.ranks.rank'))) r
WHERE JSON_TYPE(r) = 'object'
  AND (JSON_VALUE(r, '$."@value"') IS NULL OR JSON_VALUE(r, '$."@value"') != 'Not Ranked')
```

- [ ] **Step 2: Write `compare_rankings.sql`**

`core.rankings` is append-only, keyed by `load_timestamp`. The compare takes the
rows for each game's `core_inputs.fetch_timestamp`.

```sql
WITH games AS (SELECT game_id, record_id, fetch_timestamp FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT r.game_id, r.ranking_type, r.ranking_name, r.friendly_name, r.value, r.bayes_average, r.load_timestamp
              FROM `bgg-data-warehouse.core.rankings` r
              JOIN games g ON g.game_id = r.game_id AND g.fetch_timestamp = r.load_timestamp),
parsed AS (SELECT p.game_id, p.ranking_type, p.ranking_name, p.friendly_name, p.value, p.bayes_average, p.load_timestamp
           FROM `bgg-data-warehouse.scratch_parsing.parsed_rankings` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'rankings' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed
```

`core.rankings` has about 112k rows against about 488k `core.games` rows, so it may not
hold rows for every load. If `core_rows` is far below `parsed_rows`, compare per game
on the latest `load_timestamp` actually present in `core.rankings` instead, and record
the gap in `FINDINGS.md` as a processor behaviour.

- [ ] **Step 3: Run on the sample (dry-run, approval, capped run). Fix or classify.**

- [ ] **Step 4: Commit**

```bash
git add analysis/sql-response-parsing/sql/parse_rankings.sql analysis/sql-response-parsing/sql/compare_rankings.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): parse rankings; compare with core"
```

---

### Task 9: Full population, edge tags and strict-int checks

**Files:**
- Create: `sql/edge_tags.sql`, `sql/checks_strict_ints.sql`
- Modify: `FINDINGS.md`

**Interfaces:**
- Consumes: every `parsed_*` table, `responses_json`, `core_inputs`
- Produces: table `edge_tags` (`record_id`, `game_id`, `fetch_quarter`, `tags ARRAY<STRING>`)

- [ ] **Step 1: Write `edge_tags.sql`**

```sql
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.edge_tags` AS
SELECT
  r.record_id, r.game_id,
  FORMAT_TIMESTAMP('%Y-Q%Q', r.fetch_timestamp) AS fetch_quarter,
  ARRAY(SELECT t FROM UNNEST([
    IF(r.convert_status = 'empty', 'convert_empty', NULL),
    IF(r.convert_status = 'unparseable', 'convert_unparseable', NULL),
    IF(r.convert_status = 'json', 'stored_as_json', NULL),
    IF(i.record_id IS NULL AND r.convert_status IN ('repr', 'json'), 'no_matching_item', NULL),
    IF(JSON_VALUE(i.item, '$."@type"') = 'boardgameexpansion', 'expansion', NULL),
    IF(JSON_TYPE(JSON_QUERY(i.item, '$.name')) = 'object', 'single_name', NULL),
    IF(JSON_TYPE(JSON_QUERY(i.item, '$.name')) = 'string', 'bare_string_name', NULL),
    IF(JSON_TYPE(JSON_QUERY(i.item, '$.link')) = 'object', 'single_link', NULL),
    IF(JSON_QUERY(i.item, '$.link') IS NULL, 'no_links', NULL),
    IF(JSON_QUERY(i.item, '$.poll') IS NULL OR JSON_TYPE(JSON_QUERY(i.item, '$.poll')) = 'null', 'no_polls', NULL),
    IF(JSON_TYPE(JSON_QUERY(i.item, '$.statistics.ratings.ranks.rank')) = 'object', 'single_rank', NULL),
    IF(EXISTS(SELECT 1 FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.statistics.ratings.ranks.rank'))) x
              WHERE JSON_VALUE(x, '$."@value"') = 'Not Ranked'), 'not_ranked', NULL),
    IF(g.year_published IS NULL, 'year_missing_or_zero', NULL),
    IF(g.year_published < 0, 'year_negative', NULL),
    IF(JSON_QUERY(i.item, '$.description') IS NULL, 'description_missing', NULL),
    IF(JSON_TYPE(JSON_QUERY(i.item, '$.description')) = 'null', 'description_null', NULL),
    IF(REGEXP_CONTAINS(JSON_VALUE(i.item, '$.description'), r'&#?\w+;'), 'html_entities', NULL),
    IF(REGEXP_CONTAINS(JSON_VALUE(i.item, '$.description'), r'[^\x00-\x7F]'), 'non_ascii', NULL),
    IF(SAFE_CAST(TRIM(JSON_VALUE(i.item, '$.statistics.ratings.average."@value"')) AS FLOAT64) IS NULL
       AND JSON_VALUE(i.item, '$.statistics.ratings.average."@value"') IS NOT NULL, 'safe_float_fallback', NULL),
    IF(SAFE_CAST(TRIM(JSON_VALUE(i.item, '$.minplayers."@value"')) AS INT64) IS NULL
       AND JSON_VALUE(i.item, '$.minplayers."@value"') IS NOT NULL, 'safe_int_fallback', NULL)
  ]) t WHERE t IS NOT NULL) AS tags
FROM `bgg-data-warehouse.scratch_parsing.responses_input` r
LEFT JOIN `bgg-data-warehouse.scratch_parsing.parsed_items` i USING (record_id)
LEFT JOIN `bgg-data-warehouse.scratch_parsing.parsed_games` g USING (record_id)
```

- [ ] **Step 2: Write `checks_strict_ints.sql`**

These are values where the processor's plain `int()` would have raised and failed the
whole game. The SQL must not hide them.

```sql
SELECT 'link @id' AS field, COUNT(*) AS n, ARRAY_AGG(STRUCT(record_id, id_raw) LIMIT 10) AS sample
FROM `bgg-data-warehouse.scratch_parsing.parsed_links`
WHERE id_raw IS NOT NULL AND SAFE_CAST(id_raw AS INT64) IS NULL
UNION ALL
SELECT 'language @level', COUNT(*), ARRAY_AGG(STRUCT(record_id, level_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_language_dependence`
WHERE level_raw IS NOT NULL AND SAFE_CAST(level_raw AS INT64) IS NULL
UNION ALL
SELECT 'language @numvotes', COUNT(*), ARRAY_AGG(STRUCT(record_id, votes_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_language_dependence`
WHERE votes_raw IS NOT NULL AND SAFE_CAST(votes_raw AS INT64) IS NULL
UNION ALL
SELECT 'age @numvotes', COUNT(*), ARRAY_AGG(STRUCT(record_id, votes_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_suggested_ages`
WHERE votes_raw IS NOT NULL AND SAFE_CAST(votes_raw AS INT64) IS NULL
```

Expected: every `n = 0`. If a value is non-zero, check `processed_responses` for those
records. They should have status `failed` or `error`. Record the result in
`FINDINGS.md`.

- [ ] **Step 3: Switch to full input and rebuild everything**

Dry-run every file in this order: `input_full`, `parse_items`, `parse_games`,
`parse_names`, `parse_links`, `parse_player_counts`, `parse_language_dependence`,
`parse_suggested_ages`, `parse_rankings`, `edge_tags`. Report the total GB, get **one
approval for the batch** at a 1.5× cap per file, then run them in that order.

- [ ] **Step 4: Run every compare and `checks_strict_ints` on full input**

Dry-run them, get approval, run them, and paste each result into `FINDINGS.md`.

- [ ] **Step 5: Mismatches by edge tag**

Count mismatches per edge tag:

```sql
-- sql/edge_tag_mismatches.sql : games-table mismatches per edge tag and per fetch quarter
WITH core_latest AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT g.*, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_timestamp DESC) rn
    FROM `bgg-data-warehouse.core.games` g) WHERE rn = 1),
parsed AS (
  SELECT p.record_id, TO_JSON_STRING(p) AS row_json
  FROM (SELECT * EXCEPT(item_type) FROM `bgg-data-warehouse.scratch_parsing.parsed_games`) p
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id)),
core_json AS (
  SELECT c.record_id, TO_JSON_STRING(STRUCT(g.*, c.record_id AS record_id)) AS row_json
  FROM core_latest g JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (game_id)),
mismatched AS (
  SELECT p.record_id FROM parsed p
  LEFT JOIN core_json k ON k.record_id = p.record_id AND k.row_json = p.row_json
  WHERE k.record_id IS NULL),
tagged AS (
  SELECT e.record_id, e.fetch_quarter, e.tags
  FROM `bgg-data-warehouse.scratch_parsing.edge_tags` e
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id))
SELECT tag AS bucket, COUNT(*) AS responses, COUNTIF(m.record_id IS NOT NULL) AS games_mismatched
FROM tagged t, UNNEST(t.tags) tag
LEFT JOIN mismatched m ON m.record_id = t.record_id
GROUP BY tag
UNION ALL
SELECT CONCAT('era ', t.fetch_quarter), COUNT(*), COUNTIF(m.record_id IS NOT NULL)
FROM tagged t
LEFT JOIN mismatched m ON m.record_id = t.record_id
GROUP BY t.fetch_quarter
```

`TO_JSON_STRING` compares rows field by field, NULLs included. The two sides line up
because `parsed_games` (minus `item_type`) has `core.games`' columns in the same order,
followed by `record_id`. Run it on the sample first, and check that `games_mismatched`
equals `compare_games`' `only_in_parsed`.

Expected: every tag in the Review Focus list has `responses > 0`. Each tag's mismatches
are already explained by a classified row in `FINDINGS.md`. Add the table to
`FINDINGS.md`.

- [ ] **Step 6: Commit**

```bash
git add analysis/sql-response-parsing/sql/edge_tags.sql analysis/sql-response-parsing/sql/checks_strict_ints.sql analysis/sql-response-parsing/sql/edge_tag_mismatches.sql analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): full-population compare, edge tags and strict-int checks"
```

---

### Task 10: An older date works

**Files:**
- Create: `sql/asof_snapshot.sql`, `sql/asof_checks.sql`, `check_asof.py`
- Test: `test_check_asof.py`

**Interfaces:**
- Consumes: `parsed_games`, `parsed_rankings`, `responses_json`, `raw.fetched_responses`
- Produces:
  - Table `asof_inputs` (`game_id`, `record_id`, `fetch_timestamp`)
  - `check_asof.expected_from_repr(response_data: str, game_id: int) -> dict` with
    keys `users_rated`, `average_rating`, `bayes_average`, `owned_count`, `ranks`

- [ ] **Step 1: Write `asof_snapshot.sql`**

```sql
-- Latest successfully fetched, convertible response per game at or before @as_of.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.asof_inputs` AS
SELECT r.game_id, r.record_id, r.fetch_timestamp
FROM `bgg-data-warehouse.scratch_parsing.responses_json` r
JOIN `bgg-data-warehouse.raw.fetched_responses` f
  ON f.record_id = r.record_id AND f.fetch_status = 'success'
WHERE r.fetch_timestamp <= @as_of
  AND r.convert_status IN ('repr', 'json')
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.game_id ORDER BY r.fetch_timestamp DESC, r.record_id DESC) = 1
```

- [ ] **Step 2: Write `asof_checks.sql`**

```sql
WITH today AS (SELECT g.* FROM `bgg-data-warehouse.scratch_parsing.parsed_games` g
               JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` USING (record_id)),
past AS (SELECT g.* FROM `bgg-data-warehouse.scratch_parsing.parsed_games` g
         JOIN `bgg-data-warehouse.scratch_parsing.asof_inputs` USING (record_id)),
first_fetch AS (SELECT game_id, MIN(fetch_timestamp) AS first_ts
                FROM `bgg-data-warehouse.scratch_parsing.responses_json` GROUP BY game_id)
SELECT
  (SELECT MIN(fetch_timestamp) FROM `bgg-data-warehouse.scratch_parsing.responses_json`) AS earliest_fetch,
  (SELECT COUNT(*) FROM today) AS games_today,
  (SELECT COUNT(*) FROM past) AS games_as_of,
  (SELECT COUNT(*) FROM past p JOIN first_fetch f USING (game_id) WHERE f.first_ts > @as_of) AS past_games_first_fetched_after,  -- must be 0
  (SELECT COUNT(*) FROM past WHERE load_timestamp > @as_of) AS past_rows_after_as_of,                                    -- must be 0
  (SELECT COUNT(*) FROM today t JOIN past p USING (game_id) WHERE t.users_rated != p.users_rated) AS users_rated_changed,
  (SELECT COUNTIF(t.users_rated < p.users_rated) FROM today t JOIN past p USING (game_id)) AS users_rated_went_down
```

Expected: `earliest_fetch` is before `@as_of`, `games_as_of < games_today`, both
"must be 0" columns are 0, and `users_rated_changed > 0`. A few `users_rated_went_down`
can be legitimate (BGG removes ratings). Record the count in `FINDINGS.md`.

- [ ] **Step 3: Write the failing test for `check_asof.expected_from_repr`**

```python
# analysis/sql-response-parsing/test_check_asof.py
from check_asof import expected_from_repr

REPR = str({"items": {"item": {"@id": "13", "statistics": {"ratings": {
    "usersrated": {"@value": "120"}, "average": {"@value": "7.5"}, "bayesaverage": {"@value": "6.9"},
    "owned": {"@value": "300"},
    "ranks": {"rank": [
        {"@type": "subtype", "@name": "boardgame", "@friendlyname": "Board Game Rank", "@value": "42", "@bayesaverage": "6.9"},
        {"@type": "family", "@name": "strategygames", "@friendlyname": "Strategy", "@value": "Not Ranked", "@bayesaverage": "Not Ranked"},
    ]}}}}}})


def test_uses_the_processor_to_get_expected_values():
    got = expected_from_repr(REPR, 13)
    assert got["users_rated"] == 120
    assert got["average_rating"] == 7.5
    assert got["owned_count"] == 300
    assert got["ranks"] == [("subtype", "boardgame", 42)]
```

- [ ] **Step 4: Run it to verify it fails**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_check_asof.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'check_asof'`.

- [ ] **Step 5: Implement `check_asof.py`**

This is an independent oracle. It runs the **real processor** on the **original repr**
of each sampled as-of record and compares the result with the SQL snapshot.

```python
"""Spot-check an as-of snapshot against the processor run on the original repr.

  uv run python analysis/sql-response-parsing/check_asof.py --n 50
"""

import argparse
import ast
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.data_processor.processor import BGGDataProcessor  # noqa: E402

PROJECT = "bgg-data-warehouse"


def expected_from_repr(response_data: str, game_id: int) -> dict:
    game = BGGDataProcessor().process_game(game_id, ast.literal_eval(response_data), "boardgame")
    return {
        "users_rated": game["users_rated"],
        "average_rating": game["average_rating"],
        "bayes_average": game["bayes_average"],
        "owned_count": game["owned_count"],
        "ranks": sorted((r["type"], r["name"], r["value"]) for r in game["rankings"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=50)
    args = parser.parse_args()
    client = bigquery.Client(project=PROJECT)
    sample = list(client.query(f"""
        SELECT a.game_id, a.record_id, a.fetch_timestamp,
               g.users_rated, g.average_rating, g.bayes_average, g.owned_count,
               ARRAY(SELECT AS STRUCT ranking_type, ranking_name, value FROM `{PROJECT}.scratch_parsing.parsed_rankings` r
                     WHERE r.record_id = a.record_id ORDER BY 1, 2, 3) AS ranks
        FROM `{PROJECT}.scratch_parsing.asof_inputs` a
        JOIN `{PROJECT}.scratch_parsing.parsed_games` g USING (record_id)
        JOIN `{PROJECT}.scratch_parsing.core_inputs` c ON c.game_id = a.game_id AND c.record_id != a.record_id
        ORDER BY FARM_FINGERPRINT(a.record_id) LIMIT {args.n}
    """, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=int(2e9))).result())
    failures = 0
    for row in sample:
        raw = list(client.query(f"""
            SELECT response_data FROM `{PROJECT}.raw.raw_responses`
            WHERE record_id = @r AND game_id = @g AND fetch_timestamp = @t
        """, job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=int(2e8),
            query_parameters=[bigquery.ScalarQueryParameter("r", "STRING", row["record_id"]),
                              bigquery.ScalarQueryParameter("g", "INT64", row["game_id"]),
                              bigquery.ScalarQueryParameter("t", "TIMESTAMP", row["fetch_timestamp"])])).result())[0]
        want = expected_from_repr(raw["response_data"], row["game_id"])
        got = {k: row[k] for k in ("users_rated", "average_rating", "bayes_average", "owned_count")}
        got["ranks"] = sorted((r["ranking_type"], r["ranking_name"], r["value"]) for r in row["ranks"])
        if got != want:
            failures += 1
            print("MISMATCH", row["game_id"], row["record_id"], got, want)
    print(f"checked {len(sample)}, mismatched {failures}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
```

The sample query only picks games whose as-of record differs from today's record, so
the check really exercises an older response.

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing/test_check_asof.py -q`
Expected: PASS.

- [ ] **Step 7: Run the as-of check for 2026-04-05**

Dry-run `asof_snapshot.sql` and `asof_checks.sql` with
`--param as_of:TIMESTAMP:2026-04-05T00:00:00Z`, get approval, and run them. Then tell
Phil the cost of `check_asof.py --n 50` (about 2 GB for the sample query, plus 50 point
lookups at the 10 MB minimum each, ≈ 2.5 GB), get approval, and run it.
Expected: the `asof_checks` expectations hold, and `checked 50, mismatched 0`.

- [ ] **Step 8: Commit**

```bash
git add analysis/sql-response-parsing/sql/asof_snapshot.sql analysis/sql-response-parsing/sql/asof_checks.sql analysis/sql-response-parsing/check_asof.py analysis/sql-response-parsing/test_check_asof.py analysis/sql-response-parsing/FINDINGS.md
git commit -m "feat(analysis): as-of snapshot and independent spot-check against the processor"
```

---

### Task 11: Verdict and PR

**Files:**
- Modify: `analysis/sql-response-parsing/FINDINGS.md`

- [ ] **Step 1: Write the verdict at the top of `FINDINGS.md`**

```markdown
## Verdict
- Conversion: <N> rows, <mismatched> stored-hash mismatches.
- Full-population compare: per table, only_in_core / only_in_parsed, all classified (see Differences).
- Edge tags: every Review Focus tag present (counts), mismatches explained.
- Strict-int checks: <results>.
- As-of 2026-04-05: <asof_checks row>; processor spot-check <checked>/<mismatched>.
- **Proof <succeeds | does not succeed>** against the spec's criterion: layers 1–4 show no unexplained differences and layer 5 behaves as expected.
- Processor behaviours found (for the cutover spec): <list>.
```

- [ ] **Step 2: Run the analysis tests**

Run: `uv run --extra test python -m pytest analysis/sql-response-parsing -q`
Expected: all PASS.

- [ ] **Step 3: Confirm that nothing outside the allowed paths changed**

Run: `git diff --stat origin/main...HEAD -- src definitions .github terraform`
Expected: no output.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin feat/sql-response-parsing
gh pr create --base main --title "feat(analysis): prove BGG response parsing in SQL" --body-file <(cat <<'EOF'
## What
Throwaway proof (spec: docs/superpowers/specs/2026-10-05-sql-response-parsing-design.md) that BGG responses can be parsed in BigQuery SQL with nothing lost vs the Python processor, and that an older-date snapshot works.

## Result
<paste the Verdict section>

## Safety
All writes went to `scratch_parsing` (30-day table expiry); nothing in src/, definitions/, .github/ or terraform/ changed.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)
```

- [ ] **Step 5: Ask Phil about cleanup**

Ask Phil whether to `bq rm -r -f bgg-data-warehouse:scratch_parsing` now, or keep the
dataset for the cutover spec. Don't remove it unasked.
