# Findings — SQL response parsing proof

## Conversion
- Rows: 497,879 in `raw.raw_responses`, 497,879 distinct `record_id`s; 497,879 in `responses_json`, 0 missing ids.
- `convert.py` status counts: `repr` 494,251; `empty` 3,628; `json` 0; `unparseable` 0. No lossy JSON round trips.
- Stored-hash verification (`verify_conversion.py`): 497,879 checked, 0 mismatched.
- Trial (1,000 rows): `JSON_TYPE(response_json) = 'object'` for every converted row.
- Notes: the 3,628 `empty` rows have blank `response_data`; to be reconciled against `no_response` in `processed_responses` (Task 9 edge tags).

## Differences
| Table | Difference | Count | Classification | Resolution |
|---|---|---|---|---|
| games | Game 1's `core.games` row was built from a response `processed_responses` records as `failed` (2025-06-16, older processor); `core_inputs` originally required `success` and excluded it | 3 games warehouse-wide (141,561 vs 141,558 in `core_inputs`) | Processor behaviour | `core_inputs` matches on timestamp; status is only a tie-breaker. Recorded, not reproduced. |

### Sample results (5,000 games)
- `games`: 5,000 core rows vs 5,000 parsed; 0 only-in-core, 0 only-in-parsed — exact match on all 26 columns.
- `alternate_names`: 2,692 core rows vs 2,692 parsed; 0 only-in-core, 0 only-in-parsed — exact match.
- Bridge tables (all 8): exact match, identical row counts, no duplicates in core — artists 3,180; categories 12,200; designers 4,937; expansions 1,640; families 8,593; implementations 277; mechanics 12,011; publishers 7,684.
- Dimensions: compare runs; only meaningful on full population (Task 9). Sample shows a few core names differing from the earliest parsed name (e.g. designer 90564 core "Nathan Jenne" vs earliest parsed "Nate Jenne") — to classify in Task 9.
