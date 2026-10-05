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
