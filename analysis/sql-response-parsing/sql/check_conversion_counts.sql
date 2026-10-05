SELECT
  (SELECT COUNT(*) FROM `bgg-data-warehouse.raw.raw_responses`) AS raw_rows,
  (SELECT COUNT(DISTINCT record_id) FROM `bgg-data-warehouse.raw.raw_responses`) AS raw_ids,
  (SELECT COUNT(*) FROM `bgg-data-warehouse.scratch_parsing.responses_json`) AS json_rows,
  (SELECT COUNT(*) FROM (
     SELECT record_id FROM `bgg-data-warehouse.raw.raw_responses`
     EXCEPT DISTINCT
     SELECT record_id FROM `bgg-data-warehouse.scratch_parsing.responses_json`)) AS missing_ids
