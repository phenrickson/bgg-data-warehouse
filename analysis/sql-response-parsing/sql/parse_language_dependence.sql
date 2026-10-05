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
