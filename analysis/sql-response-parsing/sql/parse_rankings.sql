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
