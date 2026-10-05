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
