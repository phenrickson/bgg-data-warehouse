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
