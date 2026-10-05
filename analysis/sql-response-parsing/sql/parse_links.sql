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
