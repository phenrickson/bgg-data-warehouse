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
