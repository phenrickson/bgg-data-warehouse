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
