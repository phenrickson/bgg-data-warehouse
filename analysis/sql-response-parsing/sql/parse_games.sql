CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_games`
PARTITION BY DATE(load_timestamp)
CLUSTER BY game_id AS
WITH named AS (
  SELECT
    i.record_id,
    -- processor._extract_names: last primary wins; default 'Unknown'
    IFNULL((
      SELECT JSON_VALUE(n, '$."@value"')
      FROM UNNEST(`bgg-data-warehouse.scratch_parsing.as_array`(JSON_QUERY(i.item, '$.name'))) n WITH OFFSET o
      WHERE JSON_TYPE(n) = 'object' AND JSON_VALUE(n, '$."@type"') = 'primary'
      ORDER BY o DESC LIMIT 1
    ), 'Unknown') AS primary_name
  FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i
),
yr AS (
  SELECT record_id,
    CASE JSON_TYPE(JSON_QUERY(item, '$.yearpublished'))
      WHEN 'string' THEN JSON_VALUE(item, '$.yearpublished')
      ELSE JSON_VALUE(item, '$.yearpublished."@value"')
    END AS year_str
  FROM `bgg-data-warehouse.scratch_parsing.parsed_items`
)
SELECT
  i.game_id,
  'boardgame' AS type,                              -- process_batch hard-codes this
  JSON_VALUE(i.item, '$."@type"') AS item_type,     -- what BGG actually says
  n.primary_name,
  CAST(NULLIF(SAFE_CAST(TRIM(y.year_str) AS INT64), 0) AS FLOAT64) AS year_published,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minplayers."@value"')) AS min_players,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.maxplayers."@value"')) AS max_players,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.playingtime."@value"')) AS playing_time,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minplaytime."@value"')) AS min_playtime,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.maxplaytime."@value"')) AS max_playtime,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.minage."@value"')) AS min_age,
  -- item.get(key, ""): missing key -> '', present-but-null -> NULL
  IF(JSON_QUERY(i.item, '$.description') IS NULL, '', JSON_VALUE(i.item, '$.description')) AS description,
  IF(JSON_QUERY(i.item, '$.thumbnail') IS NULL, '', JSON_VALUE(i.item, '$.thumbnail')) AS thumbnail,
  IF(JSON_QUERY(i.item, '$.image') IS NULL, '', JSON_VALUE(i.item, '$.image')) AS image,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.usersrated."@value"')) AS users_rated,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.average."@value"')) AS average_rating,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.bayesaverage."@value"')) AS bayes_average,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.stddev."@value"')) AS standard_deviation,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.median."@value"')) AS median_rating,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.owned."@value"')) AS owned_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.trading."@value"')) AS trading_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.wanting."@value"')) AS wanting_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.wishing."@value"')) AS wishing_count,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.numcomments."@value"')) AS num_comments,
  `bgg-data-warehouse.scratch_parsing.safe_int`(JSON_VALUE(i.item, '$.statistics.ratings.numweights."@value"')) AS num_weights,
  `bgg-data-warehouse.scratch_parsing.safe_float`(JSON_VALUE(i.item, '$.statistics.ratings.averageweight."@value"')) AS average_weight,
  i.fetch_timestamp AS load_timestamp,
  i.record_id
FROM `bgg-data-warehouse.scratch_parsing.parsed_items` i
JOIN named n USING (record_id)
JOIN yr y USING (record_id)
