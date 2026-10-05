WITH processed AS (
  SELECT DISTINCT record_id FROM `bgg-data-warehouse.raw.processed_responses` WHERE process_status = 'success'
),
parsed AS (
  SELECT
    CASE link_type
      WHEN 'boardgamecategory' THEN 'categories' WHEN 'boardgamemechanic' THEN 'mechanics'
      WHEN 'boardgamefamily' THEN 'families' WHEN 'boardgamedesigner' THEN 'designers'
      WHEN 'boardgameartist' THEN 'artists' WHEN 'boardgamepublisher' THEN 'publishers'
    END AS tbl,
    entity_id,
    ARRAY_AGG(name ORDER BY fetch_timestamp, record_id LIMIT 1)[OFFSET(0)] AS first_name
  FROM `bgg-data-warehouse.scratch_parsing.parsed_links`
  WHERE record_id IN (SELECT record_id FROM processed)
    AND link_type NOT IN ('boardgameexpansion', 'boardgameimplementation')
  GROUP BY 1, 2
),
core_rows AS (
  SELECT 'categories' tbl, category_id entity_id, name FROM `bgg-data-warehouse.core.categories`
  UNION ALL SELECT 'mechanics', mechanic_id, name FROM `bgg-data-warehouse.core.mechanics`
  UNION ALL SELECT 'families', family_id, name FROM `bgg-data-warehouse.core.families`
  UNION ALL SELECT 'designers', designer_id, name FROM `bgg-data-warehouse.core.designers`
  UNION ALL SELECT 'artists', artist_id, name FROM `bgg-data-warehouse.core.artists`
  UNION ALL SELECT 'publishers', publisher_id, name FROM `bgg-data-warehouse.core.publishers`
)
SELECT
  COALESCE(c.tbl, p.tbl) AS tbl,
  COUNTIF(p.entity_id IS NULL) AS id_only_in_core,
  COUNTIF(c.entity_id IS NULL) AS id_only_in_parsed,
  COUNTIF(c.entity_id IS NOT NULL AND p.entity_id IS NOT NULL AND c.name != p.first_name) AS name_differs,
  ARRAY_AGG(IF(c.name != p.first_name, STRUCT(c.entity_id, c.name, p.first_name), NULL) IGNORE NULLS LIMIT 20) AS sample_name_diffs
FROM core_rows c
FULL JOIN parsed p ON c.tbl = p.tbl AND c.entity_id = p.entity_id
GROUP BY 1
ORDER BY 1
