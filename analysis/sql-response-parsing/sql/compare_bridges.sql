WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
parsed AS (
  SELECT DISTINCT
    CASE p.link_type
      WHEN 'boardgamecategory' THEN 'game_categories'
      WHEN 'boardgamemechanic' THEN 'game_mechanics'
      WHEN 'boardgamefamily' THEN 'game_families'
      WHEN 'boardgameexpansion' THEN 'game_expansions'
      WHEN 'boardgameimplementation' THEN 'game_implementations'
      WHEN 'boardgamedesigner' THEN 'game_designers'
      WHEN 'boardgameartist' THEN 'game_artists'
      WHEN 'boardgamepublisher' THEN 'game_publishers'
    END AS tbl, p.game_id, p.entity_id
  FROM `bgg-data-warehouse.scratch_parsing.parsed_links` p
  JOIN games g USING (record_id)
  WHERE NOT (p.link_type = 'boardgameimplementation' AND p.inbound)
),
core_rows AS (
  SELECT 'game_categories' tbl, game_id, category_id entity_id FROM `bgg-data-warehouse.core.game_categories`
  UNION ALL SELECT 'game_mechanics', game_id, mechanic_id FROM `bgg-data-warehouse.core.game_mechanics`
  UNION ALL SELECT 'game_families', game_id, family_id FROM `bgg-data-warehouse.core.game_families`
  UNION ALL SELECT 'game_expansions', game_id, expansion_id FROM `bgg-data-warehouse.core.game_expansions`
  UNION ALL SELECT 'game_implementations', game_id, implementation_id FROM `bgg-data-warehouse.core.game_implementations`
  UNION ALL SELECT 'game_designers', game_id, designer_id FROM `bgg-data-warehouse.core.game_designers`
  UNION ALL SELECT 'game_artists', game_id, artist_id FROM `bgg-data-warehouse.core.game_artists`
  UNION ALL SELECT 'game_publishers', game_id, publisher_id FROM `bgg-data-warehouse.core.game_publishers`
),
core_scoped AS (SELECT DISTINCT * FROM core_rows WHERE game_id IN (SELECT game_id FROM games)),
only_core AS (SELECT * FROM core_scoped EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_scoped)
SELECT tbl,
  COUNTIF(side = 'core') AS only_in_core, COUNTIF(side = 'parsed') AS only_in_parsed,
  ARRAY_AGG(STRUCT(side, game_id, entity_id) ORDER BY game_id LIMIT 20) AS sample
FROM (SELECT 'core' side, * FROM only_core UNION ALL SELECT 'parsed', * FROM only_parsed)
GROUP BY tbl
ORDER BY tbl
