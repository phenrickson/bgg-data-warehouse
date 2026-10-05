-- Row counts behind compare_bridges: both sides non-empty, and duplicates in core.
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
parsed AS (
  SELECT DISTINCT p.link_type AS tbl, p.game_id, p.entity_id
  FROM `bgg-data-warehouse.scratch_parsing.parsed_links` p JOIN games g USING (record_id)
  WHERE NOT (p.link_type = 'boardgameimplementation' AND p.inbound)
),
core_rows AS (
  SELECT 'boardgamecategory' tbl, game_id, category_id entity_id FROM `bgg-data-warehouse.core.game_categories`
  UNION ALL SELECT 'boardgamemechanic', game_id, mechanic_id FROM `bgg-data-warehouse.core.game_mechanics`
  UNION ALL SELECT 'boardgamefamily', game_id, family_id FROM `bgg-data-warehouse.core.game_families`
  UNION ALL SELECT 'boardgameexpansion', game_id, expansion_id FROM `bgg-data-warehouse.core.game_expansions`
  UNION ALL SELECT 'boardgameimplementation', game_id, implementation_id FROM `bgg-data-warehouse.core.game_implementations`
  UNION ALL SELECT 'boardgamedesigner', game_id, designer_id FROM `bgg-data-warehouse.core.game_designers`
  UNION ALL SELECT 'boardgameartist', game_id, artist_id FROM `bgg-data-warehouse.core.game_artists`
  UNION ALL SELECT 'boardgamepublisher', game_id, publisher_id FROM `bgg-data-warehouse.core.game_publishers`
),
core_scoped AS (SELECT * FROM core_rows WHERE game_id IN (SELECT game_id FROM games))
SELECT c.tbl, c.core_rows, c.core_distinct, p.parsed_rows
FROM (SELECT tbl, COUNT(*) core_rows, COUNT(DISTINCT FORMAT('%d-%d', game_id, entity_id)) core_distinct FROM core_scoped GROUP BY tbl) c
JOIN (SELECT tbl, COUNT(*) parsed_rows FROM parsed GROUP BY tbl) p USING (tbl)
ORDER BY tbl
