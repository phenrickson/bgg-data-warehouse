-- Duplicate rows in core.language_dependence for the sample games.
SELECT game_id, level, description, votes, COUNT(*) AS copies
FROM `bgg-data-warehouse.core.language_dependence`
WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)
GROUP BY 1, 2, 3, 4
HAVING copies > 1
ORDER BY game_id, level
