-- Edge-tag counts across every response (not just core_inputs).
SELECT tag, COUNT(*) AS responses
FROM `bgg-data-warehouse.scratch_parsing.edge_tags`, UNNEST(tags) tag
GROUP BY tag ORDER BY tag
