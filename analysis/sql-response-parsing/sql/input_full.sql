-- sql/input_full.sql
CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS
SELECT * FROM `bgg-data-warehouse.scratch_parsing.responses_json`
