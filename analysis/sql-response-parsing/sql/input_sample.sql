-- sql/input_sample.sql  (points the pipeline at the materialised sample)
CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS
SELECT * FROM `bgg-data-warehouse.scratch_parsing.responses_sample`
