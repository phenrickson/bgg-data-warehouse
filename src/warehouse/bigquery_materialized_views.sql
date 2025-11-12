-- BGG Data Warehouse Materialized Views
-- Best player counts materialized view for efficient filtering
CREATE OR REPLACE MATERIALIZED VIEW `${project_id}.${dataset}.best_player_counts_mv`
OPTIONS(
  enable_refresh = true,
