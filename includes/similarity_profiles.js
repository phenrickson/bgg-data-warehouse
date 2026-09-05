// similarity_profiles.js — generated from the bgg-viewer similarity tuning bench, 2026-09-04
// Consumed by bgg-data-warehouse definitions/game_neighbors.sqlx via includes/.
// Review the fields marked "deploy-only" before deploying.

module.exports = {
  profiles: [
    {
      name: "recommender",
      weight: 0.6,
      complexity_band: 0.75,
      max_per_family: 1,
      min_similarity: 0.5,
      min_rating_pct: 0.8,
      max_rating_pct: 1,
      min_avg_rating: 0,
      max_avg_rating: 10,
      min_users_rated: 100,
      top_k: 10,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    },
    {
      name: "sicko",
      weight: 0.7,
      complexity_band: null,
      max_per_family: 0,
      min_similarity: 0.5,
      min_rating_pct: 0,
      max_rating_pct: 0.8,
      min_avg_rating: 0,
      max_avg_rating: 10,
      min_users_rated: 30,
      top_k: 10,
      // deploy-only — review before shipping. source_min_users_rated is hand-set (the bench
      // doesn't tune it): sicko is band-less, so computing it for all ~128k sources blows
      // BigQuery's on-demand CPU ceiling — it only runs for games with >= 30 ratings, a newer
      // game falls back to `similar`.
      source_min_users_rated: 30,
      dims: 64,
      distance: "COSINE"
    },
    {
      name: "similar",
      weight: 1,
      complexity_band: 0.75,
      max_per_family: 1,
      min_similarity: 0.5,
      min_rating_pct: 0,
      max_rating_pct: 1,
      min_avg_rating: 0,
      max_avg_rating: 10,
      min_users_rated: 100,
      top_k: 10,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    }
  ]
};
