// similarity_profiles.js — generated from the bgg-viewer similarity tuning bench, 2026-09-03
// Consumed by bgg-data-warehouse definitions/game_neighbors.sqlx via includes/.
// Review the fields marked "deploy-only" before deploying.
// NOTE: "exclude shared title words" was on for an experiment; it has no SQL
//       equivalent and was dropped.

module.exports = {
  profiles: [
    {
      name: "similar",
      weight: 1,
      complexity_band: 0.75,
      max_per_family: null,
      min_similarity: 0.5,
      min_rating_pct: 0,
      min_users_rated: 100,
      top_k: 12,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    },
    {
      name: "sicko",
      weight: 0.7,
      complexity_band: null,
      max_per_family: 1,
      min_similarity: 0,
      min_rating_pct: 0,
      min_users_rated: 30,
      top_k: 10,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    },
    {
      name: "recommender",
      weight: 0.54,
      complexity_band: 0.75,
      max_per_family: 1,
      min_similarity: 0.5,
      min_rating_pct: 0.75,
      min_users_rated: 100,
      top_k: 10,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    },
    {
      name: "default",
      weight: 0.8,
      complexity_band: 1,
      max_per_family: 1,
      min_similarity: 0.5,
      min_rating_pct: 0,
      min_users_rated: 100,
      top_k: 10,
      // deploy-only — review before shipping:
      source_min_users_rated: 0,
      dims: 64,
      distance: "COSINE"
    }
  ]
};
