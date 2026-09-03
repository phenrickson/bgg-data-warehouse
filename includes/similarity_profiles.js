// similarity_profiles.js — seeded from the bgg-viewer similarity tuning bench, 2026-09-03.
// Consumed by bgg-data-warehouse definitions/game_neighbors.sqlx via includes/.
//
// `source_min_users_rated` is hand-set here (the bench doesn't tune it). `similar` and
// `recommender` cover every game (both banded, so their cross join is affordable at
// source 0). `sicko` is band-less — computing it for all 128k sources blows BigQuery's
// on-demand CPU ceiling even in its own job — so it only runs for games with >= 30
// ratings; a newer game falls back to `similar`.
//
// NOTE: "exclude shared title words" was on for an experiment; it has no SQL equivalent
//       and was dropped.

module.exports = {
  profiles: [
    {
      name: "similar",
      weight: 1,
      complexity_band: 0.75,
      max_per_family: 1,
      min_similarity: 0.5,
      min_rating_pct: 0,
      min_users_rated: 100,
      source_min_users_rated: 0,
      top_k: 10,
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
      source_min_users_rated: 30,
      top_k: 10,
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
      source_min_users_rated: 0,
      top_k: 10,
      dims: 64,
      distance: "COSINE"
    }
  ]
};
