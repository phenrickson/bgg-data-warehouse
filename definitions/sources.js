const coreTables = [
  "games",
  "player_counts",
  "categories",
  "mechanics",
  "publishers",
  "designers",
  "artists",
  "families",
  "game_categories",
  "game_mechanics",
  "game_publishers",
  "game_designers",
  "game_artists",
  "game_families",
  // reimplementation / expansion edges — feed game_product_line's propagation
  "game_implementations",
  "game_expansions"
];

coreTables.forEach(table => {
  declare({
    schema: "core",
    name: table
  });
});

// Local raw source: BGG fetch metadata (used for per-game provenance in game_profile)
declare({
  schema: "raw",
  name: "fetched_responses"
});

// Cross-project source: ML predictions from bgg-predictive-models
declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "ml_predictions_landing"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "complexity_predictions"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "game_embeddings"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "description_embeddings"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "game_coordinates"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "collection_predictions_landing"
});

declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "collection_models_registry"
});

declare({
  database: "bgg-predictive-models",
  schema: "collections",
  name: "user_collections"
});
