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
  "game_families"
];

coreTables.forEach(table => {
  declare({
    schema: "core",
    name: table
  });
});

// Cross-project source: ML predictions from bgg-predictive-models
declare({
  database: "bgg-predictive-models",
  schema: "raw",
  name: "ml_predictions_landing"
});
