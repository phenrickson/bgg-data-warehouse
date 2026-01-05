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
