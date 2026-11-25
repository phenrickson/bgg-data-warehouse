# BGG.CON 2026 library checkouts

Analysis of the checkouts recorded by `src/bggcon` at BGG.CON 2026 (Nov 18–22). Each page is a standalone HTML file, built from local flat files; the pages feed one report, which isn't assembled yet. Titles and labels in the charts are placeholders.

## Run

From the repo root (`-X utf8` is needed on Windows):

    uv run --group analysis python -X utf8 analysis/bggcon-2026/build_patterns.py

| Script | Page | Shows |
|---|---|---|
| `build_playback.py` | `playback.html` | Map of checkouts over time |
| `build_heatmap.py` | `heatmap.html` | When one game's copies were out (`#<bgg_id>` picks the game) |
| `build_complexity.py` | `complexity.html` | Checkouts by complexity, and complexity and party games by hour |
| `build_years.py` | `years.html` | Checkouts by year published |
| `build_patterns.py` | `patterns.html` | Availability of the most in-demand games, and games that gained steam, cooled off, or went out mostly at night or by day |

Templates sit next to the scripts. `page.py`, `charts.css` and `charts.js` are shared by the last three; the map and heatmap still carry their own styles.

`checkouts.py` loads and cleans everything else uses. `complexity.py`, `steam.py`, `patterns.py`, `days.py`, `daytypes.py`, `once.py` and `heat.py` print numbers and tests.

## Data

Inputs and built pages live in `data/bggcon-2026/`, which is gitignored, so a fresh clone can't rebuild the pages without those files:

- `events.json`, `runs.json`: recorder events and poll runs
- `library.json`: the Nov 19 library list (same as `library.2026-11-19.json`); the Nov 17 and 18 pulls are kept but not used yet
- `catalog.arrow.gz`, `coordinates.arrow.gz`: game catalog and map positions
- `extra_games.json`, `extra_coordinates.json`: games missing from those two
- `hot-games-geeklist.html`: the hot games list

## Caveats

- The recorder missed Thursday about 2–9 PM, plus a few short gaps (`UNK` in `checkouts.py`). Availability counts only the 59.7 observed open hours.
- "In demand" / "every copy out" uses copy counts from the Nov 19 list. The list changed during the con.
- 47 checked-out games aren't in that list, so they have no copy count. 48 (105 checkouts) aren't on the map.
- A checkout's return is its last `in` event per copy and checkout id.
- Gained steam / cooled off are picked by rule, not by test: 9 of 357 games pass p < .05 for gaining, where luck gives about 18. The gained rule is loose (127 games).
