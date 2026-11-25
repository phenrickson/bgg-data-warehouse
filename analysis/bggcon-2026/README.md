# BGG.CON 2026 library checkouts

Analysis of the checkouts recorded by `src/bggcon` at BGG.CON 2026 (Nov 18–22). Everything is built from local flat files into one standalone HTML report, `report.qmd`. Titles, labels and bracketed text are placeholders.

## Report

From this folder, with Quarto installed and `QUARTO_PYTHON` pointing at the repo's `.venv` (`uv sync --group analysis`):

    python -X utf8 build_playback.py   # once, for the map
    quarto render report.qmd

`stats_*.py` hold the statistics, `availability.py` and `report_data.py` the descriptive numbers, `report_charts.py`, `report.js`, `report_map.*` and `report.css` the drawing.

`build_trends.py` writes the separate trends page. `days.py`, `daytypes.py`, `once.py` and `heat.py` print numbers and tests.

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
