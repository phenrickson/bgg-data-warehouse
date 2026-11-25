# BGG.CON 2026 library checkouts

Analysis of the checkouts recorded by `src/bggcon` at BGG.CON 2026 (Nov 18–22). Everything is built from local flat files into one standalone HTML report, `report.qmd`. Titles, labels and bracketed text are placeholders.

## Set up on a new machine

Needs git, [uv](https://docs.astral.sh/uv/), [Quarto](https://quarto.org/docs/get-started/) (1.9 was used) and the Google Cloud CLI. Commands are for Bash (Git Bash on Windows), from the repo root.

1. Get the branch and the Python packages:

       git clone https://github.com/phenrickson/bgg-data-warehouse.git
       cd bgg-data-warehouse
       git switch feat/bggcon-analysis   # skip once merged
       uv sync --group analysis

2. Get the data, about 27 MB. It's a frozen copy in a private bucket, so log in with an account that can read the project:

       gcloud auth login
       mkdir -p data/bggcon-2026
       gcloud storage cp "gs://bgg-data-warehouse-bgg-data/bggcon-2026/*" data/bggcon-2026/

   Use the copy rather than fetching again: the catalog and coordinates change over time, and the viewer bucket deletes the library list after 30 days. The folder is gitignored; keep it out of git, since the repo is public.

## Report

From this folder:

    export QUARTO_PYTHON="$(cd ../.. && uv run --group analysis python -c 'import sys; print(sys.executable)')"
    unset VIRTUAL_ENV
    PYTHONUTF8=1 quarto render report.qmd
    mv report.html ../../data/bggcon-2026/

Then open `data/bggcon-2026/report.html`. `playback.json` (the map) comes with the data. Rebuild it, before rendering, only if `build_playback.py` changes; the trends page is separate:

    uv run --group analysis python -X utf8 build_playback.py
    uv run --group analysis python -X utf8 build_trends.py

`stats_*.py` hold the statistics, `availability.py` and `report_data.py` the descriptive numbers, `report_charts.py`, `js/`, `report_map.html` and `report.css` the drawing. The ordinary charts are in `js/plots.js`, drawn with Observable Plot (`vendor/`, with D3; both ISC license). Format `js/` with `npx prettier@3.6.2 --print-width 120 --single-quote --write js/*.js`.

`build_trends.py` writes the separate trends page. `days.py`, `daytypes.py`, `once.py` and `heat.py` print numbers and tests.

## Data

Inputs and built pages live in `data/bggcon-2026/`, which is gitignored. The frozen copy has:

- `events.json`, `runs.json`: recorder events and poll runs, from `raw.bggcon_checkout_events` and `raw.bggcon_runs`
- `library.json`: the Nov 19 library list, from the viewer bucket
- `catalog.arrow.gz`, `coordinates.arrow.gz`: bgg-viewer's game catalog and map positions, as of Nov 24
- `extra_games.json`, `extra_coordinates.json`: games missing from those two
- `hot-games-geeklist.html`: the Hot Games list, a page saved by hand from BGG
- `playback.json`: the map, built by `build_playback.py`

## Caveats

- The recorder missed Thursday about 2–9 PM, plus a few short gaps (`UNK` in `checkouts.py`). Availability counts only the 59.7 observed open hours.
- "In demand" / "every copy out" uses copy counts from the Nov 19 list. The list changed during the con.
- 47 checked-out games aren't in that list, so they have no copy count. 48 (105 checkouts) aren't on the map.
- A checkout's return is its last `in` event per copy and checkout id.
