"""Exhibits for report.qmd. Drawing only: each takes finished numbers and returns HTML to place in the text."""
import itertools
import json

from IPython.display import HTML

from availability import GAP_SPANS, OBSERVED_SPANS, OPEN_SPANS
from page import DATA, HERE

_ids = itertools.count(1)
_read = lambda f: (HERE / f).read_text(encoding="utf-8")
_json = lambda d: json.dumps(d, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def setup(games):
    """Shared chart code, the tooltip, and every game's checkouts; call once, at the top of the report."""
    game_map.loaded = False
    con = dict(O=OPEN_SPANS, U=GAP_SPANS, W=OBSERVED_SPANS)
    return HTML(f'<div id="rtip"></div><script>\n{_read("report.js")}\nR.data({_json(con)},{_json(games)});</script>')


def exhibit(kind, data, title, desc="", note="", wide=False):
    """One chart: title, description, chart, note. kind is a function in report.js."""
    i = f"x{next(_ids)}"
    src = f'<div class="src">{note}</div>' if note else ""
    return HTML(f'<figure{" class=wide" if wide else ""}><figcaption><b>{title}</b><span>{desc}</span></figcaption><div class="plot" id="{i}"></div>{src}</figure>'
                f'<script>R.{kind}(document.getElementById("{i}"),{_json(data)});</script>')


def game_map(title, desc="", note="", still=False, color="weight", top=None):
    """The map: with playback, or still=True for the whole con at once, colored by weight, category or unavailable (in demand).
    top is the share of the con, with every copy out, that gets the hottest in-demand color (default: about half).

    Needs build_playback.py to have run (it writes playback.json).
    """
    i = f"x{next(_ids)}"
    opts = _json(dict(still=still, color=color) | (dict(top=top) if top else {}))
    src = f'<div class="src">{note}</div>' if note else ""
    data = (DATA / "playback.json").read_text(encoding="utf-8")
    load = "" if getattr(game_map, "loaded", False) else "const MAPDATA=" + data + ";" + _read("report_map.js")
    game_map.loaded = True  # the data and code go in the page once
    return HTML(f'<figure class="wide" id="{i}"><figcaption><b>{title}</b><span>{desc}</span></figcaption>{_read("report_map.html")}{src}</figure>'
                f'<script>{load}R.map(document.querySelector("#{i} .cmap"),MAPDATA,{opts});</script>')


def numbers(items):
    """Headline numbers: [[number, label], ...]"""
    return HTML('<div class="kpis">' + "".join(f"<div><b>{n}</b><span>{t}</span></div>" for n, t in items) + "</div>")
