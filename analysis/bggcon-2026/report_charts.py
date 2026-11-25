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
    con = dict(O=OPEN_SPANS, U=GAP_SPANS, W=OBSERVED_SPANS)
    return HTML(f'<div id="rtip"></div><script>\n{_read("report.js")}\nR.data({_json(con)},{_json(games)});</script>')


def exhibit(kind, data, title, desc="", note="", wide=False):
    """One chart: title, description, chart, note. kind is a function in report.js."""
    i = f"x{next(_ids)}"
    src = f'<div class="src">{note}</div>' if note else ""
    return HTML(f'<figure{" class=wide" if wide else ""}><figcaption><b>{title}</b><span>{desc}</span></figcaption><div class="plot" id="{i}"></div>{src}</figure>'
                f'<script>R.{kind}(document.getElementById("{i}"),{_json(data)});</script>')


def game_map(title, desc="", note=""):
    """The playback map. Needs build_playback.py to have run (it writes playback.json)."""
    body = _read("report_map.html")
    data = (DATA / "playback.json").read_text(encoding="utf-8")
    js = _read("report_map.js").replace("/*DATA*/", data)
    return HTML(f'<figure class="wide"><figcaption><b>{title}</b><span>{desc}</span></figcaption>{body}<div class="src">{note}</div></figure><script>{{\n{js}\n}}</script>')
