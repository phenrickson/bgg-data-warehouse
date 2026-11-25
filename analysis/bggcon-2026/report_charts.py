"""Charts for report.qmd. Drawing only: each takes finished numbers and returns HTML to place in the text."""
import html
import itertools
import json

from IPython.display import HTML

from page import DATA, HERE

_ids = itertools.count(1)


def setup():
    """Shared chart code and tooltip; call once, at the top of the report."""
    return HTML('<div id="tip"></div><script>\n' + (HERE / "charts.js").read_text(encoding="utf-8") + "\n</script>")


def chart(call, data):
    """A charts.js call, drawn where the chunk is. D is the data."""
    i = f"chart-{next(_ids)}"
    d = json.dumps(data).replace("</", "<\/")
    return HTML(f'<div class="chart" id="{i}"></div><script>(D => {{ M = document.getElementById("{i}"); {call} }})({d});</script>')


def hour_chart(title, series, ticks, percent=False):
    return chart(f"hourChart({json.dumps(title)}, D, {json.dumps(ticks)}, {'pc' if percent else 'two'});", series)


def page_frame(name):
    """A built page (the map) in its own frame, so its styles and ours can't clash. Sizes itself to fit."""
    src = html.escape((DATA / f"{name}.html").read_text(encoding="utf-8"), quote=True)
    fit = ("const f=this,d=f.contentDocument,fit=()=>{f.style.height=d.documentElement.offsetHeight+'px'};"
           "fit();new f.contentWindow.ResizeObserver(fit).observe(d.body)")
    return HTML(f'<iframe class="frame" title="{name}" srcdoc="{src}" onload="{fit}"></iframe>')
