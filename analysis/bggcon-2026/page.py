"""Where the flat files live, and filling <name>.template.html with the shared styles and charts plus its data -> <name>.html."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE.parents[1] / "data" / "bggcon-2026"  # gitignored: source files in, built pages out


def page(name, data):
    rd = lambda f: (HERE / f).read_text(encoding="utf-8")
    t = rd(f"{name}.template.html").replace("/*CSS*/", rd("charts.css")).replace("/*CHARTS*/", rd("charts.js")).replace("/*DATA*/", json.dumps(data))
    (DATA / f"{name}.html").write_text(t, encoding="utf-8")
