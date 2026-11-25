from checkouts import *
from page import HERE

z = cd(18, 0)
m = lambda t: round((t - z).total_seconds() / 60, 2)


def all_out(g):
    """Intervals when every copy was out (minutes since Wed 00:00)."""
    L, cur, start = [], 0, None
    for t, x in sorted([(k["o"], 1) for k in g["ks"]] + [(k["i"] or END, -1) for k in g["ks"]]):
        cur += x
        if cur >= g["k"] and start is None:
            start = t
        elif cur < g["k"] and start is not None:
            L.append([m(start), m(t)])
            start = None
    return L


games = [g for g in sorted(GAMES.values(), key=lambda g: -g["c"]) if g["k"]]
data = dict(
    W=[[m(a), m(b)] for a, b in W],
    O=[[m(a), m(b)] for a, b in OPEN],
    G=[
        dict(b=g["b"], n=g["n"], y=g["y"], k=g["k"], c=g["c"], p=round(g["p"], 1),
             iv=[[m(k["o"]), m(k["i"] or END)] for k in g["ks"]],
             ao=all_out(g))
        for g in games
    ],
)
s = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
html = open(HERE / "heatmap.template.html", encoding="utf-8").read().replace("/*DATA*/", s)
open(DATA / "heatmap.html", "w", encoding="utf-8").write(html)
print(len(games), "games; omitted (no copy count)", len(GAMES) - len(games))
