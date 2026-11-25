from checkouts import *
from page import HERE

z = cd(18, 0)
m = lambda t: round((t - z).total_seconds() / 60, 1)
r = open(DATA / "coordinates.arrow.gz", "rb").read()
r = gzip.decompress(r) if r[:2] == b"\x1f\x8b" else r
try:
    t = pa.ipc.open_file(r).read_all()
except Exception:
    t = pa.ipc.open_stream(r).read_all()
XY = {str(a): (b, c) for a, b, c in zip(*(t.column(x).to_pylist() for x in ("game_id", "umap_1", "umap_2")))}
PC = {str(a): (b, c) for a, b, c in zip(*(t.column(x).to_pylist() for x in ("game_id", "pc_1", "pc_2")))}
# Checked-out games the catalog leaves out: same model and version, from predictions.bgg_game_coordinates
EXTRA = {r_["game_id"]: (float(r_["umap_1"]), float(r_["umap_2"])) for r_ in json.load(open(DATA / "extra_coordinates.json"))}
PC |= {r_["game_id"]: (float(r_["pca_1"]), float(r_["pca_2"])) for r_ in json.load(open(DATA / "extra_coordinates.json")) if r_["game_id"] not in PC}
# Primary category: same list, slot order and priority as bgg-viewer src/lib/catalog/primary-category.ts
SLOTS = ["Trains", "Economic", "Wargame", "Party Game", "Children's Game", "Abstract Strategy", "Card Game"]
PRIORITY = ["Trains", "Wargame", "Economic", "Party Game", "Children's Game", "Abstract Strategy", "Card Game"]
import checkouts as con
CATS = {str(r["game_id"]): set(r["categories"] or []) for r in con._t.select(["game_id", "categories"]).to_pylist()}


def cat_code(b):
    return next((SLOTS.index(c) + 1 for c in PRIORITY if c in CATS.get(b, ())), 0)


def weight(b):
    w = CAT[b]["average_weight"] if b in CAT else float(X[b]["average_weight"] or 0) if b in X else 0
    return round(w, 2) if w else 0


def all_out(b):
    """Intervals when every copy was out (minutes since Wed 00:00); empty without a copy count."""
    g, L, cur, start = GAMES.get(b), [], 0, None
    if not g or not g["k"]:
        return L
    for t_, x in sorted([(k["o"], 1) for k in g["ks"]] + [(k["i"] or END, -1) for k in g["ks"]]):
        cur += x
        if cur >= g["k"] and start is None:
            start = t_
        elif cur < g["k"] and start is not None:
            L.append([m(start), m(t_)])
            start = None
    return L


def row(b, xy):
    n, y = meta(b)
    return dict(b=b, n=n or LIB.get(b, {}).get("name"), y4=y, k=LIB.get(b, {}).get("copies"),
                ao=all_out(b), w=weight(b), c=cat_code(b), x=round(xy[0], 3), y=round(xy[1], 3), a=round(PC[b][0], 3), b2=round(PC[b][1], 3),
                iv=[[m(k["o"]), m(k["i"] or END)] for k in GAMES[b]["ks"]] if b in GAMES else [])


rows = [row(b, XY[b]) for b in sorted(set(LIB) | set(GAMES), key=int) if b in XY and XY[b][0] is not None]
# Region labels: where each category is densest AND makes up most of its neighborhood, over all library games.
import numpy as np
P = np.array([(g["x"], g["y"]) for g in rows]); C = np.array([g["c"] for g in rows])
BW = 0.35
ext = (P.max(0) - P.min(0)).max()


def dens(q, pts):
    return np.exp(-((q[:, None, :] - pts[None, :, :]) ** 2).sum(2) / (2 * BW**2)).sum(1)


Q = np.array([(g["a"], g["b2"]) for g in rows])


def pca_spot(members):
    """Where these games are densest on the PCA map."""
    q = Q[members]
    bw = BW * (Q.max(0) - Q.min(0)).max() / ext
    best = q[np.exp(-((q[:, None] - q[None]) ** 2).sum(2) / (2 * bw**2)).sum(1).argmax()]
    return dict(a=round(float(best[0]), 3), b2=round(float(best[1]), 3))


LABELS = []
for c, name in enumerate(SLOTS, 1):
    pts = P[C == c]
    dc, da = dens(pts, pts), dens(pts, P)
    score, purity = dc * dc / da, dc / da
    order = np.argsort(-score)
    peaks = []
    for i in order:
        if len(peaks) == 2 or score[i] < 0.45 * score[order[0]]:
            break
        if all(np.hypot(*(pts[i] - pts[j])) > 0.2 * ext for j in peaks):
            peaks.append(i)
    i = peaks[0]  # one label per category, at its strongest spot
    LABELS.append(dict(t=name, x=round(float(pts[i][0]), 3), y=round(float(pts[i][1]), 3), **pca_spot(np.where(C == c)[0])))
    print(f"label {name:18s} local share {purity[i]:.0%}")
from regions import regions
# The busiest region splits cleanly in two (co-op adventure games vs heavy Euros).
R, lab = regions(rows, con)
from sklearn.cluster import KMeans
big = int(np.argmax([r["co"] for r in R])); bm = np.where(lab == big)[0]
lab[bm] = np.where(KMeans(2, n_init=20, random_state=2026).fit_predict(P[bm]) == 0, big, 100)
# PLACEHOLDER names (Phil's to write). Each names the region holding its first example; regions not listed get no label.
NAMES = [("Heavy Euros", ["Agricola", "A Feast for Odin"]), ("Co-op adventures", ["Gloomhaven", "Arkham Horror"]), ("Puzzly tile-layers", ["Cascadia", "Calico"]),
         ("Economic games", ["Brass: Birmingham", "Power Grid"]), ("Trick-taking & card games", ["SCOUT", "The Crew: The Quest for Planet Nine"]), ("Party & social deduction", ["Codenames", "The Resistance"]),
         ("Kids & dexterity", ["Rhino Hero", "ICECOOL"]), ("Wargames", ["Twilight Struggle", "Undaunted: Normandy"])]
REG, used = [], {}
for name, ex in NAMES:
    ids = [next(i for i, g in enumerate(rows) if g["n"] == x) for x in ex]
    assert len({lab[i] for i in ids}) == 1, (name, ex, [lab[i] for i in ids])
    assert lab[ids[0]] not in used, (name, used)
    used[lab[ids[0]]] = name
    m_ = np.where(lab == lab[ids[0]])[0]
    c_ = m_[np.exp(-((P[m_][:, None] - P[m_][None]) ** 2).sum(2) / (2 * .35**2)).sum(1).argmax()]
    REG.append(dict(t=name, s=", ".join(x.split(":")[0] for x in ex), x=rows[c_]["x"], y=rows[c_]["y"], **pca_spot(m_)))
    co_ = sum(len(rows[i]["iv"]) for i in m_)
    print(f"{name:28s} games {len(m_):5d} checkouts {co_:5d} ({co_ / len(m_):.1f}/game)")
print("labeled share of checkouts", round(sum(len(rows[i]["iv"]) for i in range(len(rows)) if lab[i] in used) / sum(len(g["iv"]) for g in rows), 3))
rows += [row(b, xy) for b, xy in EXTRA.items() if b not in XY]  # after the regions, so they don't move
on_map = {g["b"] for g in rows}
OFF = [dict(iv=[[m(k["o"]), m(k["i"] or END)] for k in g["ks"]]) for b, g in GAMES.items() if b not in on_map and g["ks"]]  # counted, but nowhere to draw them
data = dict(N=len(set(LIB) | set(GAMES)), OFF=OFF, R=REG, topNA=round(float(np.percentile([g["p"] for g in GAMES.values() if g["k"]], 95)) / 100, 2), top=25, L=LABELS, W=[[m(a), m(b)] for a, b in W], O=[[m(a), m(b)] for a, b in OPEN], U=[[m(a), m(b)] for a, b in UNK], G=rows)
s = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
open(DATA / "playback.json", "w", encoding="utf-8").write(s)  # for the report
open(DATA / "playback.html", "w", encoding="utf-8").write(open(HERE / "playback.template.html", encoding="utf-8").read().replace("/*DATA*/", s))
print(len(rows), "games on the map;", sum(len(g["iv"]) for g in rows), "checkouts")
print("category counts", collections.Counter(g["c"] for g in rows), "| no weight", sum(not g["w"] for g in rows), "| no year", sum(not g["y4"] for g in rows), "| not in catalog", sum(g["b"] not in CAT for g in rows))


print(len(OFF), "checked-out games not on the map;", sum(len(g["iv"]) for g in OFF), "checkouts; library", len(LIB), "| checked out but not in library list", sum(b not in LIB for b in GAMES))
