"""Draft map regions: cluster library games by map position, name each by its most distinctive categories/mechanics."""
import collections, numpy as np
from sklearn.cluster import KMeans

def regions(rows, con, k=11):
    P = np.array([(g["x"], g["y"]) for g in rows])
    T = {str(r["game_id"]): set(r["categories"] or []) | set(r["mechanics"] or []) for r in con._t.select(["game_id", "categories", "mechanics"]).to_pylist()}
    lab = KMeans(k, n_init=20, random_state=2026).fit_predict(P)
    tags = [T.get(g["b"], set()) for g in rows]; n = len(rows)
    co = np.array([len(g["iv"]) for g in rows]); allc = collections.Counter(t for s in tags for t in s)
    out = []
    for c in range(k):
        m = np.where(lab == c)[0]; cnt = collections.Counter(t for i in m for t in tags[i]); nm = len(m)
        sc = []
        for t, v in cnt.items():
            p, q = v / nm, (allc[t] - v) / (n - nm)
            if p >= .15 and p > q: sc.append((p * np.log((p + .01) / (q + .01)), t, p, q))
        sc.sort(reverse=True)
        d = ((P[m][:, None] - P[m][None]) ** 2).sum(2); i = m[np.exp(-d / (2 * .35 ** 2)).sum(1).argmax()]
        out.append(dict(x=float(P[i][0]), y=float(P[i][1]), n=nm, co=int(co[m].sum()), tags=[(t, round(p, 2), round(q, 2)) for _, t, p, q in sc[:6]],
                        top=[rows[j]["n"] for j in sorted(m, key=lambda j: -co[j])[:4]], w=float(np.median([rows[j]["w"] for j in m if rows[j]["w"]]))))
    return out, lab
