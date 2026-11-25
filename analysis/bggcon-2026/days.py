"""Day-to-day similarity of checkouts, against shuffled day labels. Prints only."""
from checkouts import *
import numpy as np
from itertools import combinations

rng = np.random.default_rng(2026)
R = 2000
DN = {18: "Wed", 19: "Thu", 20: "Fri", 21: "Sat"}
CATS = {str(r["game_id"]): set(r["categories"] or []) for r in _t.select(["game_id", "categories"]).to_pylist()} if "_t" in globals() else {}
import checkouts as con
CATS = {str(r["game_id"]): set(r["categories"] or []) for r in con._t.select(["game_id", "categories"]).to_pylist()}


def weight(b):
    if b in CAT:
        return CAT[b]["average_weight"] or None
    if b in X:
        return float(X[b]["average_weight"] or 0) or None


rows = []
for k in K:
    t = ct(k["d"])
    if t.day in DN and any(o <= k["d"] < c for o, c in OPEN):
        rows.append((k["b"], t.day, t.hour + t.minute / 60))
gid = {b: i for i, b in enumerate(sorted({r[0] for r in rows}))}
G_ = np.array([gid[r[0]] for r in rows]); Dd = np.array([r[1] for r in rows]); H = np.array([r[2] for r in rows])
ids = sorted(gid, key=gid.get); nG = len(ids)


def counts(g, d):
    return {x: np.bincount(g[d == x], minlength=nG) for x in DN}


def morisita(a, b):  # corrects for sampling: ~1 when both days draw from the same demand
    na, nb = a.sum(), b.sum()
    da = (a * (a - 1)).sum() / (na * (na - 1)); db = (b * (b - 1)).sum() / (nb * (nb - 1))
    return 2 * (a * b).sum() / ((da + db) * na * nb)


def jac(a, b):
    return ((a > 0) & (b > 0)).sum() / ((a > 0) | (b > 0)).sum()


def shuffle(d, h, strat):
    out = d.copy()
    if not strat:
        return rng.permutation(d)
    for s in np.unique(h.astype(int)):  # keep each hour's number of checkouts per day
        i = np.where(h.astype(int) == s)[0]
        out[i] = d[rng.permutation(i)]
    return out


def run(label, mask):
    g, d, h = G_[mask], Dd[mask], H[mask]
    C = counts(g, d)
    print(f"\n## {label}: checkouts", {DN[x]: int(C[x].sum()) for x in DN}, "games", {DN[x]: int((C[x] > 0).sum()) for x in DN})
    P = list(combinations(DN, 2))
    nullJ = {p: [] for p in P}; nullM = {p: [] for p in P}
    for _ in range(R):
        N = counts(g, shuffle(d, h, True))
        for p in P:
            nullJ[p].append(jac(N[p[0]], N[p[1]])); nullM[p].append(morisita(N[p[0]], N[p[1]]))
    for p in P:
        a, b = C[p[0]], C[p[1]]
        j, m = jac(a, b), morisita(a, b)
        nj, nm = np.array(nullJ[p]), np.array(nullM[p])
        two = lambda o, n: min(1, 2 * min((n <= o).sum() + 1, (n >= o).sum() + 1) / (R + 1))
        print(f"  {DN[p[0]]}-{DN[p[1]]}: shared {int(((a>0)&(b>0)).sum())} jaccard {j:.3f} (chance {nj.mean():.3f}, {np.percentile(nj,2.5):.3f}-{np.percentile(nj,97.5):.3f}, p={two(j,nj):.4f}) | morisita {m:.3f} (chance {nm.mean():.3f}, {np.percentile(nm,2.5):.3f}-{np.percentile(nm,97.5):.3f}, p={two(m,nm):.4f})")
    return g, d, h


allm = np.ones(len(G_), bool)
run("all open hours", allm)
# hours every day has: 9 AM-1 PM and 10 PM-midnight (skips 9-10 PM catch-up)
mm = ((H >= 9) & (H < 13)) | (H >= 22)
g, d, h = run("matched hours", mm)

C = counts(G_, Dd)
seen = sum((C[x] > 0).astype(int) for x in DN)
print("\ngames by number of days (Wed-Sat):", {int(n): int((seen == n).sum()) for n in range(1, 5)}, "checkout share:", {int(n): round(100 * sum(C[x][seen == n].sum() for x in DN) / len(G_), 1) for n in range(1, 5)})
for x in DN:
    print(" ", DN[x], "games new that day", int(((C[x] > 0) & (sum((C[y] for y in DN if y < x), np.zeros(nG, int)) == 0)).sum()), "of", int((C[x] > 0).sum()))
tops = {x: [i for i in np.argsort(-C[x], kind="stable")[:25]] for x in DN}
print("top-25 overlap:", {f"{DN[a]}-{DN[b]}": len(set(tops[a]) & set(tops[b])) for a, b in combinations(DN, 2)})

print("\n## type mix by day, matched hours, p from 2000 day-label shuffles within hour")
yr = np.array([meta(b)[1] or np.nan for b in ids], float); wt = np.array([weight(b) or np.nan for b in ids], float)
hot = np.array([b in HOT for b in ids], float)
feats = {"2026 share": np.where(np.isnan(yr), np.nan, (yr == 2026)), "2025 share": np.where(np.isnan(yr), np.nan, yr == 2025), "pre-2020 share": np.where(np.isnan(yr), np.nan, yr < 2020), "hot share": hot, "mean weight": wt}
cc = collections.Counter(c for b in set(ids) for c in CATS.get(b, ()))
w_ = collections.Counter()
for i in g:
    w_.update(CATS.get(ids[i], ()))
for c, _ in w_.most_common(12):
    feats["cat: " + c] = np.array([c in CATS[b] if b in CATS else np.nan for b in ids], float)


def daymeans(v, d):
    return np.array([np.nanmean(v[d == x]) for x in DN])


sh = [shuffle(d, h, True) for _ in range(R)]
for n, f in feats.items():
    v = f[g].astype(float)
    o = daymeans(v, d); stat = np.nanvar(o); null = np.array([np.var(daymeans(v, s)) for s in sh])
    k_ = 1 if n == "mean weight" else 100
    print(f"  {n:28s}", " ".join(f"{DN[x]} {k_*m:.2f}" for x, m in zip(DN, o)), f"p={(1+(null>=stat).sum())/(R+1):.4f}", f"n={int((~np.isnan(v)).sum())}")
