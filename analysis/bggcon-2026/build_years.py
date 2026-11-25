"""Data for years.html: every library game, and the share checked out, by year published."""
from checkouts import *
from page import page

# Before 2015 single years have too few library games, so they're grouped; from 2015 each year has 200+.
BUCKETS = [(-9999, 1989, "<1990"), (1990, 1999, "1990s"), (2000, 2004, "2000–04"), (2005, 2009, "2005–09"), (2010, 2014, "2010–14")]
BUCKETS += [(y, y, str(y)) for y in range(2015, 2026)] + [(2026, 9999, "2026+")]
col = lambda y: next(i for i, (a, b, _) in enumerate(BUCKETS) if a <= y <= b)
dots = []
for b in LIB:
    n, y = meta(b)
    if y:
        dots.append([n or LIB[b].get("name"), str(y), GAMES[b]["c"] if b in GAMES else 0, col(y)])
rate = [[sum(g[3] == i for g in dots), sum(g[3] == i and g[2] > 0 for g in dots)] for i in range(len(BUCKETS))]
for (_, _, n), (c, k) in zip(BUCKETS, rate):
    print(f"{n:9s} {k:4d} of {c:4d} {k / c:.0%}")
print(len(dots), "library games with a year of", len(LIB))
page("years", dict(dots=dots, rate=rate, names=[n for _, _, n in BUCKETS]))
