"""Shared loader: flat files -> cleaned checkouts (K) and games (GAMES). No outputs written."""
import json,gzip,re,collections
from datetime import datetime as D,timedelta as T
import pyarrow as pa
from page import DATA
J=lambda f:json.load(open(DATA/f,encoding='utf-8'))
p=lambda s:D.fromisoformat(s.replace(' UTC',''))
cd=lambda d,h,m=0:D(2026,11,d)+T(hours=h+6,minutes=m)   # Chicago -> UTC
ct=lambda t:(t-T(hours=6))
OPEN=[(cd(18,9),cd(19,0)),(cd(19,8),cd(20,0)),(cd(20,8),cd(21,0)),(cd(21,8),cd(22,0)),(cd(22,8),cd(22,13))]
UNK=[(cd(18,14,52),cd(18,15,4)),(cd(18,15,19),cd(18,15,26)),(cd(19,13,12),cd(19,13,38)),(cd(19,14,5),cd(19,21)),(cd(21,13,2),cd(21,13,38))]
W=[]
for o,c in OPEN:
    cur=o
    for a,b in UNK:
        if o<=a<c:W.append((cur,a));cur=b
    W.append((cur,c))
mins=lambda a,b:sum(max(0,(min(b,y)-max(a,x)).total_seconds()) for x,y in W)/60
OPEN_MIN=mins(W[0][0],W[-1][1])
HOT=set(re.findall(r'/boardgame/(\d+)/',open(DATA/'hot-games-geeklist.html',encoding='utf-8').read()))
runs=[p(r['observed_at']) for r in J('runs.json') if r['ok'] in(True,'true') and r['note']!='test run']
BASE,END=min(runs),max(runs)
LIB={str(g['bgg_id']):g for g in J('library.json')['games']}
raw=open(DATA/'catalog.arrow.gz','rb').read();raw=gzip.decompress(raw) if raw[:2]==b'\x1f\x8b' else raw
try:_t=pa.ipc.open_file(raw).read_all()
except Exception:_t=pa.ipc.open_stream(raw).read_all()
CAT={str(r['game_id']):r for r in _t.select(['game_id','name','year_published','average_weight','predicted_complexity','users_rated','min_playtime','max_playtime']).to_pylist()}
X={e['game_id']:e for e in J('extra_games.json')}
def meta(b):
    if b in CAT:c=CAT[b];return c['name'],c['year_published'] or None
    if b in X:e=X[b];return e['primary_name'],int(float(e['year_published'] or 0)) or None
    return None,None
seq=collections.defaultdict(list)
for e in J('events.json'):e['t']=p(e['observed_at']);seq[(e['copy_id'],e['lastcheckout_id'])].append(e)
K=[]
for (cp,_),v in seq.items():
    v.sort(key=lambda e:(e['t'],e['event']=='out'));o=next(e for e in v if e['event']=='out');d=p(o['tte_checkout_date'])
    if not o['bgg_id'] or o['t']<=BASE or d<cd(17,0):continue   # no id; baseline; pre-con records
    K.append(dict(b=o['bgg_id'],copy=cp,name=o['name'],o=o['t'],d=d,i=v[-1]['t'] if v[-1]['event']=='in' else None))
G=collections.defaultdict(list)
for k in K:G[k['b']].append(k)
GAMES={}
for b,ks in G.items():
    n=LIB.get(b,{}).get('copies');nm,y=meta(b);al=0;cur=0;s=None
    for t,x in sorted([(k['o'],1) for k in ks]+[(k['i'] or END,-1) for k in ks]):
        cur+=x
        if n and cur>=n and s is None:s=t
        elif n and cur<n and s is not None:al+=mins(s,t);s=None
    GAMES[b]=dict(b=b,n=nm or ks[0]['name'],y=y,c=len(ks),k=n,p=100*al/OPEN_MIN if n else None,hot=b in HOT,ks=ks)
