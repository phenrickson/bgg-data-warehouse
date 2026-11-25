import io,contextlib
with contextlib.redirect_stdout(io.StringIO()):
    from days import *
g,d,h=G_[mm],Dd[mm],H[mm]
sh=[shuffle(d,h,True) for _ in range(2000)]
allc=collections.Counter()
for i in g:allc.update(CATS.get(ids[i],()))
cs=[c for c,n in allc.items() if n>=40]
M=np.array([[c in CATS.get(b,()) for c in cs] for b in ids],float);has=np.array([b in CATS for b in ids])
k=has[g];gm,dm=g[k],d[k]
sm=lambda dd:np.array([M[gm][dd[k]==x].mean(0) for x in DN])
o=sm(d);N=np.array([sm(s) for s in sh]);va=o.var(0);vn=N.var(1)
p=(1+(vn>=va).sum(0))/2001
z=lambda v:(v/vn.mean(0)).sum()
print('categories',len(cs),'omnibus p',(1+sum(z(v)>=z(va) for v in vn))/2001,'p<.05:',int((p<.05).sum()),'expected',round(.05*len(cs),1),'bonf',.05/len(cs))
for i in np.argsort(p)[:8]:print(' ',cs[i],np.round(100*o[:,i],1),'p',round(p[i],4))
print('all by share');[print(' ',cs[i],np.round(100*o[:,i],1)) for i in np.argsort(-o.mean(0))[:14]]
w=wt[g];B=[0,1.5,2,2.5,3,5.1]
for x in DN:
    v=w[(d==x)&~np.isnan(w)];print(DN[x],len(v),'mean',round(v.mean(),2),'q',np.round(np.percentile(v,[10,25,50,75,90]),2),'bins <1.5,1.5-2,2-2.5,2.5-3,3+',np.round(100*np.histogram(v,B)[0]/len(v),1))
med=lambda dd:np.array([np.nanmedian(w[dd==x]) for x in DN]).var()
hi=lambda dd:np.array([np.nanmean(np.where(np.isnan(w),np.nan,w>=3)[dd==x]) for x in DN]).var()
for n,f in(('median',med),('3+ share',hi)):
    a=f(d);print(n,'p',(1+sum(f(s)>=a for s in sh))/2001)
vv=[np.nanmean(w[d==x]) for x in DN];dr=[np.nanmean(w[s==18])-np.nanmean(w[s!=18]) for s in sh];a=np.nanmean(w[d==18])-np.nanmean(w[d!=18]);print('Wed vs rest mean diff',a,'p',(1+sum(abs(np.array(dr))>=abs(a)))/2001)
k2=d!=18;vr=lambda dd:np.array([np.nanmean(w[dd==x]) for x in(19,20,21)]).var()
rr=[]
for _ in range(2000):
    s=d.copy();s[k2]=shuffle(d[k2],h[k2],True);rr.append(vr(s))
print('Thu-Sat mean differs p',(1+(np.array(rr)>=vr(d)).sum())/2001,'missing weight',int(np.isnan(w).sum()))
