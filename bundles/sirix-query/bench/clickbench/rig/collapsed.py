"""collapsed.py FILE [pattern...]: summarise an async-profiler collapsed-stack file (top self frames, inclusive
totals for each pattern). Produce FILE with e.g. asprof -d 30 -e cpu -o collapsed -f FILE <pid> during the hot try."""
import sys, re, collections
f=sys.argv[1]; pats=sys.argv[2:]
tot=0; incl=collections.Counter(); self_=collections.Counter(); byframe=collections.Counter()
rows=[]
for line in open(f):
    line=line.rstrip('\n')
    if not line: continue
    stack,_,n=line.rpartition(' '); n=int(n); tot+=n
    frames=stack.split(';')
    rows.append((frames,n))
    self_[frames[-1]]+=n
    seen=set()
    for fr in frames:
        # inclusive by simple class.method (strip signature)
        key=fr
        if key in seen: continue
        seen.add(key); incl[key]+=n
print("total samples", tot)
print("\n-- top self frames --")
for k,v in self_.most_common(25): print(f"{v:7d} {100*v/tot:5.1f}%  {k[:150]}")
print("\n-- inclusive for matched patterns --")
for p in pats:
    s=sum(n for frames,n in rows if any(p in fr for fr in frames))
    print(f"{s:7d} {100*s/tot:5.1f}%  {p}")
print("\n-- inclusive top (io.sirix frames) --")
shown=0
for k,v in incl.most_common(400):
    if 'sirix' in k and shown<45:
        print(f"{v:7d} {100*v/tot:5.1f}%  {k[:160]}"); shown+=1
