"""Turn a clickBench leg log into the 43x3 result JSON rank.py scores: python3 mkleg.py TAG LOG.

Writes legs/query-<TAG>.json next to this file, using query-N1FULL1.json as the metadata template
(machine, tags, date) so the leg is comparable with the board entries. Only a leg run with --tries 3
produces a scorable file: the site's selectRun needs all three tries per query.
"""
import json, os, re, sys
RIG = os.path.dirname(os.path.abspath(__file__))
tag, log = sys.argv[1], sys.argv[2]
text = open(log, errors="ignore").read()
# "# q12 try 2: wall=11.170 s ..." is the authoritative per-try time; the table's hot column is a
# derived min and cannot rebuild the triple the site's selectRun needs.
tries = {}
for m in re.finditer(r'^# q(\d+) try (\d+): wall=([0-9.]+) s', text, re.M):
    tries.setdefault(int(m.group(1)), {})[int(m.group(2))] = float(m.group(3))
# A query that FAILED has a table row but no wall line; leave it None so rank.py applies the
# site's own missing-answer penalty rather than a number we invented.
result = []
for q in range(43):
    t = tries.get(q)
    result.append([t.get(1), t.get(2), t.get(3)] if t and len(t) >= 3 else None)
ref = json.load(open(os.path.join(RIG, "legs", "query-N1FULL1.json")))
out = dict(ref)
out["result"] = result
out["system"] = "SirixDB (segment lane)"
json.dump(out, open(os.path.join(RIG, "legs", f"query-{tag}.json"), "w"))
have = [q for q in range(43) if result[q]]
print(f"{tag}: {len(have)}/43 queries with 3 tries; missing {[q for q in range(43) if not result[q]]}")
if have:
    print("hot sum: %.1f s" % sum(min(result[q][1], result[q][2]) for q in have))
