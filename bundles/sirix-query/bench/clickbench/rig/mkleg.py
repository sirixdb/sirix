"""Turn a clickBench leg log into the 43x3 result JSON rank.py scores: python3 mkleg.py TAG LOG.

Writes legs/query-<TAG>.json next to this file. Only a leg run with --tries 3 produces a scorable
file: the site's selectRun needs all three tries per query. A log records timings and nothing else,
so every field a run would have to observe -- date, machine, load_time, data_size -- is written null
rather than copied from another leg; inventing them would describe a run that did not happen.

A raw log does not carry the regime it was measured under either, so mkleg never mints publication
provenance: it records the scope it can establish and no regime at all. A log that declares a
steering-only protocol (every rig log opens with a `# steering-only` line) or that records more than
three tries for any query -- which historical wider-schedule logs do -- converts as steering;
anything else converts as unknown. rank.py refuses both, and refuses any leg with no rig.regime, so
a converted leg cannot rank until someone records the conditions it actually ran under.
"""
import json, os, re, sys
from rank import STEERING_LOG_MARKER
from rank import STEERING_SCOPE
RIG = os.path.dirname(os.path.abspath(__file__))
UNKNOWN_SCOPE = "unknown"
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
widest = max((len(t) for t in tries.values()), default=0)
declared = any(line.startswith(STEERING_LOG_MARKER) for line in text.splitlines())
out = dict(system="SirixDB (segment lane)", date=None, machine=None, cluster_size=1,
           proprietary="no", hardware="cpu", tuned="no",
           tags=["Java", "document-oriented", "embedded", "versioned"],
           load_time=None, data_size=None, concurrent_qps=None, concurrent_error_ratio=None,
           result=result)
out["rig"] = dict(scope=STEERING_SCOPE if declared or widest > 3 else UNKNOWN_SCOPE,
                  declared_in_log=declared, widest_recorded_tries=widest,
                  source_log=os.path.abspath(log))
json.dump(out, open(os.path.join(RIG, "legs", f"query-{tag}.json"), "w"))
have = [q for q in range(43) if result[q]]
print(f"{tag}: {len(have)}/43 queries with 3 tries; missing {[q for q in range(43) if not result[q]]}")
print(f"{tag}: scope {out['rig']['scope']}; rank.py refuses every leg that does not state the publication regime")
if have:
    print("hot sum: %.1f s" % sum(min(result[q][1], result[q][2]) for q in have))
