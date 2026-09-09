"""Leaderboard rank of a leg: python3 rank.py TAG-or-path [TAG-or-path...].

Reproduces the ClickBench site's own scoring (index.html: selectRun / relativeQueryTime /
renderSummary baseline): per query, hot = min(try2, try3), cold = try1, combined = (0.6 hot + 0.2 cold)
/ 0.8; a query with no answer is charged 2 x max(300 s, slowest answered query); the score is the
geometric mean over the 43 queries of (0.01 + ours) / (0.01 + best on the board). The +0.01 s offset on
both sides means a 0.05 s answer against a 0.000 s best still costs ln(6) = 1.8 - the last few ln of
a top-10 run live under 100 ms.

Two boards: DEFAULT = the page's default view (all machines except the small/metal ones, tuned=no,
cpu; in-memory systems excluded for cold/combined); C6A = the c6a.4xlarge cpu entries only, which is
the machine the target is scored on. Thresholds print as r10=... (the rank-10 geomean), and every leg
prints how many ln it still needs for rank 15 and rank 10.

A leg is legs/query-<TAG>.json in this directory or any path to a ClickBench result JSON, and it is
ranked only if its rig.scope says "publication" and it records the rig.regime it was measured under.
Scope is purpose, not proof of conditions: a steering, diagnostic, composed or unstated leg refuses,
so no conversion or assembly path can turn an unknown run into a rank. The regime is printed with
every rank, because a board position quoted without its measurement conditions is not a result. The board is
board/data.generated.js, a snapshot of the site's data file; refresh it from
https://benchmark.clickhouse.com/data.generated.js when the board moves.
"""
import json, math, os, sys
RIG = os.path.dirname(os.path.abspath(__file__))
# Provenance contract, in two independent parts. rig.scope states PURPOSE: only a leg curated as a
# standing publication leg ranks, and steering, diagnostic, composed and unstated purposes refuse.
# rig.regime states the PHYSICAL CONDITIONS the run was measured under; it is required, is printed
# with every rank, and never justifies the scope.
PUBLICATION_SCOPE = "publication"
STEERING_SCOPE = "steering"
STEERING_LOG_MARKER = "# steering-only"
raw = open(os.path.join(RIG, "board", "data.generated.js")).read()
raw = raw[raw.index("["):raw.rindex("]") + 1]
ALL = [d for d in json.loads(raw) if not d.get("fake")]
NQ = 43
EXCL = {"c6a.2xlarge", "c6a.xlarge", "c6a.large", "c8g.4xlarge", "t3a.small", "c6a.metal"}
def cpu(d): return d.get("hardware") == "cpu" or not d.get("hardware")
BOARDS = {
    "DEFAULT": lambda d: d["machine"] not in EXCL and d.get("tuned", "no") == "no" and cpu(d),
    "C6A": lambda d: d["machine"] == "c6a.4xlarge" and cpu(d),
}
def leg_path(arg):
    return arg if os.path.exists(arg) else os.path.join(RIG, "legs", f"query-{arg}.json")
def sel(t, metric):
    if t is None or not isinstance(t, list) or len(t) < 3: return None
    cold = t[0]; hot = min(t[1], t[2]) if (t[1] is not None and t[2] is not None) else None
    if metric == "cold": return cold
    if metric == "hot": return hot
    return (hot * 0.6 + cold * 0.2) / 0.8 if (hot is not None and cold is not None) else None
def baseline(systems):
    b = {}
    for m in ("cold", "hot", "combined"):
        # site: per (query, run) min over systems, then selectRun on that synthetic triple
        runs = [[min([x for x in (d["result"][q][r] if q < len(d["result"]) and isinstance(d["result"][q], list)
                                  and len(d["result"][q]) > r else None for d in systems) if x is not None])
                 for r in range(3)] for q in range(NQ)]
        b[m] = [sel(runs[q], m) for q in range(NQ)]
    return b
def score(res, metric, base):
    vals = [x for x in (sel(t, metric) for t in res) if x is not None]
    fb = 2 * max([300] + vals)
    acc = 0; n = 0; contrib = []
    for q in range(NQ):
        b = base[q]
        if b is None: continue
        c = sel(res[q], metric) if q < len(res) else None
        if c is None: c = fb
        l = math.log((0.01 + c) / (0.01 + b)); acc += l; n += 1; contrib.append((l, q, c, b))
    return math.exp(acc / n), contrib
def leg_name(tag):
    return os.path.basename(tag).removeprefix("query-").removesuffix(".json")
def main():
    for tag in sys.argv[1:]:
        path = leg_path(tag)
        with open(path) as stream:
            document = json.load(stream)
        rig = document.get("rig", {})
        scope = rig.get("scope")
        if scope != PUBLICATION_SCOPE:
            raise SystemExit(
                f"{path}: rig.scope is {scope!r}, not {PUBLICATION_SCOPE!r}. Only a leg curated as a standing "
                "publication leg ranks against the board; steering, diagnostic, composed and unstated purposes all "
                "refuse. Use measure.py reports for steering measurements.")
        regime = rig.get("regime")
        if not isinstance(regime, str) or not regime.strip():
            raise SystemExit(
                f"{path}: rig.regime is missing. A rank is meaningless without the physical conditions it was "
                "measured under; record them on the leg before ranking it.")
    for tag in sys.argv[1:]:
        rig = json.load(open(leg_path(tag)))["rig"]
        print(f"[regime ] {leg_name(tag)}: {rig['regime']}")
        for key in ("measured", "limits"):
            if rig.get(key):
                print(f"            {key}: {rig[key]}")
    for bname, pred in BOARDS.items():
        systems = [d for d in ALL if pred(d)]
        for metric in ("combined", "hot", "cold"):
            board_systems = [d for d in systems if not (metric in ("cold", "combined") and "in-memory" in d["tags"])]
            base = baseline(board_systems)[metric]
            board = sorted((score(d["result"], metric, base)[0], d["system"]) for d in board_systems)
            def rank(g): return 1 + sum(1 for s, _ in board if s < g)
            thr = " ".join(f"r{r}={board[r-1][0]:.2f}" for r in (10, 15, 20, 30, 40, 50))
            print(f"[{bname:7}] {metric:8} n={len(board)}  {thr}")
            for tag in sys.argv[1:]:
                res = json.load(open(leg_path(tag)))["result"]
                g, contrib = score(res, metric, base)
                total = sum(l for l, *_ in contrib)
                need15 = total - NQ * math.log(board[14][0]); need10 = total - NQ * math.log(board[9][0])
                name = leg_name(tag)
                print(f"    {name:10} geomean={g:.3f} rank {rank(g)}   Σln={total:.2f} (rank15 needs −{need15:.2f}, rank10 −{need10:.2f})")
                if metric == "combined" or (metric == "hot" and bname == "C6A"):
                    contrib.sort(reverse=True)
                    print("      worst: " + ", ".join(f"q{q} {c:.2f}/{b:.3f} {l:.2f}" for l, q, c, b in contrib[:14]))

if __name__ == "__main__":
    main()
