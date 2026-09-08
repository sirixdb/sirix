# ClickBench rig

The scripts that load, run, gate and **score** a ClickBench leg the way the leaderboard scores it.
They were extracted from a working session on 2026-09-06 so that the next agent starts from a
committed HEAD; the campaign state and the lever queue are in
[`docs/HANDOFF_SEGMENT_LANE_2026-09-06.md`](../../../../../docs/HANDOFF_SEGMENT_LANE_2026-09-06.md).

Every path is derived from `rig.env` (this directory → repo root) or overridable by environment
variable; working files live under `bundles/sirix-query/build/diagnostics/` (gitignored).

| file | what |
|---|---|
| `rig.env` | shared paths, JVM envelopes, load/serve flags, the box lock, the live-JVM guard |
| `load100m.sh [DIR]` | load the 100M corpus with the segment lane (~45–60 min, ~48 GB); writes the pointer the query scripts read |
| `suite100m.sh [TRIES]` | **the scoring leg**: 43 queries, 3 tries → `$D100M/suite100m.log` |
| `diag100m.sh Q[,Q…] [JVMFLAGS]` | one diagnostic run with `-Dsirix.projDiag=true`: routes, declines, `[proj]` counters — never for timing |
| `mkleg.py TAG LOG` | turn a leg log into `legs/query-TAG.json` (43 × 3 tries). It writes that path unconditionally, so reusing a tag overwrites a measured leg — pick one `legs/` does not already hold. Only `result` is measured; `date` and `data_size` are template fields copied into every leg and ignored by `rank.py` — do not read a leg's database size or run date off them |
| `rank.py TAG…` | **the score**: reproduces the site's scoring against `board/data.generated.js`; prints rank and Σln per board/metric |
| `load1m.sh` / `seggate1m.sh` | 1M load + correctness gate against DuckDB (`0 mismatch, 0 missing` required) |
| `junit.py START CLASS…` | read JUnit XML refusing anything older than the run start (a compile error leaves stale XML) |
| `collapsed.py FILE [pat…]` | summarise an async-profiler collapsed-stack file |
| `legs/` | reference legs: `N1FULL1` (rank 10, the old global-dictionary DB), `SEG2T`/`SEG3T`/`SEG3TB` (segment lane), `SEG4T` (the earlier measured leg the handoff's §4 lever ln values are drawn from) and `SEG5T` (historical snapshots); `SEG6T` — **the measured standing** (the handoff's §2 owns the number and the caveats) |
| `board/data.generated.js` | snapshot (2026-09-02) of https://benchmark.clickhouse.com/data.generated.js |

## The one loop that matters

**Measurement status, 2026-09-08:** Firstmate's 20-leg instrument study reports a
minimum detectable effect of approximately **1.345 summed ln** for this harness,
over a **3.023 ln** leg-to-leg range on unchanged code. The box now runs at a flat
**PL1 = PL2 = 50 W** power cap. `SEG6T` and every earlier campaign leg were measured
before that cap, so they are **not commensurable** with anything measured after it —
compare a post-cap leg only against another post-cap leg. The string-decode change
makes no performance claim; its performance effect is unverified pending measurement
resolution. Builds, tests and reviews are released, but this lane still needs explicit
Firstmate authorization for any 100M work and its merge remains held. A free lock does
not grant a benchmark window. See
[the string-decode report](../../../../../docs/CLICKBENCH_STRING_DECODE_2026-09-08.md).

The `STRDEC*` records in `legs/` preserve historical observations. They are not
accepted performance evidence and must not be used to claim this lane's gain.

```sh
cd bundles/sirix-query/bench/clickbench/rig
cat ../../../build/diagnostics/rig/current-100m-dir.txt   # which 100M DB the query scripts read
bash suite100m.sh 3                                   # ~10 min at the current state; one leg per box
python3 mkleg.py SEG7T "$(cat ../../../build/diagnostics/rig/current-100m-dir.txt)/suite100m.log"
python3 rank.py SEG7T SEG6T N1FULL1                   # read the [C6A] hot block
```

`rank.py` prints, per board and metric, the rank thresholds (`r10=3.35` is the target) and for each
leg its geomean, rank, Σln and the 14 worst queries as `qN ours/best ln`. **Score every leg; never
report seconds as progress** — 686.8 s total was rank 81 while 32.13 s was rank 10, and a query at
0.05 s against a 0.000 s best still costs ln(6) ≈ 1.8 because of the site's +0.01 s offset.

## Rules the scripts enforce, and why

- **One leg per box** (`take_lock` + `refuse_live_jvm`): the 100M envelope is 14 GB heap + 10 GiB
  arena; a second JVM gets the whole box OOM-killed (`exit 137` → read `dmesg` first).
- **Never launch a gate with a mutant in the tree**: `./gradlew :sirix-query:clickBench` recompiles
  from source. Check the gate log's `compileJava` line recompiled (not `UP-TO-DATE`) when it should.
- **A database is a build output without a version stamp**: reload 1M (`load1m.sh`, ~1 min) whenever
  the write path changed before trusting a gate against it. At 100M, only one database fits on the
  box; deleting one is a decision, not a side effect.
- **The DuckDB reference cannot coexist with the 100M Sirix DB on disk** — the 1M gate is where
  correctness is proven; 100M legs prove speed.
- **Do not put the main-class name on the launching command line** (`bash -c "… ClickBenchRunMain …"`
  would trip `refuse_live_jvm` on itself).
- `-Dsirix.projDiag=true` costs time: `diag100m.sh` for routes and counters, `suite100m.sh` for time.
