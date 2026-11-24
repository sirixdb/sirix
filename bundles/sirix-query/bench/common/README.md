# Shared benchmark protocol tools

What every kit in `bench/` shares: the shell helpers, the page-cache evictor, and the three tools
that carry the timed half of the [benchmark protocol](../../../../docs/DEVELOPMENT.md#the-benchmark-protocol).
The rules they implement are requirements
[CONV-302 to CONV-304](../../../../spec/CONV-conventions.md#conv-302--timed-acceptance-verdict); this
page says how to use them.

| File | What it does |
|---|---|
| `bench-common.sh` | Sourced by the kits: failure-loud helpers, the cool gate, eviction, Gradle, and the run record |
| `evict.py` | Drops the page cache for the files of one database, without root |
| `timed_gate.py` | The timed acceptance verdict: paired difference, noise threshold, stop rule, allocation check |
| `bench_ledger.py` | The results ledger, the environment capture, the conditions lookup and the run directory |
| `bench_trend.py` | The nightly trend report over the ledger |
| `test_*.py` | Their tests; `testdata/` holds the A/A control of 2026-11-23 |
| `MUTATION_DISPOSITIONS.md` | The mutation record of the three tools |

All three tools are standard-library Python 3 and none of them times anything.

## Recording a run

Every kit run writes `environment.json` into its output directory before its first timed process:
kernel, firmware, CPU, frequency policy, power limits, temperatures, runtimes and the filesystem of
the benchmark data. A probe that cannot be read is recorded as unavailable with its reason.

To make a run a protocol result, name a results ledger:

```bash
export SIRIX_BENCH_LEDGER="$HOME/.local/state/sirix-bench/ledger.jsonl"   # any durable path outside a worktree
```

The run then also

1. computes the fingerprint of the machine and looks it up in
   [`docs/bench/CONDITIONS.md`](../../../../docs/bench/CONDITIONS.md). The latest entry containing the fingerprint applies, including a new baseline for an unchanged
   fingerprint. If no entry holds it, the run
   refuses to start and names the fields that changed since the previous run. Add a dated entry
   first; `python3 bench_ledger.py fingerprint` prints what to put in it.
2. appends one row per fresh process and figure to the ledger, with the commit and kind of binary of
   each arm. The ledger is JSON Lines and is only ever appended to. It stays on the measuring
   machine; a pull request cites the rows it relies on.

| Variable | Meaning |
|---|---|
| `SIRIX_BENCH_LEDGER` | The ledger file. Unset: the kit runs as before and its numbers are not protocol results |
| `SIRIX_BENCH_TIER` | `gate`, `control` (default) or `nightly` |
| `SIRIX_BENCH_NIGHT` | The night of a nightly run, `YYYY-MM-DD` |
| `SIRIX_BENCH_JUDGE` | Space-separated benchmark names a gate judges; default: every benchmark the run measures |
| `SIRIX_BENCH_MIN_PAIRS` | A gate's minimum number of pairs, 10 to 20 (default 10) |
| `SIRIX_BENCH_CONDITIONS` | A conditions record other than the one in this checkout (for tests) |

Every kit measurement carries its dataset identity in `params`: JSONBench records its `tier`,
and ClickBench cold rounds require `--dataset NAME` with a ledger. The name is caller-stated,
contains only letters, digits, dots, underscores and hyphens, and is never a database path. Both
`import-jsonbench` and `import-clickbench` accept repeatable `--param NAME=VALUE` options.

A kit that stops early — a failed process, an interrupt — still appends the processes it measured.
A run whose answers turn out wrong is recorded with every measurement rejected
(`bench_ledger.py reject --run DIR --reason ...`), so its timings never enter a verdict or a trend.

A ledger row needs the commit of every arm. The JVM arms take it from this checkout; for an
ahead-of-time image pass it (`--commit` to `jsonbench/run-benchmark.sh`, `--arm-commit NAME=SHA` to
`clickbench/cold-rounds.sh`).

## Judging a change

A change on a timed path is judged from alternating fresh-process pairs of a `baseline` and a
`candidate` arm. For each benchmark the gate computes the paired difference *d* (the mean log ratio
of candidate to baseline) and the benchmark's own noise threshold *T* (the 0.99 quantile of the
difference under every relabelling of the pairs, with the mean removed), and gives one verdict:

| Verdict | When | Effect |
|---|---|---|
| `regression` | *d* is above 4% and above *T* | the comparison fails |
| `improvement` | the same, downward | reported |
| `unstable` | neither, and *T* is above 4% | not judged: the run could not have seen a 4% slowdown. Open an issue against the benchmark; never widen the bar |
| `unchanged` | otherwise | no regression shown |

The run length is not chosen by hand. It collects at least 10 accepted pairs and stops as soon as
every judged benchmark's threshold is at most 3%, or at 20 pairs. The stop reads thresholds only,
and a threshold does not change when one arm is slower or faster by a constant factor, so stopping
cannot favour a result. A gate run that ended before or after that point gets no verdict.

Allocation is judged apart from time and exactly: one fresh process per arm with the allocation
profiler and no timing. Each arm's figure is the unrounded median of its accepted allocation legs.
The candidate's median minus the baseline's is rounded to whole bytes, ties to the even byte, and
must be at most zero. Reports show both rounded arm figures and the rounded delta.

With the ClickBench kit, a gate is one command:

```bash
SIRIX_BENCH_TIER=gate SIRIX_BENCH_JUDGE="suite" \
  ../clickbench/cold-rounds.sh /path/to/db --dataset hits-100m \
    --arm baseline=/path/to/old-image --arm-commit baseline=<sha> \
    --arm candidate=/path/to/new-image --arm-commit candidate=<sha>
```

It runs rounds until the stop rule ends them, records everything, prints the verdicts and exits 1 on
a regression.

Any other driver uses the run directory directly:

```bash
L="python3 bundles/sirix-query/bench/common/bench_ledger.py"
$L begin --out "$RUN" --tier gate --kit jmh \
   --arm "baseline,commit=$OLD,binary=jvm,java=$JAVA" --arm "candidate,commit=$NEW,binary=jvm,java=$JAVA"
for PAIR in $(seq 1 20); do
  # one JMH invocation per arm here (forks = 1, a time mode, no profiler), alternating which arm goes first
  $L import-jmh --run "$RUN" --json "$PAIR-baseline.json"  --arm baseline  --pair "$PAIR" --position 1 --process "$PAIR"
  $L import-jmh --run "$RUN" --json "$PAIR-candidate.json" --arm candidate --pair "$PAIR" --position 2 --process "$PAIR"
  [ "$($L stop --run "$RUN" | tail -1)" = stop ] && break
done
# once: the allocation legs, each arm in its own fresh process with -prof gc
$L import-jmh --run "$RUN" --json alloc-baseline.json  --arm baseline  --process 1 --allocation-leg
$L import-jmh --run "$RUN" --json alloc-candidate.json --arm candidate --process 1 --allocation-leg
$L judge --run "$RUN"        # appends to the ledger, then prints the verdicts; exit 1 = regression
```

A pair rejected for outside load is still recorded: add `--rejected "<reason>"` to both imports. It
does not count towards the pair numbers above, and it cannot be accepted later. A run is judged
once: after `judge`, the run directory takes no more measurements.

`timed_gate.py judge ROWS.jsonl` and `timed_gate.py stop ROWS.jsonl` apply the same analysis to a
file of measurement rows without a ledger, for looking at data that is not a protocol result.

A control run (`--tier control`), such as an A/A run of one build against itself, is recorded and
judged the same way, with a pair count fixed by its plan instead of the stop rule. A single-arm run
(`--arm head,...`) is recorded with `commit` and never judged.

## The nightly trend report

```bash
python3 bundles/sirix-query/bench/common/bench_trend.py report            # the latest night in the ledger
python3 bundles/sirix-query/bench/common/bench_trend.py report --night 2026-12-24
```

A nightly run is a single-arm run with `SIRIX_BENCH_TIER=nightly` and `SIRIX_BENCH_NIGHT` set, with
at least 20 fresh processes per benchmark. The report compares each series' level tonight (the
median of its processes) with the median of the 7 oldest of the 14 nights before, and with the
series' own night-to-night noise. A change above 2% and above that noise is `unconfirmed` on its
first night and `drift` when the next night repeats it. The report exits 1 when a series is in
`drift`. It is an alarm to investigate, never a verdict on a change. Nothing schedules a nightly run
yet; the analysis is ready for the machine that will.

## Tests

```bash
cd bundles/sirix-query/bench/common && python3 -B -m unittest discover -p 'test_*.py'
```

`test_kit_recording.py` drives the real kit scripts with stub engines; it times nothing and opens no
database.
