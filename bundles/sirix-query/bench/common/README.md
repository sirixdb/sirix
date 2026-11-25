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

The ClickBench cold-round runner and JSONBench runner write `environment.json` into their output
directory before their first timed process:
kernel, firmware, CPU, frequency policy, power limits, temperatures, runtimes and the filesystem of
the benchmark data. A probe that cannot be read is recorded as unavailable with its reason.
Version probes exclude the JVM option environment variables; echoed option announcements from
wrappers or supplied runtime text, including multiline continuations, are discarded before capture.
JMH and bitemporal drivers use the run-directory commands below; they do not record automatically.
Commands on this page run from the repository root.

To make a run a protocol result, name a results ledger:

```bash
export SIRIX_BENCH_LEDGER="$HOME/.local/state/sirix-bench/ledger.jsonl"   # any durable path outside a worktree
```

The run then also

1. computes the fingerprint of the machine and looks it up in
   [`docs/bench/CONDITIONS.md`](../../../../docs/bench/CONDITIONS.md). If none matches, add an entry
   under [CONV-303](../../../../spec/CONV-conventions.md#conv-303--results-ledger-and-conditions-record)
   before measuring; `python3 bundles/sirix-query/bench/common/bench_ledger.py fingerprint` prints
   the fingerprint and its fields.
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
Every successfully opened ledger run also appends its run row, even if its first process failed
and no measurements exist. A kit refused before opening the run appends nothing.
A run whose answers turn out wrong is recorded with every measurement rejected
(`bench_ledger.py reject --run DIR --reason ...`), so its timings never enter a verdict or a trend.
Rejection is refused once any measurement is already in the ledger, even without a recording marker.
Commit retries reconcile complete row contents; a conflicting identity refuses the whole append.
A failed ClickBench hot try withholds the query's hot figure and the suite hot total; cold figures
are preserved and the skipped query is reported.

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
must be at most zero. Reports show both rounded arm figures and the rounded delta. If a series has
allocation legs, both arms must retain an accepted leg or judgment is refused.

The allocation-leg settings and reproducibility requirements are defined in
[CONV-302](../../../../spec/CONV-conventions.md#conv-302--timed-acceptance-verdict).
They measure allocation as written, an **upper bound** on production allocation. Use the same
settings for both arms, with a heap large enough for the non-collecting run. `import-jmh
--allocation-leg` checks the required settings and converts the profiler's bytes per batch to
bytes per operation. Fresh-process equality of the whole-byte figure is a property of the settings
and the batch, not of the importer. A batch of 100,000 operations per iteration keeps the residual
far below half a byte per operation for the gate's benchmarks (observed about 0.0001 B/op).
The measured sub-byte residual is
recorded in the [compiler follow-up evidence](MUTATION_DISPOSITIONS.md#compiler-follow-up-root-cause-and-verification).

With the ClickBench kit, set `OLD` and `NEW` to the full commit SHAs used to build the two
images, then run the gate:

```bash
SIRIX_BENCH_TIER=gate SIRIX_BENCH_JUDGE="suite" \
  bundles/sirix-query/bench/clickbench/cold-rounds.sh /path/to/db --dataset hits-100m \
    --arm baseline=/path/to/old-image --arm-commit "baseline=$OLD" \
    --arm candidate=/path/to/new-image --arm-commit "candidate=$NEW"
```

It runs rounds until the stop rule ends them, records everything, prints the verdicts and exits 1 on
a regression.

Any other driver uses the run directory directly:

```bash
L="python3 bundles/sirix-query/bench/common/bench_ledger.py"
mkdir -p "$RUN"             # a fresh output directory; OLD and NEW are full commit SHAs
$L begin --out "$RUN" --tier gate --kit jmh \
   --arm "baseline,commit=$OLD,binary=jvm,java=$JAVA" --arm "candidate,commit=$NEW,binary=jvm,java=$JAVA"
for PAIR in $(seq 1 20); do
  # one JMH invocation per arm here (forks = 1, a time mode, no profiler), baseline then candidate
  $L import-jmh --run "$RUN" --json "$PAIR-baseline.json"  --arm baseline  --pair "$PAIR" --position 1 --process "$PAIR"
  $L import-jmh --run "$RUN" --json "$PAIR-candidate.json" --arm candidate --pair "$PAIR" --position 2 --process "$PAIR"
  [ "$($L stop --run "$RUN" | tail -1)" = stop ] && break
done
# once: the allocation legs, each arm in its own fresh process with -prof gc, run as
#   java -jar <arm>.jar <benchmarks> -f 1 -bm ss -bs 100000 -wi 3 -i 3 -prof gc -rf json -rff alloc-<arm>.json \
#     -jvmArgsAppend "-Xmx3g -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -XX:-DoEscapeAnalysis -XX:-UseJVMCICompiler"
$L import-jmh --run "$RUN" --json alloc-baseline.json  --arm baseline  --process 1 --allocation-leg
$L import-jmh --run "$RUN" --json alloc-candidate.json --arm candidate --process 1 --allocation-leg
$L judge --run "$RUN"        # appends to the ledger, then prints the verdicts; exit 1 = regression
```

A pair rejected for outside load is still recorded: add `--rejected "<reason>"` to both imports. It
does not count towards the accepted-pair minimum or cap, and it cannot be accepted later. Give every
attempt a new pair number, including replacements for rejected pairs; extend the driver's loop to
collect the required accepted pairs. A run is judged once: after `judge`, the run directory takes no
more measurements.

`timed_gate.py judge ROWS.jsonl` and `timed_gate.py stop ROWS.jsonl` apply the same analysis to a
file of measurement rows without a ledger, for looking at data that is not a protocol result.

A paired control run (`--tier control`), such as an A/A run of one build against itself, uses a pair
count fixed by its plan instead of the stop rule. The ClickBench kit records it without a verdict;
call `bench_ledger.py judge --run DIR` afterwards to judge it under
[CONV-302](../../../../spec/CONV-conventions.md#conv-302--timed-acceptance-verdict).
A single-arm run (`--arm head,...`) is recorded with `commit` and never judged.

## The nightly trend report

```bash
python3 bundles/sirix-query/bench/common/bench_trend.py report            # the latest night in the ledger
python3 bundles/sirix-query/bench/common/bench_trend.py report --night 2026-12-24
```

A nightly run is a single-arm run with `SIRIX_BENCH_TIER=nightly` and `SIRIX_BENCH_NIGHT` set, with
at least 20 fresh processes per benchmark. The night must be a real calendar date written exactly
as `YYYY-MM-DD`; invalid dates are refused before opening a run and on ledger append or read.
The report compares each series' level tonight (the
median of its processes) with the median of the 7 oldest of the 14 nights before, and with the
series' own night-to-night noise. A change above 2% and above that noise is `unconfirmed` on its
first night and `drift` when the next night repeats it. The report exits 1 when a series is in
`drift`. It is an alarm to investigate, never a verdict on a change. Nothing schedules a nightly run
yet; the analysis is ready for the machine that will.
The default date is the latest nightly run's date, including an aborted or rejected run. If that
night has no series with enough accepted processes, the report names the night and exits 2;
it never substitutes an earlier night. When other series are usable, missing series with earlier
history are listed as `not measured`.

## Tests

```bash
cd bundles/sirix-query/bench/common && python3 -B -m unittest discover -p 'test_*.py'
```

`test_kit_recording.py` drives the real kit scripts with stub engines; it times nothing and opens no
database.
