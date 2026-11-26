# The nightly benchmark tier

Every evening a GitHub Actions job builds `main`, measures a curated set of benchmarks in fresh
processes, appends the figures to the results ledger, and publishes a dashboard. A confirmed upward
change opens one issue. The rules are [CONV-303](../../../../spec/CONV-conventions.md#conv-303--results-ledger-and-conditions-record)
and [CONV-304](../../../../spec/CONV-conventions.md#conv-304--nightly-trend-tier); the narrative is in
[DEVELOPMENT.md](../../../../docs/DEVELOPMENT.md#the-nightly-tier). This page says how the pieces fit
and how to run them.

| File | What it does |
|---|---|
| `benchmarks.json` | The curated set: JMH benchmark methods, their parameters and their allocation batch |
| `nightly_run.py` | Records one night: 20 fresh processes of the set, CPU time, wall-clock and allocation, into the ledger |
| `nightly_analysis.py` | CONV-304 states and Apache Otava change points for every series, merged into `analysis.json` |
| `nightly_dashboard.py` | The static dashboard: inline SVG, no script, no request to another site |
| `nightly_issues.py` | One issue per change point, deduplicated, labelled, linked |
| `nightly_publish.py` | Pulls and pushes the `nightly-data` branch, replaces the `gh-pages` branch |
| `nightly_window.py` | The reserved weekday window and the publishing context, judged in code |
| `requirements.txt` | Apache Otava and the two libraries its analysis needs, pinned by hash |
| `test_*.py` | Their tests; nothing in them is timed |

## What is measured

`:sirix-benchmarks` is built into its JMH jar, and each entry of `benchmarks.json` runs in its own JVM,
`-f 1`, once per process, 20 processes per night (CONV-304):

| Metric | From | Judged on a hosted runner |
|---|---|---|
| `cpu_ns_per_op` | `CpuTimeProfiler` (`-prof io.sirix.benchmark.CpuTimeProfiler`): process CPU time, all threads, per operation | yes |
| `alloc_bytes_per_op` | the CONV-302 allocation leg: single-shot batches, `-prof gc`, escape analysis off, no collector | yes |
| `ns_per_op` | the same process as the CPU figure: wall-clock average time | **no, informational** |

Wall-clock time moves with the neighbours of a shared virtual machine; CPU time and allocation do
not, or far less. The wall-clock series is drawn and analysed all the same, marked as not judged, so the
dedicated machine of stage 2 has a baseline to be compared with.

The recorder works one process at a time across the whole set, so a time budget that ends a night early
leaves every series with the same number of processes. A night with fewer than 20 processes is not a
recorded night of that series (CONV-304) and the job fails.

## Where the history lives

| Branch | Holds | Writer |
|---|---|---|
| `nightly-data` | `ledger.jsonl`, the append-only results ledger, one commit per night | the workflow only |
| `gh-pages` | the generated dashboard, one commit that every night replaces | the workflow only |

Pages is served from `gh-pages` (repository settings: Pages, deploy from a branch). The ledger on
`nightly-data` is checked to be an append-only extension of the branch before it is pushed.

## Running it by hand

Commands run from the repository root. A hosted-runner fingerprint must be in
[`docs/bench/CONDITIONS.md`](../../../../docs/bench/CONDITIONS.md) before the recorder will measure:

```bash
python3 bundles/sirix-query/bench/common/bench_ledger.py fingerprint \
    --runner kind=hosted,provider=github,label=ubuntu-24.04      # the fingerprint and its fields
./gradlew :sirix-benchmarks:jmhJar --no-daemon
python3 bundles/sirix-query/bench/nightly/nightly_run.py \
    --jar bundles/sirix-benchmarks/build/libs/*-jmh.jar --java "$JAVA_HOME/bin/java" \
    --ledger /tmp/ledger.jsonl --night "$(date -u +%F)" --out /tmp/night \
    --runner kind=hosted,provider=github,label=ubuntu-24.04 --budget-seconds 6000
pip install --require-hashes --no-deps -r bundles/sirix-query/bench/nightly/requirements.txt
python3 bundles/sirix-query/bench/nightly/nightly_analysis.py --ledger /tmp/ledger.jsonl --out /tmp/analysis.json
python3 bundles/sirix-query/bench/nightly/nightly_dashboard.py --analysis /tmp/analysis.json --out /tmp/site
python3 bundles/sirix-query/bench/nightly/nightly_issues.py --analysis /tmp/analysis.json --dry-run \
    --repository sirixdb/sirix --dashboard https://sirixdb.github.io/sirix/
```

`nightly_run.py` exits 4 when a series is short of its processes and 5 when the night is already in the
ledger. Local runs publish nothing: the publisher and the issue step refuse outside a scheduled or
dispatched run of the default branch of the canonical repository.

## When the recorder refuses a new machine

CONV-303 refuses to measure when no conditions entry holds the run's fingerprint. A hosted runner's
fingerprint is its class: the label, the architecture, the CPU model, the logical CPU count and the
memory. GitHub's pool mixes processors, so the first night on a new CPU model fails with the fingerprint
printed in the job summary. A maintainer appends a dated entry to `docs/bench/CONDITIONS.md` (a `new
baseline`, since a different processor is a different condition) and the next night proceeds. Series of
two conditions are never compared with each other, so each processor builds its own history.

## The window

Nothing public is committed, pushed, deployed or opened on a weekday between 04:00 and 17:30
Europe/Berlin. The cron (20:17 UTC) puts the job in the evening; `nightly_window.py` is the second guard
and is called by the code that pushes (`nightly_publish.py`) and the code that opens issues
(`nightly_issues.py`), with a 20 minute margin. `test_nightly_window.py` checks the cron and the job's time
limit against the window on every day of three years, including both clock changes. If a night's
publication is refused, its rows stay in the workflow's artifact (30 days); append them to `ledger.jsonl`
on `nightly-data` by hand, outside the window.
