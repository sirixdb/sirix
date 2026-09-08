# ClickBench measurement rig

Use a fixed paired comparison to measure a change. Each arm keeps the exact C6A score:
43 queries, three tries, hot = min(try 2, try 3), and the geometric mean of
`(0.01 + hot) / (0.01 + board best)`. The board snapshot and scoring arithmetic remain
in `rank.py` and `board/data.generated.js`. Positive reported benefit means the candidate
reduced sum-ln. Seconds and per-query ln contributions are reported alongside it.

**Measurement status, 2026-09-08:** Firstmate's 20-leg instrument study reports two
distinct figures for this harness, and they answer different questions. The **paired**
(A/A) minimum detectable effect is approximately **1.345 summed ln**: that is the
criterion for a difference read off one baseline/candidate leg pair, where pairing
cancels the common-mode component. The **3.023 ln** leg-to-leg range on unchanged code
is **unpaired** spread across those 20 independent legs — how far apart two unrelated
legs can land, not what a paired comparison can resolve. Do not judge a paired result
against the unpaired range. The box now runs at a flat
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

Install the analysis dependency once in your Python environment:
`python3 -m pip install -r bundles/sirix-query/bench/clickbench/rig/requirements.txt`.
Set `CB100M_DIR` to the existing database's parent directory and reserve a quiet rig window.
Then run from this checkout, substituting the two revisions and a fresh output path:

```sh
python3 bundles/sirix-query/bench/clickbench/rig/measure.py compare \
  --baseline aa4d81d54 --candidate YOUR_COMMIT \
  --out bundles/sirix-query/build/diagnostics/comparison-001 \
  --pairs 10 --effect-ln 0.5
```

Both revisions are built in isolated worktrees using the same current benchmark main and
process-guard sources. This explicit harness overlay leaves engine sources at the requested
commits and avoids applying new startup instrumentation to only one arm. The source overlay
hashes and query-catalog identity are recorded and must match across arms. Their classpaths
are copied and hashed before any timing begins. Collection uses a prespecified even number of pairs, with AB/BA order
randomized in balanced blocks. Each leg gets a fresh JVM. Build work, other Java workloads,
a changed CPU policy, or a changed power cap invalidate collection. The runner checks the two MSR RAPL limits it pins are
50 W by default; it never changes frequency, governor, turbo, or power settings. It records every
other RAPL domain, including the platform-managed MMIO limit, as observed context and notes a
mid-run change without invalidating the leg — those limits move on their own, and discarding hours of
measurement over a firmware adjustment would be worse than not watching them. The effective envelope
is the lower of the MSR and MMIO long-term limits, and the
[power audit](evidence/power-audit-20260908/README.md) shows historical cohorts did **not** share
one: 16 of the 20 quiet-control legs were sampled under an MMIO limit below the 50 W MSR cap, while
the fast-tries pilot ran at 76 W throughout. The
[live smoke](evidence/live-smoke-20260908/README.md) is one real leg taken to prove these gates fire,
pass and release on hardware; the MMIO limit moved seven times inside its 170 seconds while the MSR
limits held at 50 W, so gating on the platform value would have aborted it repeatedly. That record
also states what one leg cannot show.
The cooldown gate requires three package-temperature readings below 55 C, five seconds apart,
with a final launch recheck. Thermal throttle counts are recorded, not used as a pass/fail gate.
The Java-process census cannot exclude every background workload: reserve the quiet window
through Firstmate and inspect the retained load/thermal telemetry.

The report gives the mean paired benefit and nominal 95% paired t interval, the estimated
80%-power detectable effect, and required pair counts for the requested target. A second count
uses a one-sided 95% upper bound on noise. These estimates assume independent, approximately
normal whole-pair differences and stable conditions; they are not guarantees. Query covariance
is preserved by forming each suite difference before computing its uncertainty. Fewer than ten
pairs, zero observed spread from rounded timings, or an effect below estimated resolution is
explicitly **UNRESOLVED**. A resolved target describes measurement precision; the actual
candidate still needs its confidence interval to support improvement. Never extend a collection
until significance appears. Plan a new fixed series if a pilot shows more repetitions are needed.

The unchanged-runtime quiet 50 W calibration did **not** resolve 0.5 ln with ten null
pairs: its estimated 80%-power detectable effect was 1.345 ln, with 60 pairs projected
for 0.5 ln (158 using the upper noise bound and balanced ordering). Those figures are arithmetic
facts about the legs as measured, but that series was not one power regime: the
[power audit](evidence/power-audit-20260908/README.md) finds 16 of its 20 legs sampled under an MMIO
limit below the MSR cap, and its name refers to the MSR setting alone. Those are conditional
planning estimates from Sirix observations, not a hardware limit or a promise about a future
candidate. The split protocol did not improve that result. Read the
[variance study](../../../../../docs/CLICKBENCH_RIG_VARIANCE_2026-09-08.md) and
[query attribution](../../../../../docs/CLICKBENCH_RIG_VARIANCE_ATTRIBUTION_2026-09-08.md)
before investigating an individual timing anomaly.

[Additional tries](../../../../../docs/CLICKBENCH_RIG_FAST_TRIES_2026-09-08.md) reduced the
component variance of four leading fast queries in a selected-query pilot, but q6 remained unstable.
That pilot was briefly exposed as an opt-in `--steering-fast-tries 5|9` mode; **the live mode has
been removed** pending a separate full-context calibration, because its full-suite resolution was
never validated and its within-pilot component figures cannot be compared against the full-suite
cohorts. Every leg is three tries per query, and the command above is the only comparison command.
The finding itself is kept in that report for whoever calibrates it.

[JIT/GC diagnostics](../../../../../docs/CLICKBENCH_RIG_JIT_GC_2026-09-08.md) also did not
justify changing the default compilation or GC policy. No 0.5-ln precision guarantee is made.

Exit 0 means collection/analysis completed with the target resolved under that model; exit 3 means
it completed but the target is unresolved; exit 2 means an invalid or incomplete collection. An
existing output directory is never overwritten. `plan.json`, runtime manifests, raw logs,
telemetry, every paired leg, `report.json`, `report.md`, and sorted `queries.csv` retain the evidence.
The plan archives the exact board bests, board hash and measurement-script hashes; replay uses
those board bests even if the repository's board snapshot is subsequently refreshed.

### Disk cost and what is disposable

`compare --baseline REV --candidate REV` builds each arm in its own `git worktree` checkout under
`--out`, so a comparison transiently holds two full source checkouts plus each one's Gradle build
outputs. That is the rig's own scratch, and it is **deleted automatically** — with its worktree
registration — once the fixed plan finishes and the reports are written, including a completed
**UNRESOLVED** result. What stays behind is the evidence: `plan.json`, the frozen runtimes under
`runtime-*/frozen/`, every leg, the logs, telemetry and reports.

If a run fails or is interrupted the scratch is deliberately kept for diagnosis, and the command
prints the exact `git worktree remove` line for each checkout it created. Nothing else under `--out`
is disposable. Measure what a given output directory holds with:

```sh
du -sh OUTPUT                              # everything the run holds, recursively
du -csh OUTPUT/runtime-*/source 2>/dev/null # per-arm scratch and its total, while it still exists
```

`prepare --revision REV` also leaves a checkout, on purpose: its output is a prepared runtime meant
to be reused by a later `--baseline-runtime`, so the operator removes it when done with it.

The "never delete shared diagnostics" rule above protects the 100M database, the source corpora and
the retained raw diagnostic evidence. It does not protect these scratch checkouts, which are
reproducible by rebuilding the same revision.

`measure.py analyze OUTPUT` reproduces a completed report without launching Java. A failed or
partial plan cannot be reported as a complete paired measurement.

## Protocol boundaries

Every leg runs the full suite in one JVM with the inherited OS page cache. This is a recorded
condition, not a promise that the entire 48 GB database fits in 32 GB RAM.

Two protocol arms were built, measured and then removed. The **split** arm ran q0..20 and q21..42 in
separate JVMs with a cooldown before each; it made variance worse, not better — 1.054766 against
0.784485 ln² suite variance — because the restart moves variability between queries instead of
removing shared variation. The **cache-drop** arm would have evicted the OS page cache before each
fresh JVM; it was cancelled before it ever ran and produced no observation. Both were deleted rather
than kept as options: a mode nobody may use still obliges every reader and every report to say which
protocol a leg used. The evidence and the write-ups stay —
[split study](evidence/split50-20260908/README.md),
[attribution](../../../../../docs/CLICKBENCH_RIG_VARIANCE_ATTRIBUTION_2026-09-08.md), and the
cache-drop authorization request preserved under `evidence/split50-20260908/` — so the negative
results outlive the code. Reviving either means rebuilding it against fresh evidence.

Provenance is affirmative, and `rank.py` fails closed on it. `rig.scope` states **purpose**: only a
leg curated as a standing publication leg ranks, and steering, diagnostic, composed and unstated
purposes all refuse, which keeps capped, converted and diagnostic observations out of the
ranking without anyone having to remember to mark them. `rig.regime` separately states the
**physical conditions** the run was measured under. It is required — a leg with no regime refuses —
and it is printed with every rank, because a board position quoted without its measurement
conditions is not a result. A regime never justifies a scope.

Six of the seven curated legs under `legs/` carry `publication`: N1FULL1, SEG2T, SEG3T, SEG4T, SEG5T
and SEG6T. The seventh, SEG3TB, carries `composed` and is refused: it is SEG3T with q21/q22/q28
spliced in from separate runs, not one measured leg. All seven were collected before the 50 W cap of
2026-09-08T03:58Z, and none of them recorded a temperature, power or exclusivity observation, so each
one's regime says so and names what could not be established. Their collection paths differ and are
mostly unrecorded: N1FULL1 predates `suite100m.sh` by three days and came from the out-of-repo
N-series tooling, SEG2T identifies no run at all, SEG3T falls on the day the script first appears,
and SEG4T/SEG5T/SEG6T postdate it but were never observed using it. SEG2T's measurement date and
SEG6T's exact run time are bounded by their commits, not observed, and the fields no leg ever
recorded — `machine`, `load_time` and `data_size` — are null rather than carrying a placeholder or an
inherited value. The throttled exports under
`evidence/thermal-20260908/` carry `diagnostic` and are refused as protocol experiments; their
regime is directly observed rather than inferred.
Provenance also travels with the log, not only with the JSON: every `suite.log` the runner writes
opens with a `# steering-only` line, so `mkleg.py` can record steering rather than merely failing to
recognize it. No conversion or
assembly path produces a rankable leg, because no converter mints publication provenance. No
cross-regime transfer to an uncapped publication run has been established by the quiet control.

## Entry points and ownership

`measure.py prepare --out FRESH_DIR` builds and freezes the current worktree, including local
changes. `--revision COMMIT` prepares an isolated revision instead. A later comparison can
use `--baseline-runtime MANIFEST --candidate-runtime MANIFEST`. Runtime hashes and the exact
6 GiB initial / 14 GiB maximum heap, 10 GiB arena, 5 GiB eager residency, and disabled JVMCI
compiler are verified. Use `--baseline-jvm-arg=-Dproperty=value` (and candidate equivalent) for
explicit mechanism ablations prepared from revisions; profiling options require diagnostics.
Original classpath provenance also detects an external snapshot JAR changing in place between
the two builds. Java's module image, VM library and release identity are hashed as well as the
launcher executable. Classpath or main-class overrides and argument files are rejected. Diagnostic
query-boundary records are output-receipt timestamps; use JVM events for exact phase timing.

After preparing a runtime containing this harness, run the native-lock integration check without
accessing a database:

```sh
python3 bundles/sirix-query/bench/clickbench/rig/verify-java-rig-lock.py \
  --runtime PREPARED/frozen/runtime.json --out FRESH_LOCAL_TEST_DIRECTORY
```

`poweraudit.py TELEMETRY.jsonl...` reports, per retained telemetry file, the MSR and MMIO power
limits observed and how many samples ran under an MMIO limit below the MSR cap; see the
[power audit](evidence/power-audit-20260908/README.md) for what it found in the historical cohorts.
`suite100m.sh 3` collects one complete steering leg without an effect-size claim.
`diag100m.sh QUERIES [JVMFLAGS]` profiles selected queries with `TRIES` repetitions and projection
diagnostics. Both freeze the current worktree, acquire the same leases, and write fresh logs under
this worktree's `bundles/sirix-query/build/diagnostics/rig-runs/`. Set `CB_RUN_OUT` to choose
another fresh output directory. Advanced profiling uses `measure.py run --diagnostic --runtime MANIFEST`
with repeated `--diagnostic-arg=-XX:...` arguments, an explicit `--out`, `--queries`, and `--tries`.

The host lease is `/tmp/sirix-clickbench-<uid>.lock`, stable across worktrees. With `CB_RIG_WORK`
set, the launcher also holds its legacy `leg.lock`. Kernel flock ownership is authoritative;
an empty or stale file does not block a run. Never unlink lock files. The launcher passes open
descriptors directly to Java, which verifies the inode and kernel lock and retains ownership
until process exit. Raw Java/Gradle benchmark entry points also acquire a process-lifetime
lease. Large query JVMs require the full campaign envelope; smaller validation JVMs use shared
host ownership and cannot overlap an exclusive 100M process. Losing a wrapper does not release
the lease while its JVM survives. Legacy executables must use `rig_lock.py -- COMMAND...`.

The 100M database and both source corpora are irreplaceable and read-only for this campaign.
Never run `load100m.sh`, rebuild projection indexes, shrink the query envelope, or delete shared
diagnostics to make a run fit. `load1m.sh` and `seggate1m.sh` remain the correctness-validation
entry points when a small-data window is authorized. `mkleg.py` is a historical log conversion
utility; it is not the new measurement command and carries no uncertainty or protocol proof. It
converts a log into a `steering` or `unknown` leg and never into a publication one, so `rank.py`
refuses everything it writes; designating a leg publication is a deliberate curation of `legs/`.
Five raw evidence archives were discarded from `evidence/`; what was dropped, what can no longer be
replayed, and what cannot be recaptured are recorded in [`evidence/RETENTION.md`](evidence/RETENTION.md).
Campaign context is in
[`HANDOFF_SEGMENT_LANE_2026-09-06.md`](../../../../../docs/HANDOFF_SEGMENT_LANE_2026-09-06.md).
