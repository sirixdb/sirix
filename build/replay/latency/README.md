# Identity replay matched latency acceptance

The harness reproduces the design report's 4096-element array, 4096-element skipped-root
append, then no-op commit with FILE_CHANNEL, ROLLING hashes, Dewey IDs disabled and
SLIDING_SNAPSHOT versioning. Both variants use the same harness and immutable compiled
class snapshots. A baseline without the identity delta classes measures its unpruned
replay diff; the candidate measures authoritative delta construction. Complete history
copy always invokes the public JsonResourceCopy API. Every copied revision is checked
against source JSON outside the measured interval; the separate identity oracle covers
keys, links, hashes, metadata, indexes and path summaries.

Measured operations are source append+commit, compact public diff, materialized public
diff, replay input construction, unchanged public diff and complete history copy.
Extended scenarios cover a 32768-element unchanged prefix, nesting depth 64, a reserved
frontier of one trillion, equal-value subtree moves and deletion/restoration by revert.
The latter two name their source operation `sourceMutationCommit`; restore's last diff
is named `restorationPublicDiff`, because it is not a no-op. Baseline failures are retained
as UNSUPPORTED lines and are never converted into successful timings or speedups.
Baseline af9f20e5a hangs during source insertion after the trillion-key reservation;
the smoke log retains its thread dump and intentional termination. Its already repaired
trie-growth defect prevents a sparse paired comparison. Sparse is measured separately
on the candidate, with no baseline timing or speedup claim.

`export.init.gradle` compiles the standalone harness and snapshots main classes/resources
plus its runtime classpath. Use it through `build/replay/run.sh`, setting
`-Dsirix.replay.bench.variant=baseline` or `candidate`, with
`-I build/replay/latency/export.init.gradle :sirix-core:exportReplayLatency`.
The baseline source is restored only inside this disposable worktree after archiving test
results; the committed candidate is restored immediately after exporting the baseline.
No sibling checkout or worktree is used. Validate absence/presence of identity classes in
the respective exported artifact before starting any measurement.
The export manifest records SHA-256 hashes of compiled classes, harness, resources and
external dependencies; baseline and candidate dependency paths and hashes must match.

`campaign.sh OUTPUT SCENARIOS PAIRS WARMUPS SAMPLES DIAGNOSTICS` runs sequential pinned
forks, alternating baseline/candidate and candidate/baseline. Every fork acquires the
required MemAvailable >=6 GiB/two-slot limiter, then rechecks the deadline before starting.
JVMs use the same exported Java executable, assertions, 512 MiB initial / 3 GiB maximum
heap (matching the design report), exported core-test JVM options,
vector/module flags and CPU set 0–11. No collector flag is forced: core's task replaces
the root project's initial JVM options. The admission cutoff is 03:30 Berlin and each
fork times out at five minutes, so no admitted fork crosses the 03:40 validation cutoff.
No machine power settings or other lanes' processes are touched.

Timed campaigns disable replay and HOT diagnostics. Separate diagnostic forks expose
record visits, created/staged identities, memoized ancestry hops, sidecar reads, page
resolutions, path-summary cursor work and diff bookkeeping. An unavailable/disabled
counter is -1, never zero. ThreadMXBean allocation deltas measure the calling thread only;
they exclude allocations in background threads and are not process-wide heap measurements.

`bootstrap.py OUTPUT` retains within-fork correlation by resampling matched fork pairs,
5000 fixed-seed draws, and reports the 2.5th/97.5th percentiles of the candidate/baseline
pooled-median ratio. At least six matched forks are required for each reported metric.
A lower bound above 1.05 is a confirmed regression. Raw data and unmatched/unsupported
cases must be inspected alongside the report; missing baseline cases cannot pass a
comparative latency gate. No wall-clock assertions are added to correctness tests.
The reader rejects incomplete runs, missing candidate metrics, unmatched forks and
mixed successful/unsupported comparisons instead of silently dropping samples.
Only trailing spaces in two Gradle logs and the baseline smoke thread dump are normalized
for repository whitespace checks; every SAMPLE line is unchanged. Unnormalized originals
remain locally under raw-originals/ and are not part of the source commit.

The report's 89–111 ms copy medians are historical context. Fresh pinned matched baseline
and candidate medians establish this change's comparison on the shared laptop. A passing
paired interval is not a production p99 or exclusive-CPU guarantee.
