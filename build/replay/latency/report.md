# Identity replay latency evidence

Candidate production source: `c10f64aae7bdd8de505eb8a213a8045a73c2b501`.
Baseline main with the tombstone/restore prerequisite: `af9f20e5a8d917066c143773ca18c08f0118ca2f`.
The candidate includes the independently tested source hash/count, sparse-trie and CAS
repairs as well as identity replay. These measurements compare the whole branch and do
not isolate each commit's contribution to source or public-diff costs.

## Current acceptance status

The primary append campaign is complete. No primary metric has a 95% paired-bootstrap
lower ratio bound above 1.05, the specified confirmed-regression rule. Copy is 34.16 ms
versus a fresh 93.66 ms baseline, within the design report's historical 89–111 ms baseline
range. Replay input construction is 2.93 ms versus 30.25 ms.

Source append's point estimate is **7.5% higher**, with a **+0.7% to +12.8%** interval.
This does not establish unchanged source latency or exclude a slowdown above 5%; it
falls short of the specified confirmed-regression threshold. Public compact/materialized
diff intervals also extend slightly above 5%. These uncertainties are retained, not
converted into claims of source-side speedup or equivalence.

All 24 extended paired metrics also have lower bounds below 1.05, but their intervals
are wide. The 32768-element unchanged-prefix workload has a copy point estimate of
133.22 ms versus 116.42 ms (ratio interval 0.887–1.376), and public compact diff is
39.01 ms versus 31.43 ms (interval 1.044–1.614). These results do not exclude material
slowdowns; absence of a confirmed regression is not equivalence. Six candidate-only
sparse forks are complete. Final combined full suites and formatting passed: 13912 core
tests (77 skipped) and 2573 query tests (seven skipped), zero failures/errors. The last
three normal historical replay modes each passed 150/150, completing normal/forced
coverage for all four versioning types. This is the committed implementation handoff;
Firstmate's no-mistakes validation/push stage has not started, and merge is not authorized.

## Primary matched results

Twelve alternating AB/BA fork pairs; 24 untimed warmups and 17 measured fresh-database
iterations per fork (204 samples per variant and metric). Diagnostics disabled. Ratios
are candidate/baseline pooled medians. The fixed-seed paired fork-block bootstrap uses
5000 draws, preserving correlation among measurements from the same JVM. Every sample
is retained, with no outlier trimming or optional stopping.

| Operation | Baseline ms | Candidate ms | Ratio | 95% paired interval |
|---|---:|---:|---:|---:|
| bulkAppendCommit | 47.0312 | 50.5795 | 1.0754 | 1.0072–1.1282 |
| publicCompactDiff | 31.9049 | 32.3471 | 1.0139 | 0.9833–1.0504 |
| publicMaterializedDiff | 30.5226 | 31.1780 | 1.0215 | 0.9901–1.0535 |
| replayRead | 30.2546 | 2.9293 | 0.0968 | 0.0936–0.1035 |
| revisionCopy | 93.6554 | 34.1586 | 0.3647 | 0.3339–0.3836 |
| unchangedPublicDiff | 0.2686 | 0.2540 | 0.9460 | 0.8754–1.0357 |

Current-thread allocated bytes are collected around the same operations. They exclude
background-thread allocations and are not total heap/RSS measurements.

| Operation | Baseline MiB | Candidate MiB |
|---|---:|---:|
| bulkAppendCommit | 35.057 | 37.538 |
| publicCompactDiff | 34.133 | 35.552 |
| publicMaterializedDiff | 33.953 | 35.372 |
| replayRead | 33.758 | 2.168 |
| revisionCopy | 46.711 | 13.961 |
| unchangedPublicDiff | 0.016 | 0.016 |

## Environment and scope

Both artifacts use Java 25.0.3 GraalVM, 512 MiB initial / 3 GiB maximum heap, identical
exported core-test JVM options and external dependency SHA-256 hashes, and CPUs 0–11.
No collector flag is forced. The core test task replaces the root project's initial
JVM options; the earlier preparation note assuming ZGC was corrected before acceptance.
The fork logs retain actual JVM arguments. The laptop is shared; its powersave governor
was only read, never changed. Machine details are in `machine.json`.

Every JVM passes the required MemAvailable >=6 GiB/two-slot flock limiter. ADMITTED
lines mark slot acquisition; campaign queued times include waiting. Every fork has a
five-minute cap, with admission checked against the captain's cutoff. This is a matched
median comparison, not an exclusive-CPU experiment or a production p99/SLA claim.

Source uses FILE_CHANNEL, ROLLING hashes, Dewey IDs off, SLIDING_SNAPSHOT versioning,
default path summary and diff storage, without extra secondary indexes. The primary
fixture starts with 4096 primitive array values, appends 4096 skipped-root values and
commits, then commits an unchanged revision. Copy includes all three revisions and
excludes target open/close. Source seeding, JSON verification of every copied revision,
reflection/counter setup and diagnostic reads are outside the timed sections. Public
compact/materialized diff, replay input construction and no-op public diff are measured
separately. The separate identity oracle validates more than JSON.

Compiled classes/resources and the harness are snapshotted per variant. Exported
classpaths contain no live module build output. Manifests retain source commits, Java,
JVM options, dependency hashes and hashes of every exported class/resource. Candidate
source was restored immediately after baseline export. No second worktree was created.

## Sparse baseline limitation

The baseline's trillion-key source append did not complete. Its preserved smoke thread
dump shows the main thread consuming CPU in prepareRecordPage/addParentHash during
insertSubtreeInternal. Only this lane's verified JVM was terminated; the incomplete
smoke fork and exit 143 are excluded from acceptance. The branch's independent sparse
trie growth fix (`1a6088b67`) already has 24 direct regression configurations, and the
candidate completed all six smoke scenarios, including sparse allocation and copy.

Sparse candidate results below have no baseline time, ratio or speedup.
The other extended scenarios are paired normally. No unsupported operation is assigned
a zero time or included as a successful sample.

## Work evidence and reproducibility

`smoke/work-summary.json` contains repeated diagnostic minima/maxima for record visits,
created/staged identities, ancestor steps, sidecar reads, fallback page resolutions,
path-summary work and diff bookkeeping. All 36 scenario/operation rows have stable
counts across the two smoke samples. Diagnostic timings are not acceptance timings.
For primary full history copy, including its initial snapshot: 87195 record visits,
8193 created identities, 8197 staged records, 4098 ancestor steps, zero sidecar reads,
27 fallback pages and 19 path steps. The separate work-budget suite guards incremental
append/no-op/sparse behavior and retains every existing bound.

Commands are in `README.md`, `export-pair.sh`, `campaign.sh`, `candidate-only.sh`,
`bootstrap.py`, `candidate-summary.py` and `work-summary.py`. Raw primary forks and
`primary/result.json` are retained. All result scripts reject incomplete runs rather
than silently dropping them. Use an explicitly authorized admission window when
repeating the campaign; the checked-in window records this run's stop constraint.

## Extended paired results

Six alternating matched pairs, eight warmups and nine measured iterations per scenario
and fork (54 samples per variant/metric). Scenario order is unchanged-prefix, deep,
equal-value moves, deleted-key restore. Raw runs are under `extended/`; result intervals
use the same 5000-draw fork-block bootstrap. No metric's lower bound exceeds 1.05.

| Scenario | Operation | Baseline ms | Candidate ms | Ratio | 95% paired interval |
|---|---|---:|---:|---:|---:|
| deep | bulkAppendCommit | 4.8916 | 5.5023 | 1.1249 | 0.9038–1.4233 |
| deep | publicCompactDiff | 0.7903 | 0.9504 | 1.2026 | 0.7555–1.6992 |
| deep | publicMaterializedDiff | 0.6313 | 0.7929 | 1.2560 | 0.7624–1.8762 |
| deep | replayRead | 0.8372 | 0.3677 | 0.4392 | 0.3510–0.7751 |
| deep | revisionCopy | 14.1501 | 12.1609 | 0.8594 | 0.7302–1.1857 |
| deep | unchangedPublicDiff | 0.1994 | 0.1496 | 0.7505 | 0.6097–1.4922 |
| moves | publicCompactDiff | 0.5077 | 0.4846 | 0.9546 | 0.7729–1.2449 |
| moves | publicMaterializedDiff | 0.5074 | 0.5138 | 1.0127 | 0.8568–1.1862 |
| moves | replayRead | 0.5859 | 0.1589 | 0.2711 | 0.2274–0.3397 |
| moves | revisionCopy | 10.1177 | 8.8870 | 0.8784 | 0.7778–1.0802 |
| moves | sourceMutationCommit | 3.6119 | 3.6678 | 1.0155 | 0.8424–1.2114 |
| moves | unchangedPublicDiff | 0.1497 | 0.1415 | 0.9453 | 0.8130–1.0699 |
| restore | publicCompactDiff | 0.3374 | 0.3734 | 1.1065 | 0.9742–1.3179 |
| restore | publicMaterializedDiff | 0.1805 | 0.2100 | 1.1636 | 0.9249–1.3056 |
| restore | replayRead | 0.1978 | 0.2035 | 1.0288 | 0.7739–1.1859 |
| restore | restorationPublicDiff | 0.2766 | 0.3221 | 1.1646 | 0.9612–1.4690 |
| restore | revisionCopy | 11.6601 | 9.5353 | 0.8178 | 0.7568–0.9597 |
| restore | sourceMutationCommit | 3.3944 | 3.5223 | 1.0377 | 0.9396–1.1262 |
| unchanged | bulkAppendCommit | 6.3693 | 7.4244 | 1.1657 | 0.9485–1.3396 |
| unchanged | publicCompactDiff | 31.4344 | 39.0062 | 1.2409 | 1.0443–1.6144 |
| unchanged | publicMaterializedDiff | 13.2701 | 15.0687 | 1.1355 | 0.8885–1.7380 |
| unchanged | replayRead | 10.9412 | 2.3327 | 0.2132 | 0.1474–0.2871 |
| unchanged | revisionCopy | 116.4249 | 133.2245 | 1.1443 | 0.8872–1.3757 |
| unchanged | unchangedPublicDiff | 0.3403 | 0.4133 | 1.2144 | 0.8799–1.6112 |

## Candidate-only sparse results

Six independent candidate forks, eight warmups and nine measured iterations per fork
(54 samples per metric), with diagnostics disabled. Baseline source does not complete;
there is deliberately no comparative interval or regression/speedup verdict.

| Operation | Candidate median ms | Current-thread allocated MiB |
|---|---:|---:|
| bulkAppendCommit | 8.7734 | 1.270 |
| publicCompactDiff | 1.2902 | 0.056 |
| publicMaterializedDiff | 1.0691 | 0.056 |
| replayRead | 0.6709 | 0.027 |
| revisionCopy | 20.9534 | 3.732 |
| unchangedPublicDiff | 0.4317 | 0.016 |
