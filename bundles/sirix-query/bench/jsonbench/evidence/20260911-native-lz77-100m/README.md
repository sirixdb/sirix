# Enable the existing LZ77 native decoder in native images

This is a validated intermediate improvement, **not rank one**. The paired 100M
hot score improves 53–55% from the preceding scalar-handle build, but Sirix still
loses by 10.67–10.82 times. All five canonical answers match exactly.

The production change registers the existing decoder's shared-library resource
and critical foreign downcall, including heap access. Before this change the
native image returned null when extracting its unregistered library and silently
used Java decompression. No codec algorithm, storage format, query predicate,
import-built data, global prepass or result cache changes. The existing fallback
remains available on unsupported platforms.

## Workload and comparator

Upstream [ClickHouse/JSONBench revision
e6c7c98dc766394d51f7d506a3dd2b5d51165d70](https://github.com/ClickHouse/JSONBench/tree/e6c7c98dc766394d51f7d506a3dd2b5d51165d70)
provides the 100M Bluesky tier; its dashboard default is actually 1B/hot with
retained structure. The 100M hot leader is ClickHouse 25.11, score 1.0229245566.
The cold leader is StarRocks 4.0.1. As authorized, this campaign uses local
ClickHouse 26.7.3.19 for the informal leader-engine comparison. It is not a
version-identical reproduction of the published entry or a cold-leader comparison.
The upstream revision was rechecked unchanged before this paired result; one
transient TLS timeout and its successful retry are retained.

The five queries are collection counts; create counts and exact distinct DIDs;
hourly post/repost/like create counts; earliest three post users; longest three
post-activity spans. Exact SQL, result schema and upstream file hashes are in
`upstream-pin.json`, matching the benchmark kit. Ranking uses the geometric mean
of `(seconds + .010)/(fastest seconds + .010)`. Thus the paired score ratio is
`geomean((Sirix seconds + .010)/(ClickHouse seconds + .010))`; lower is better.

Both engines use the same cleaned **99,999,968-row** corpus (32 rejected lines),
logical SHA-256 `f063d7db1d71009122ad4e99feb956423f52df3aa3f1d090a602cd44e121e538`.
Its retained gzip is `925e84df83435fb1dcb621ad89e9b48f859c8b7c0cbeedf084605bd3ff48312a`.
The existing successful databases were reused. Sirix import took 9622.525 seconds
and reported 48,738,272,656 data bytes; ClickHouse setup took 2415.132 seconds and
its whole-directory allocated measurement was 29,010,276,352 bytes. Setup is
excluded from ranking. Import and original download manifests remain retained.

## Validation and paired result

The fresh candidate passed **43 tests in eight classes, zero skips/failures/errors**.
The new native smoke entry point checks actual native dispatch for heap and
native destinations, offsets, slack and canaries; it fails rather than silently
skipping when the native image lacks the decoder. A controlled pair of smoke
images used the same 47 classpath components, changing only the core metadata:
the old metadata failed availability; the new metadata passed dispatch and
existing decoder contract tests. Native extraction was also examined in an owned
debug process before the fix. Raw control evidence and exact hashes are archived.

An instrumented 1M gate checked five answers for each build, 60 paired attempts,
unchanged routes and 20 cold blocks. Its hot after/before ratios were 0.654798 and
0.643116; these are unranked steering results. Fresh **100M** PGO training then
ran every query twice with exact answers and the expected ten aggregate/sliced,
four numeric/dense executions. Only that full-tier profile built the measured image.

The final comparison uses `jsonbench-isolated-v1`, two per-query AB/BA rounds and
three attempts per engine/query. Attempt one has targeted database file eviction
verified by mincore; hot is min(attempts two and three), both fresh processes with
warm OS cache. Catalog opening remains inside Sirix's query timer. GNU time
separately records whole-process wall, RSS and I/O; those are not ranking inputs.

| Round | Before cold | After cold | Before hot | After hot | Hot after/before |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 24.189669 | 13.248295 | 22.814849 | 10.818661 | 0.474194 |
| 2 | 22.636173 | 11.368116 | 23.757488 | 10.669560 | 0.449103 |

Each ratio uses that run's paired ClickHouse measurements. The audit checks all
60 unique attempts, five exact differential results, 30 unchanged route contracts,
20 verified cold blocks, source/profile/binary hashes and recomputed scores.
Every individual query still loses. Hot query sums are 22.191/22.207 seconds for
Sirix and 3.110/3.085 for ClickHouse; sums do not define rank.

All build, test, training, correctness and timed phases ran under continuous
fail-closed guards: balanced profile, every EPP balance_power, unchanged throttle
counters, temperature below 90 C, nominal 0.5-second sampling with at most a
two-second gap. Engine blocks start below 55 C. Private scopes limit memory to
24 GiB with zero swap and stop only owned process trees on failure. The paired
guard passed 599 samples, maximum 72 C and maximum gap 0.772 seconds. No shared
service, desktop or unrelated process was signalled.

## Profiles and next investigation

Actual optimized diagnostics report native decoder enabled. Q1 metadata/physical
order work falls from about three seconds to 0.126 seconds, while directory
walking remains 2.900 seconds (Q4 2.933 seconds). Matching symbolized perf captures
and async-profiler CPU captures identify directory capture as a major cost.

Six native CPU/wall/native-memory captures and four JVM allocation/monitor controls
passed exact answers and guards. The JVM controls are labeled; native malloc
samples do not measure Java heap allocations, and all-thread wall samples include
idle GC threads. Native async-profiler 4.2 unwinding often stops at Graal Java
frames. Failed probes and the deliberately bounded optional perf source-line
render are retained separately; neither is accepted timing. See
[the focused ClickBench comparison and index inventory](clickbench-jsonbench-contrast.md)
for measured causes, disconfirming evidence, ClickHouse's actual five query plans,
profiling limitations and the next traversal experiment.

## Reproduction and evidence

The source is a clean archive of `647255b8df445e4e03af191ec046cc195ff931b9`
plus exactly two hashed files in `native-lz77-v1-source.json`. Source archive
SHA-256 is `d970e737084f593c3fe1c39cb9782c6dcc263815e22b9e704f0bb763999aacf1`.
Fresh offline Gradle builds reran tasks without build-cache reuse in the private
campaign home. GraalVM is `jdk-25i4-25.0.4.1.1-ea.01`, source
`cb905c0ea0e868072ee525107e468ac2b5ff964d`. This lane downloaded no tools or data;
existing async-profiler 4.2 was copied into the worktree and checksum verified.

Measured binary SHA-256:
`888cc7fec35ef84c145dacca1dd4e9c43059ede314ef9dfb1b9d0c1f134a6ccb`.
Full-tier profile SHA-256:
`f544038c6f49c1a8f2527754a768297a001d38313d247d8c66647dff215fab78`.
Runtime manifests retain the JDK, all JARs and exact native-image arguments.
The symbolized companion has its own matching build-ID/debug-file manifest.

`raw-attempts.tar.gz` contains **1,615 files**, 5,274,503 bytes, SHA-256
`74c5c51121101a8c783289a3abdd478561f87f0e4a8ca3cee5cb3d229c0dff42`.
Every archive member was round-trip checked against `raw-files.json`. The 91
larger binaries/profiles are retained locally with paths, lengths and checksums
in `retained-large-files.json`; these are not embedded in Git. Archived drivers
include `lz77-lane.sh`, `lz77-native-smoke.py`, `advance-lz77-100m.py`,
`paired-retention-100m.py`, `audit-lz77-paired-100m.py`, profiler and inspection
helpers, and the guard. Adjust the recorded task-root paths when reproducing.
Repeat instrument/train/optimize after changing hot code; never reuse pooled output.

This commit preserves the best validated generic change for continued work. The
required no-mistakes shipping pipeline and validated PR remain outstanding.
No main push or merge has occurred.
