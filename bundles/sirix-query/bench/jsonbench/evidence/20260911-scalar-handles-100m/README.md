# Specialize foreign-memory scalar reads for native images

This is a validated intermediate improvement, **not rank one**. Sirix's paired
100M hot score improves by 31–36%, but remains 22.8–23.8 times slower than the
authorized local ClickHouse leader-engine comparator. All five canonical queries
match exactly. The campaign must continue.

## Workload and ranking

The target is [ClickHouse/JSONBench at revision
e6c7c98dc766394d51f7d506a3dd2b5d51165d70](https://github.com/ClickHouse/JSONBench/tree/e6c7c98dc766394d51f7d506a3dd2b5d51165d70),
the Bluesky workload, not ClickBench's web hits. Upstream provides a 100M tier;
the dashboard's actual default is 1B/hot with retained structure. At the pinned
revision, the 100M hot leader is ClickHouse 25.11, score 1.0229245566. The cold
leader is StarRocks 4.0.1. This comparison uses captain-authorized ClickHouse
26.7.3.19 for an informal local comparison; it is neither a version-identical
reproduction of the published entry nor a cold-leader comparison.

The five queries are collection counts; collection counts and exact distinct
DIDs for creates; hourly post/repost/like create counts; the three earliest post
users; and the three users with longest post activity spans. Their exact SQL
matches `../../queries.sql`.

Upstream ranks the geometric mean of `(seconds + .010)/(fastest seconds + .010)`
over all five queries. The paired Sirix/ClickHouse score ratio is therefore
`geomean((Sirix seconds + .010)/(ClickHouse seconds + .010))`: the common fastest
denominators cancel. Each engine has three attempts per query. Attempt 1 is cold;
hot is the minimum of attempts 2–3. Lower is better, and a ratio below one is
required to beat this comparator.

## Completed fresh import

The repaired source `45769b0feee0df55d9358a11805bccbea2dc41d3` completed the
previously pending fresh 100M import. An independent read-only reopen of revision
1 reports **99,999,968** persisted root-array children, matching the cleaned
input exactly. This check reads structural metadata without traversing rows or
creating query-serving state. Both engines subsequently match all five queries.

Sirix import took 9622.525 seconds and reported 48,738,272,656 data bytes. The
24 GiB scope peaked at 19,482,038,272 bytes with no OOM or swap event. Recorded
ordinary record-page residency stayed at 16–20, discarded encodes stayed zero,
and the guard accepted 16,366 samples, at most 64 C and a 0.640-second maximum
sampling gap. This completes full-tier validation of the preceding carrier
retention repair; it does not make ingestion time part of the ranked score.
ClickHouse's separate setup took 2415.132 seconds and verified the same row count.

The 100 source gzip files have retained download checksums. Cleaning rejected
32 lines, leaving 47,811,781,297 logical bytes, SHA-256
`f063d7db1d71009122ad4e99feb956423f52df3aa3f1d090a602cd44e121e538`.
Both imports consumed the same cleaned data. Its round-trip-verified shared gzip
has SHA-256 `925e84df83435fb1dcb621ad89e9b48f859c8b7c0cbeedf084605bd3ff48312a`.
The old failed store was deleted as directed before this checkpoint; its metadata
and file hashes remain, but the deleted physical data file cannot be restored
from metadata alone. The successful fresh store, source inputs, ClickHouse store,
references and failed-attempt evidence remain retained locally.

## Profile and generic change

Fresh GraalVM 100M PGO builds preceded measurement. The baseline is the repaired
source above, not a pooled worktree artifact or the older JVM gate. Actual-binary
diagnostics put catalog opening at 9.9–10.6 seconds: about three seconds reading
metadata and physical order, then 6.7–7.5 seconds walking row-group directories.
The `metaParse` diagnostic includes physical-order blob reads; it is not a
measurement of parsing alone.

A symbolized companion, built from the same source and profile, reproduced all
five exact results. Q1 and Q4 CPU profiles attribute approximately 44.7% and
49.9% of weighted sampled cycles to method-handle conversion/reflection helpers.
HOT key and value reads are prominent callers. Raw profiles, reports and the
explicit symbol-grouping calculation are retained.

`io.sirix.node.SegmentAccess` provides constant, typed, unaligned foreign-memory
read handles with explicit little- and big-endian wire orders. `HOTLeafPage` and
`MemorySegmentBytesIn` use these access sites so native-image can specialize
them. Native-image metadata initializes only the immutable layout/handle constants
at build time. No segment, arena, resource, configuration, row, dictionary or
query result is captured. Bounds, null, scope and thread checks remain those of
the foreign-memory API; HOT optimistic-stamp validation remains unchanged.

This adds no global prepass, answer cache, benchmark predicate, storage-format
change or benchmark counter to a production hot path. The same freshly imported
database serves the before and after reader builds.

## Validation

The frozen candidate passed **86 tests in 11 classes, zero skips**. New scalar
tests cover heap/native/read-only segments, unaligned offsets, both byte orders,
floating-point bit patterns, boundary and overflowing reads, closed/confined
arenas and failed input-cursor advancement. Existing HOT wire, format, eviction,
frame ownership, dictionary retention and all five serving-shape tests also pass.
The helper's bytecode is identical to the version already checked in a standalone
native executable for scalar values, bounds, null, lifetime and thread behavior
on this GraalVM release. The equivalence and original guarded verdict are retained.

The instrumented 1M steering gate first checked all five answers for both builds,
then ran 60 paired attempts with 20 verified cold blocks. Routes were identical
between builds for every query. Its after/before hot ratios were 0.4297496371
and 0.4468992217. These are explicitly unranked instrumented observations.
They were not used as a substitute for 100M results or as the 100M PGO profile.

Fresh 100M training then executed all five queries twice, matched every answer,
and produced the new profile
`6f3652c89392f517e046c6b9ab4362695404aec328e1c4e06c5426d5a1ea43b7`.
Its counters were ten aggregate/sliced executions, four numeric/dense executions,
and zero other serving routes. Only that full-tier profile built the measured
optimized candidate.

## Paired canonical result

Both complete comparisons use `jsonbench-isolated-v1`: one fresh process per
attempt, targeted file eviction verified by `mincore` before the first attempt,
warm OS cache for subsequent attempts, and two per-query AB/BA rounds. The guard
requires package temperature below 55 C before each engine block. Internal engine
query seconds determine rank; separate GNU `time -v` output retains process wall,
memory and I/O. Import, correctness, training and diagnostic time are excluded.
Catalog opening is inside Sirix's ranked query window.

| Round | Baseline cold ratio | Candidate cold ratio | Baseline hot ratio | Candidate hot ratio | Hot after/before |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 39.362215 | 24.189669 | 35.720963 | 22.814849 | 0.638696 |
| 2 | 35.800918 | 22.636173 | 34.305077 | 23.757488 | 0.692536 |

Each ratio compares Sirix with the ClickHouse measurements paired in that run.
The candidate's hot query sums were 46.121 and 46.677 seconds; ClickHouse's were
2.909 and 2.952 seconds. Sums are descriptive only, not the ranking calculation.
Both rounds still lose every individual query. No rank-one claim is warranted.

The final audit recomputes both scores from all 60 unique attempts, verifies five
exact differential files, all 30 Sirix route contracts against the baseline,
all 20 cold-cache blocks and the frozen binary hash. The candidate guard accepted
844 samples, maximum 71 C and maximum sampling gap 0.751 seconds, with no failure.

Every load-bearing phase ran under continuous fail-closed sampling: balanced
profile, every EPP `balance_power`, unchanged throttle counters, temperature below
90 C, nominal 0.5-second sampling and a two-second maximum gap. Private process
scopes cap memory at 24 GiB with zero swap. Invalid telemetry, low available memory
or disk, or policy changes fail the phase and stop only its process tree. No shared
service, desktop process or unrelated user process was stopped or signalled.

Post-measurement diagnostics still match exactly. They place directory walking
at 2.7–2.8 seconds, metadata/physical-order reads near three seconds, and catalog
opening near six seconds. They also report `NATIVE_DECODER_ENABLED=false`.
The next investigation is the remaining catalog/decompression cost and native
decoder activation, guided by a fresh symbolized profile.

## Reproduction and retained evidence

The candidate is a fresh `git archive` of the baseline plus exactly five hashed
files. Its source archive SHA-256 is
`a621963582e48ca5c2026722966c444184ec0844c80aa965d3100b2160c2f964`.
Builds ran offline with no build-cache reuse, tasks rerun, a private Gradle home,
and the captain-selected GraalVM `jdk-25i4-25.0.4.1.1-ea.01`, source
`cb905c0ea0e868072ee525107e468ac2b5ff964d`. This lane downloaded no new tool or data.
Runtime manifests freeze JDK hashes, native arguments and every retained JAR.
Measured executable SHA-256:
`28cb196a67a5a1f83719546fbd8bbc4acbe110c3a26ff72b8e753bff7a5a7e69`.

`raw-attempts.tar.gz` contains 1,356 files: before/after attempts, commands, dumps,
exact comparisons, tests, telemetry, source/runtime/input manifests, profiles'
text reports and the drivers. Its SHA-256 is
`a10ec69d2df6de2b83d89ba76071dcfd6e634460efe4bf23451c7be5d647b972`.
Large perf/JFR/steering-profile files remain at task-local paths;
executables, full-tier profiles, runtime JARs and the source archive are described
by their retained manifests. They are not embedded in this Git archive.

The archived `scalar-lane.sh`, `advance-scalar-100m.py` and `check-scalar-1m.py`
record the exact build, test, steering, full-tier training and paired sequence.
`paired-retention-100m.py` is shared by the before and after measurements;
`audit-scalar-paired-100m.py` independently validates the latter. Absolute paths
identify this laptop attempt; adjust the task root when reproducing elsewhere.
Re-run the full instrument/collect/optimize cycle after any hot-code change.

The production change and evidence are ready for continued campaign work. The
required no-mistakes shipping pipeline and validated PR remain outstanding;
main has not been pushed to or merged.
