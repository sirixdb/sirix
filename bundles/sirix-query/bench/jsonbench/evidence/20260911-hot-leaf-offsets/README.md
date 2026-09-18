# Single-leaf offset decoding: 1M steering evidence

This is an intermediate JSONBench result, **not a 100M win**. The change replaces scalar reads of
`entryCount` slot offsets in `PageKind.HOT_LEAF_PAGE.deserializePage` with the existing
`BytesIn.readInts` operation. It reads the same offset-table bytes into the same `MAX_ENTRIES` array.
It does not walk, decode, or prefetch another leaf, change the disk format, or precompute query answers.

## Upstream workload and target

Pinned ClickHouse/JSONBench revision: `e6c7c98dc766394d51f7d506a3dd2b5d51165d70`.
The dashboard offers the requested 100M tier; its current default is actually 1B, hot, retaining
structure. At 100M, the hot leader is ClickHouse 25.11 (score 1.0229245566). Cold has a different
leader, StarRocks 4.0.1. The captain authorized ClickHouse **26.7.3.19** for an informal local
comparison: “no, the new version is ok, we just want an informal check”. This is not a submission
claim or a version-identical reproduction of the published ClickHouse result.

The five canonical SQL queries are byte-identical to `../../queries.sql`: collection counts;
collection counts and exact distinct users for creates; hourly counts for three collections;
three users with the earliest post; and three users with the longest posting span. See
[the pinned dashboard](https://github.com/ClickHouse/JSONBench/blob/e6c7c98dc766394d51f7d506a3dd2b5d51165d70/index.html)
for queries and rankings.

Ranking is the geometric mean of `(time + 0.010)/(fastest time + 0.010)` across all five queries.
Cold is attempt 1; upstream hot is the minimum of attempts 2–3. This local 1M steering gate uses
`jsonbench-isolated-v1`: one cold plus three hot fresh processes per query, with the hot minimum.
The final 100M gate must use the upstream three-attempt definition. Setup/load time is excluded.

## Build and validation

Base source is clean main `df9b763fbc5e5387a3cd01715975cc28b3aeba19`. Each build used an extracted
`git archive`, private Gradle caches, offline resolution, `--no-build-cache --rerun-tasks`,
GraalVM 25.0.3, O3 and native CPU code generation. Each variant has a separately built instrumented
image, its own 1M training profile, and a fresh optimized image. Compiler builds used six processors
and a 12 GiB heap cap. No pooled worktree binaries were accepted. Runtime manifests retain the exact
classpath, binary/profile hashes and compiler arguments. The sparse test was added after image
construction; the production source hash is identical in both candidate source manifests.

All 31 selected tests passed: `HOTLeafPageNativeWireTest` (8), `HOTLeafPageFrameOwnershipTest` (4),
`GoldenFormatTest` (14), and `JsonBenchShapeServingTest` (5). New coverage reads legacy wire data at
all eight byte alignments, handles empty and full leaves, rejects a truncated offset table, and
reads a sparse one-entry fragment without consuming the following leaf.

Both engines use the same cleaned 1M corpus and immutable Sirix database from the trusted local
validation gate. Each baseline/candidate query separately matched the ClickHouse UTC reference
before timing. All 80 timed attempts retained identical per-query projection/route counters.
The before/after order reverses per query in round 2. Each cold attempt starts after targeted
`fsync`/`fadvise` eviction, with `mincore` proving zero resident database-file pages. Hot attempts
use new processes and warm OS file caches. There is no global cache drop or result reuse.

Every build, training, test and benchmark phase ran beneath the continuous fail-closed guard:
0.5 s sampling, maximum 2 s gap, stop at 90 C, invalid telemetry, EPP/profile change or increased
throttle counters. Balanced and `balance_power` remained in force. The timed guard passed all
72 samples. Only this campaign's download groups were paused during timing (43.52 s), then resumed.

## Paired result

Lower after/before is better; this uses the same 10 ms offset as upstream.

| Round | Cold after/before | Hot after/before | Hot reduction |
| --- | ---: | ---: | ---: |
| 1 | 0.981514 | 0.894726 | 10.53% |
| 2 | 0.866109 | 0.902655 | 9.73% |

Cold has visible variance; this does not establish a precise cold speedup. Before this change,
the fresh native Sirix/ClickHouse 1M hot score ratio was 5.19 in both paired rounds. Native PGO
therefore improves the older JVM gate substantially, but neither that baseline nor this small
storage improvement proves rank one. Remaining work includes the separately pinned EA compiler
arm (profiles expose scalar MemorySegment method-handle overhead) and the paired canonical 100M run.

## Memory and I/O scope

Cold major-page-fault counts matched exactly: 151 for Q1–Q3 and 156 for Q4–Q5; hot counts were zero.
GNU time file-input counters were zero for both variants on this filesystem, so they do not establish
physical-device reads. The change has identical logical input bounds and no added page-reader call.
The sparse test and exact final input positions cover those bounds.

Median peak RSS across attempts fell from 481,120 to 378,320 KiB, driven by Q2. Per-query values
matter: Q1/Q4/Q5 showed approximately 1% higher RSS, Q3 was unchanged, and Q2 was lower.
Do not interpret the aggregate median as a uniform memory improvement.
Separate GC diagnostics show the same 566 MiB committed heap capacity throughout both variants;
Q2 avoids one young collection. There is no new buffer, larger offset array, or additional leaf
materialization in the change. These are bounded observations, not an allocation-profiler proof.

## Raw evidence and reproduction

`attempts.jsonl` is the readable timing record. `raw-attempts.tar.gz` retains every
stdout/stderr, query JSON, exact command, resource-usage record, differential answer, cache-eviction
record, telemetry sample, focused test XML and local orchestration script. `SHA256SUMS` covers this
bundle. The larger native images, profiles and symbolized perf captures remain under the task
worktree's ignored `build/jsonbench-campaign/`, identified by the manifests.

Reproduce by extracting clean source for each variant, applying the retained candidate patch,
using `../../pgo-native.sh` for instrument → train → optimize under the retained guard, and running
the retained `paired-variants.py` with its two binary/database paths adjusted to the new evidence
root. The reference results must be regenerated from the same cleaned corpus using
`../../clickhouse-setup.sh`; they are not query constants in the implementation. The retained helper
scripts deliberately describe this local experiment, not a standalone portable benchmark package.
