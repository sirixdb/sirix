# HOT read progress under continuous eviction

This is intermediate correctness and 1M steering evidence for the JSONBench campaign.
It does **not** establish a 100M win. The upstream workload and ranking pin are in
[`../20260911-hot-leaf-offsets/upstream-pin.json`](../20260911-hot-leaf-offsets/upstream-pin.json).

## Failure and repair

`HOTLeafUseAfterCloseTest.everyKeySurvivesReadbackUnderEviction` exhausted all 64
stamp-validation retries on both the original SegmentAccess experiment and unchanged
production baseline `b5374bda39b8f9fefbac74c32547ed322a0199d4`. Those paired failure logs
are retained under `evidence/eviction-failure-preserved/` in the raw archive. The
SegmentAccess experiment was put aside; it is not part of this repair.

The optimistic reader detected torn reads correctly but could not ensure progress:
every retry again exposed the replacement leaf to eviction for the entire read batch.
The cache already held a guard during publication, then released it before returning
the leaf. Raising a retry count would leave that gap intact.

After a failed stamp, `HOTTrieReader` now uses
`StorageEngineReader.loadHOTPageAndGuard()` to retain the loader's guard through the
read batch. Advancing releases the old guard; closing releases it and resets recovery
mode. Uncontended reads still use optimistic stamps. The mechanism is local to the
reader and page lifetime; it adds no global precomputation, extra leaf scan, benchmark
branch or benchmark counter. The 64 stamp-retry limit is unchanged.

## Regression evidence

The new deterministic regression closes the real leaf immediately after its chunk
prefix comparison, on every read attempt. On the unchanged baseline it fails with
the exact original exception:

```
HOT: chunk walk failed stamp validation on every one of 64 attempts — sustained allocator thrashing
```

Its uncontended control passes on that same baseline. The exact original test source
and XML are retained; subsequent test additions do not overwrite this reproduction.

The final repair passed 43 focused tests and 14 stress/integration checks:

- Deterministic read progress, uncontended operation, pooled-reader reset and guard
  release after a genuine read exception.
- Guard transfer during cache publication, immediate eviction, competing decoded
  copies, empty caches, and cold/cached/swizzled loads under all four versioning strategies.
  The loader tests require one disk read total across cold, cached and swizzled access.
- Existing leaf-stamp, frame-ownership, writer-guard and canonicalization tests.
- The original, unchanged eviction stress test: **0 missing keys out of 500,000**.
- Eight PEXT seek/range tests and five JSONBench query-shape integration tests.

The stress/integration snapshot and final snapshot have identical production behavior;
the later changes add test coverage and clarify comments. Each snapshot has a source
manifest. The original stress test was not weakened, skipped or given a higher timeout.

## Paired 1M JVM check

Both variants separately matched ClickHouse 26.7.3.19 exactly on all five queries over
the same cleaned 1,000,000-row corpus. Every timed attempt retained the same serving
counters: one aggregate and sliced group route; Q4/Q5 additionally used the numeric,
dense group route. There were 80 timed attempts, with before/after order reversed
in the second round. Every attempt used a fresh JVM. The first attempt followed verified
database-file eviction; hot is the minimum of the next three attempts.

The score below is the geometric mean of `(after seconds + .010) / (before seconds + .010)`:

| Round | Cold after/before | Hot after/before |
| --- | ---: | ---: |
| 1 | 1.015410 | 1.003847 |
| 2, reversed order | 0.969350 | 0.979228 |

This shows no consistent slowdown at this measurement's resolution; it is not a claim
of a performance improvement. Native PGO and 100M measurements remain necessary.
Maximum process RSS was 747,496 KiB before and 745,288 KiB after; raw per-process
resource usage is retained, without attributing this small difference to the repair.

Both arms use GraalVM JDK 25.0.3, identical JVM flags (`-Xms4g -Xmx12g`, Vector API,
native access and preview enabled), and frozen classpaths. All dependency artifacts
match. The query JAR differs only in generated build provenance; its class files match.
The core JAR differences are recorded in `eviction-jvm-jar-differences.json`.

All builds, tests and query phases ran under continuous fail-closed telemetry guards.
Accepted verdicts contain no guard failures. The machine remained on balanced profile
and `balance_power` EPP, below 90 C, without a throttle-counter increase or sampling
gap above two seconds. During each engine invocation, only verified campaign download
trees were suspended through PIDfds and resumed afterward; their guards kept sampling.
No shared service or unrelated process was signalled.

## Reproduction and remaining work

`raw-attempts.tar.gz` retains exact test logs/XML, commands, stdout/stderr, query dumps,
per-query differential results, counters, file-residency checks, resource usage,
telemetry and campaign scripts. Run the focused test classes listed above plus
`HOTLeafUseAfterCloseTest`, `HOTTrieReaderPextSeekTest`, and `JsonBenchShapeServingTest`.
The paired script records the two frozen JVM launcher/classpath manifests and uses
the repository's JSONBench comparator and file-eviction helper.

The decisive comparison must use the captain-selected Oracle GraalVM
`jdk-25i4-25.0.4.1.1-ea.01`, based on
`cb905c0ea0e868072ee525107e468ac2b5ff964d`, with a newly collected 100M PGO profile.
At this checkpoint that toolchain and the complete official corpus are still being
downloaded. The old incomplete 25i3 archive is not authoritative. Archive checksums,
runtime versions and the paired 100M result will be recorded when available.
