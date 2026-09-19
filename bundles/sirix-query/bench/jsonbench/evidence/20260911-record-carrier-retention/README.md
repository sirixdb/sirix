# Release ordinary overflow carriers between storage epochs

This is an intermediate import repair. It does **not** establish a 100M query win.
The first full-tier import exhausted the 8 GiB frame allocator before completing;
the complete cleaned corpus and that failed attempt are retained.

## Failure and controlled reproduction

On source `5d76b53acf14231dde7041cd35183c94b5c20a6a`, the full-tier JSONBench loader
used the default memory-mapped backend. It failed allocating a 64 KiB frame while
copying a frozen NAME record page during `StreamingGlobalDictionary.flush()`.
The allocator reported 8,052,178,944 active bytes, including 122,750 live 64 KiB
frames. The 24 GiB process scope recorded no kernel OOM or swap event. Its peak
memory was 20,841,013,248 bytes.

Two unchanged-source controls used the same 1M corpus, JDK, 12 GiB heap, 8 GiB
frame pool, six processors, loader and diagnostics. The second changed only
`-DstorageType=FILE_CHANNEL`. Counts below are the last common checkpoint, storage
epoch 3075; they are resident page objects, not an exact allocator ownership census.

| Configuration | Pinned entries | Resident HOT leaves | Resident record pages | Setup seconds |
| --- | ---: | ---: | ---: | ---: |
| Unchanged, memory-mapped | 4668 | 3917 | 585 | 72.409 |
| Unchanged, file-channel | 732 | 113 | 584 | 71.885 |
| Repaired, file-channel | 157 | 116 | 18 | 113.040 |

`MMStorage.createWriter()` deliberately disables preallocated writes. Consequently
`FileChannelWriter.supportsReclaimableUncommittedWrites()` rejects early structural
spill on that backend. The existing file-channel configuration removes most HOT
retention, but ordinary overflow-bearing record pages still accumulate. The repair
does not change either backend's capability or publication rules.

The focused regression avoids input-shape and capacity confounders: it appends 8192
dictionary values over 32 storage epochs in one logical transaction. Unchanged
production code grows from one to 22 resident record pages and leaves 22 promotion
pins. Cold reads still recover every value. Staging alone reduces the final pins
to ten; adding progress-aware deferrals keeps residency at one or two pages with
zero promotion pins. The exact baseline and both intermediate source/test snapshots
are retained. No allocator budget was increased.

## Generic mechanism

`TransactionIntentLog.forEachActiveRecordPage()` visits authoritative mutable record
pages in the current epoch. It excludes frozen, pinned, superseded and retired
pages. Publication during the callback may add entries; the initial visitation
bound leaves those entries for the next pass and forbids generation rotation.

Before starting an asynchronous epoch, `NodeStorageEngineWriter` materializes those
pages on their owning thread and stages ordinary overflow carriers through the
existing bounded side-page append owner. This happens before acquiring the append
permit because staging can fill and publish a side-only batch. Adopted immutable
pages already use that mechanism and are excluded from this new pass.

A mutable page can acquire fresh overflow values across many epochs.
`KeyValueLeafPage.resetFlushDeferralsIfCarriersResolved()` resets the retry count
only after all previous carriers have durable references, before materializing
fresh records. Unchanged pending or unstaged carriers retain their retry limit.
The existing terminal failure remains the first error reported, including after
reader teardown. Final revision publication and rollback still use the existing
storage protocol. This adds no corpus pass, global precomputation, answer cache,
benchmark predicate or query-serving counter.

## Validation and limits

The final frozen source passed **101 tests, zero skips**: four versioning strategies
for dictionary retention, visitor ownership/publication cases, carrier progress,
streaming dictionaries, adopted overflow staging, snapshot encoding and failure
paths, plus the earlier HOT guard/eviction regressions. The original 500,000-key
eviction stress test passed unchanged. Five query-shape integration tests passed.

The rebuilt real 1M import completed with zero discarded encodes and zero promotion
pins for DOCUMENT and NAME pages. NAME pages instead used 5520 bounded snapshot
deferrals. All five queries exactly matched the retained ClickHouse 26.7.3.19 UTC
references. Each query used one aggregate/sliced route; Q4/Q5 additionally used
the numeric/dense route. Dumps, comparisons and route counters are retained.

The observed setup cost increased from 71.885 to 113.040 seconds in these diagnostic
runs, and reported store size increased from 596,051,353 to 621,217,174 bytes.
These single setup attempts are not a paired query performance result. Earlier
publication does more work during ingestion; ranked query latency remains to be
measured separately. Bounded residency at 1M is not proof that every allocator
owner remains bounded at 100M.

Every test, import and correctness phase used continuous fail-closed telemetry:
balanced profile, `balance_power` EPP, below 90 C, unchanged throttle counters,
nominal 0.5-second sampling and at most two seconds between validated samples.
The process scope capped memory at 24 GiB with no swap; the guard required 2 GiB
available memory and 20 GiB free disk. Accepted final verdicts have no violations.
The failed full import and one intermediate failed test also record a secondary
`powerprofilesctl` SIGTERM during owned-tree cleanup. Those original verdicts are
preserved as failures; the allocator exception and assertion identify their
primary functional failures. No shared service or unrelated process was signalled.

## Why this appeared in JSONBench

The documented ClickBench HFT gate in `../../../clickbench/README.md` uses parallel
bulk import, file-channel storage and segment dictionaries (`globalDict=never`).
Its node threshold is 4,194,304; `ClickBenchLoadMain` defaults to 1,048,576.
JSONBench uses `JsonReader` and the ordinary subtree shredder, defaults to 131,072,
and its store builder does not override the Linux memory-mapped default. Both
routes go through `BasicJsonDBStore.beginImportTrx()` and
`KEEP_OPEN_ASYNC_FLUSH`: intermediate epochs do not publish revisions. The shared
default node-modification cap is 16,384, and the intent-log page bound can trigger
earlier flushes. Thus the nominal thresholds alone do not describe the cadence.

ClickBench's flat web-hits representation and parallel chunk adoption exercise a
different payload-ownership path from the JSONBench ordinary dictionary writes.
The backend-only control above proves a spill difference, and the dictionary-only
regression proves the retention mechanism without any nested JSON. Nested shape
alone has not been established as its cause. The previous HOT failure concerned
reader progress under concurrent eviction; see
`../20260911-hot-eviction-progress/README.md` for its independent reproduction.
Both defects reproduced before their respective repairs. Their exact introducing
commits have not been bisected, and documented ClickBench configuration is not
proof of the settings used in every historical run.

## Provenance and remaining campaign

The source came from a fresh `git archive` of the base above with exactly six
hashed overlays; no pooled build outputs were reused. Compilation was offline,
without build-cache reuse and with tasks rerun. The runtime manifest freezes 42
JARs and the JDK hashes. GraalVM is the captain-selected
`jdk-25i4-25.0.4.1.1-ea.01`, source `cb905c0ea0e868072ee525107e468ac2b5ff964d`.
Seven missing formatter dependencies were fetched into the private cache.

ClickHouse/JSONBench remained pinned at
`e6c7c98dc766394d51f7d506a3dd2b5d51165d70` when the full import began. Its 100M tier
is the requested target; the dashboard default is actually 1B/hot. At 100M the
hot leader is ClickHouse 25.11, score 1.0229245566; the cold leader is StarRocks
4.0.1. The captain authorized current ClickHouse 26.7.3.19 for the informal local
leader-engine comparison. The five exact queries match `../../queries.sql`.

The score is the geometric mean of `(seconds + .010)/(fastest seconds + .010)`
across all five queries. The final local comparison uses paired AB/BA rounds,
cold attempt 1 and hot minimum of attempts 2–3, after exact correctness checks.
Setup and PGO training are excluded. No 100M query result exists at this checkpoint.

The complete cleaned corpus has 99,999,968 accepted rows and 32 rejected lines:
47,811,781,297 logical bytes, SHA-256
`f063d7db1d71009122ad4e99feb956423f52df3aa3f1d090a602cd44e121e538`.
Its verified shared gzip is 13,286,750,359 bytes, SHA-256
`925e84df83435fb1dcb621ad89e9b48f859c8b7c0cbeedf084605bd3ff48312a`.
The 1M steering input SHA-256 is
`7beb29f6c036fe784754ff34d68d1f216c6cc89de12155da06f725bdf5c8536e`.
Next are a fresh committed-source native build, repaired full-tier import,
full-tier PGO training and the paired five-query comparison. The old instrumented
image/profile must not be reused across this production change.

`raw-attempts.tar.gz` contains 170 files: raw attempts, failed and passing tests,
telemetry, source overlays, manifests, commands, answer dumps and comparisons.
Archive SHA-256:
`12c514b074b4dc6ea3c6cdba90c025c5d1f49e24c6d599234238f5c88d582bf8`.
Reproduce the regression with
`StreamingDictionaryOverflowRetentionTest`; the retained scripts enumerate the
broader tests and exact real-input control commands.
