# Writer retention across commits

A one-operation-per-commit load can retain a closed storage engine through a revision's
cached index controller. A count bound on catalogue entries alone does not bound the
large transaction state by the number of active writers.

## Diagnosis

Baseline: `28a95fe8efe4d0910a931475b1d61ee884d1b48f` (main at task launch).
A single JSON resource used CAS and VALIDTIME indexes, SLIDING_SNAPSHOT with a
three-revision window, and a 512 MiB heap. Its document contained one object;
each commit changed only its integer value. Live class histograms and live heap
dumps were captured at 50 and 150 updates. The initial shred/index setup also
committed, so writer counts include those epochs.

| Updates | Live writers | Refused-overflow tables | Total live histogram bytes |
| ---: | ---: | ---: | ---: |
| 50 | 53 | 53 | 119,356,728 |
| 150 | 153 | 153 | 238,241,456 |

Both dumps contained this strong retaining path:

```text
JsonResourceSessionImpl.wtxIndexControllers
  -> Caffeine cache / ConcurrentHashMap entries
  -> JsonIndexController.listenerSnapshot
  -> JsonValidTimeIndexListener.storageEngineWriter
  -> NodeStorageEngineWriter
```

CAS listeners also own a HOT writer and path-summary reader bound to their storage
engine. The cache retained transaction listeners alongside catalogue definitions.
Each retained writer included a `RefusedOverflowLeafTable` with a 1 MiB primitive
array, despite its native pages and backend already having been closed.

The reported rollback exception had a different immediate cause: successor
construction threw after the durable predecessor was closed. The node transaction
still referenced that predecessor, whose close had nulled its local container
holders. With assertions disabled, rollback entered those cleared holders and
threw a NullPointerException; with assertions enabled it rejected the closed reader.
No half-constructed successor had been published to the node transaction.

The fixed code retained two writers and two refused-overflow tables at both
checkpoints. Its live histogram total was 59,272,016 bytes at 50 updates and
59,577,584 bytes at 150 updates. Cached retired controllers no longer led to
closed writers through transaction listeners. The dumps were deleted after analysis;
the histograms and retaining-path reports remain in `build/writer-retention/`.
The constant second writer is held by the node transaction's Dewey-ID manager;
it does not accumulate as revisions advance.

## Lifetime rules

`NodeStorageEngineWriter.close()` retires its controller's change listeners and
transaction-serving handles after fencing asynchronous work. Cached catalogue
definitions remain available, while listener retention follows active writer
lifetimes, including the bounded pending writer of a pipelined commit. Listener retirement leaves
the catalogue cache size and persisted formats unchanged.

The controller belongs to the prepared revision root, which can differ from the
represented revision after a revert. Factory construction restores its catalogue
from the latest persisted snapshot at or below `representRevision` through the
session's memoized resolver. An absent snapshot inherits; an explicitly persisted
empty catalogue stops inheritance within that revision's history. Restoration
discards uncommitted definitions on reused controllers. `revertTo(r)` restores revision `r`'s
catalogue; incompatible VALIDTIME formats are rejected as described in
[Valid-time key slices](VALID_TIME_KEY_SLICES.md#index-representation); it leaves the intervening
revisions and their catalogues readable.

On successful successor handoff, the predecessor's complete live catalogue replaces
that restored catalogue, including an empty set or partial drops. This is necessary
while a pipelined predecessor has not yet published its catalogue. Listener rebinding
then derives maintenance and capability flags from exactly those definitions.
The handoff retains the existing listener snapshot locally until rebinding succeeds;
failure aborts those exact owners independently, while successful intermediate
epochs preserve active projection builds. The handoff uses the existing listener
array and definition-set snapshot.

Dropping definitions publishes pending non-projection maintenance through the page-flush
hook before replacing listeners. Retained VALIDTIME definitions therefore keep endpoint,
membership and verification changes when the writer commits immediately after a drop.
Projection listeners keep the existing exact-owner abort and retained-owner handoff.
`ValidTimePendingIndexDropTest` covers CAS, projection and partial VALIDTIME drops,
strict and inclusive reads, rounded bounds, and historical revisions after reopen.

The factory owns the reader, transaction intent log and backend until construction
succeeds. A construction failure closes all three independently and preserves the
original exception, attaching cleanup failures only as suppressed diagnostics.
Native append scratch is acquired after fallible heap initialization instead of in
an early field initializer.

If a durable commit closed its predecessor but failed to construct a successor,
node rollback creates a fresh writer from the session's published durable revision.
It never calls rollback on the closed predecessor, and restores the node transaction
to its running state after rebinding succeeds.

## Deterministic guards

`WriterListenerRetentionBudgetTest` changes one indexed value over 256 commits and
counts actual listener roots in the session's controller cache. Its budget is the
two listeners of the single active writer, then zero after close. On the baseline
it fails at 64 commits with 130 listeners. It also reads historical document values
and catalogue definitions back.

`WriterConstructionFailureCleanupTest` injects an OutOfMemoryError before successor
local container initialization. It verifies reader and intent-log cleanup, preserves
the original failure, recovers through rollback, commits again, and reads the
predecessor revision back. A second test fails before reader construction and
checks backend cleanup and suppressed cleanup errors. Loading baseline factory
classes makes both construction tests fail; loading the baseline rollback class
makes the recovery test fail on its closed reader. These guards do not require GC scheduling, sleeps or
wall-clock assertions.

Run them through the normal `:sirix-core:test` task; the retained-listener guard is
also part of the work-budget block in `docs/VERIFICATION.md`.

## Ownership regression follow-up

The original workload, full-suite and latency evidence below belongs to submitted
`7557433ac1f786645e11759fb1b13403d02ff5ec`; it is not evidence for the review fixes.

Prepared-revision controller ownership and projection-owner handoff follow the
[lifetime rules](#lifetime-rules).

The listener guard additionally performs sixteen indexed reverts after ten commits
in both JSON and XML. It retains the existing two-listener bound, checks zero roots
after close, and reopens the database to verify historical values, definitions and
CAS lookups. Construction guards cover rollback and clean close in both synchronous
and pipelined load modes, including two owned builds and an unrelated active owner.
Successful intermediate-load guards finalize and read the two projected values back.

Focused review verification on 2026-10-04 passed all 21 selected tests: eight
construction/projection guards, three listener-retention guards, four catalogue
budgets and six existing projection-lineage guards. Submitted-head factory overlays
failed both revert guards with four listeners against the unchanged bound of two;
submitted-head handoff overlays failed all four synchronous/pipelined rollback/close
guards because their exact projection owners remained unfinished. The overlays
compiled into separate directories, leaving the verified production classes intact.

Results, source hashes, all attempt logs and mutation reports are recorded in
`build/writer-retention/focused-review.json`, `focused-review.log`, `fixed-results/`
and `mutations/`. The final command used the supplied memory gate, private Maven
repository, two workers and 512 MiB–2 GiB test heaps. Its task-owned
`/var/tmp/sirix-bitemporal/writer-retention-review-*` stores were deleted. The outer pipeline owns
full-suite, all-work-budget, formatting and original-workload revalidation. These
ownership checks make no new latency claim.

## Inherited catalogue recovery follow-up

Round 1's controller-identity correction still reset definitions before loading only
`lastStoredRevision.xml`. An unchanged intermediate commit intentionally omits that
file, so reverting, rolling back after successor failure, or reopening could leave
the next writer without index maintenance listeners. Persisted restoration now
follows the [lifetime rules](#lifetime-rules); fresh controller initialization uses
the shared loader without an extra reset. The resolver and its memoization are
unchanged.

`WriterCatalogueRecoveryTest` persists indexed revision 1, triggers a count-based
unchanged intermediate revision 2, and confirms that `2.xml` is absent. It covers
JSON/XML revert and failed-successor rollback plus database close/reopen, each in
synchronous and pipelined modes. It checks restored definitions, maintained CAS
lookups, removal of obsolete postings, and historical results after reopen. Revert
also discards an uncommitted index definition. Two additional guards preserve an
explicit empty catalogue through an absent later snapshot and rollback of an
uncommitted definition. Reverting to the earlier indexed revision restores its
definition and CAS lookup at the new head, including after reopen, while the
intervening empty-catalogue revision and its data remain readable. Round 1's
projection-owner and listener retention guards remain in place.

Before changing production code, all twelve skipped-catalogue cases reproduced
round 1's loss of definitions against `98507f0aa2def53bd56bf82059b781611a5ca98c`;
the two explicit-empty guards passed. Baseline results and source hashes are retained
in `build/writer-retention/r3/round1.json`, `round1.log`, and `round1/results/`.
Exploratory projection fixture attempts are also retained separately; they are not
catalogue-loss regression evidence.

After the correction, the focused verification passed all 35 tests with zero
failures or skips: fourteen new catalogue guards plus the existing twenty-one
construction/projection, listener-retention, catalogue-budget and projection-lineage
guards. Production classes were freshly compiled from this worktree. Results,
command, source SHA-256 values and cleanup status are recorded in
`build/writer-retention/r3/fixed.json`, `fixed.log`, and `fixed/results/`.
Both baseline and fixed runs used the supplied memory-gated wrapper, private Maven
repository, two workers and 512 MiB–2 GiB test heaps. Every task-owned
`/var/tmp/sirix-bitemporal/writer-retention-r3-*` store was deleted. Existing work
budgets were not changed.

The outer pipeline owns broader suites, all work budgets, latest-head SH1 workload
and latency verification, and formatting. Previous submitted-head evidence remains
retained and does not validate this catalogue correction.

## Pending catalogue handoff guards

`WriterCatalogueHandoffTest` holds a count-based `KEEP_OPEN_ASYNC_COMMIT` publication
at the existing `before-harden` hook with latches. Its JSON/XML cases drop all
definitions, one CAS definition while retaining a sibling, or NAME while retaining
CAS. They check the pending successor, live and reopened catalogues, capability
flags, document values and maintained CAS lookups across successor mutation and an
explicit commit. The successful handoff uses the [lifetime rules](#lifetime-rules).

The guards reproduced catalogue resurrection on
`12275306172915bcb5e66a5fbffd4a18e0eae770` before repair. Baseline and fixed commands,
source hashes and results are retained in `build/writer-retention/r4/baseline.json`,
`fixed.json` and their result/log directories. The fixed run also included the
existing recovery, construction, retention, catalogue-budget and projection-lineage
guards. This focused evidence does not replace final-head original-workload,
full-suite, all-work-budget or SH1 latency verification by the outer pipeline.

## XML NAME bulk-builder follow-up

The follow-up is implemented; see [NAME index usage](../README.md#indexes) and
[NameIndexBulkBuildTest](../bundles/sirix-core/src/test/java/io/sirix/index/name/NameIndexBulkBuildTest.java).
The round 1 retention fixture creates its NAME index before inserting elements,
so its evidence covers incremental maintenance. The original failing bulk-build
log and stack are retained in `build/writer-retention/focused-review.log.attempt-2`
and `fixed-results-attempt-2/`.

## Original workload verification

The original SH1 per-operation byte driver completed at 2 GiB: 21,116 commits,
including all insert/edit measurement windows at 2,500, 5,000, 10,000 and 20,000
array elements. The original SH1 t100k E0 driver completed all 112,000 event commits
at 4 GiB. Both used CAS plus VALIDTIME, SLIDING_SNAPSHOT/window 3, custom commit
timestamps and disabled diff sidecars, as in the original failure evidence.
Inputs were the kit's supplied event streams. Stores were created under the task-owned
`/var/tmp/sirix-bitemporal/sirix-writer-heap-retention-many-commits` directory and
deleted after the successful measurements. JVMs used the shared memory-gated limiter.

## Suite and work-budget verification

The full core suite ran 12,112 tests without failures (76 skipped). The full query
suite ran 1,894 tests (7 skipped), with only the two already-known Brackit
codepoint-ordering differential failures:

- `GroupTopKDifferentialTest.stringMinAndMaxWithCollationAdversariesAndAllMissingGroup`
- `StringPredicateDifferentialTest.supplementaryCharacterOrderingMatchesTheInterpreter`

All 30 core work-budget tests and 16 query work-budget tests passed with their
existing bounds. The two construction guards also passed in the final core budget
run. Every Gradle invocation used the memory-gated limiter, two workers, a 2 GiB
Gradle heap, and test heaps of 512 MiB–2 GiB. Maven's private repository was
`build/writer-retention/m2`; no installation into `~/.m2` was required.

## Latency method

Matched alternating runs use the SH1 t25k loader's existing 25 publications and
77 commits per load. The adapter times only each existing `commit(message,
timestamp)` call with `System.nanoTime()` and records a CSV; batching, input,
index definitions and revision settings are unchanged. Store-capacity accounting
is scoped to this task's directory, so other workers' stores do not affect its
capacity checks. Each arm uses the same query classes and dependencies with
frozen baseline or fixed core classes, a 2 GiB heap, and the shared limiter.
The task's full suites and large loads finished before these runs started.
The p50 is the median; p99 is the nearest-rank percentile of all 77 commits.

Sixteen matched pairs completed: eight baseline-first followed by eight fixed-first.
All 32 runs are retained in the comparison; every successful store was deleted.

| Commit percentile | Baseline mean ms | Fixed mean ms | Change | Median paired fixed/baseline ratio | 95% paired-bootstrap interval |
| --- | ---: | ---: | ---: | ---: | --- |
| p50 | 14.414 | 14.120 | -2.04% | 0.99635 | [0.93903, 1.03300] |
| p99 | 1176.419 | 1125.600 | -4.32% | 0.96001 | [0.86876, 1.04292] |

Intervals use 5,000 percentile bootstrap draws with seed 20261003, resampling whole
matched pairs together. Both average percentiles and median paired ratios are
lower on the fixed code. The intervals include equality: these shared-laptop
measurements show no observed regression and do not establish a speedup.
Timing CSVs, invocation manifests, input/source hashes and the full per-pair
comparison remain in `build/writer-retention/`.
