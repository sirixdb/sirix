# Bug: storage epochs left FSST-compressible strings in overflow carriers

- Fix / issue: branch `fm/sirix-fsst-epoch-carrier-spill`. Found when the 1.0 change of the node-hash
  default to `NONE` turned `StorageEpochStoreIdentityTest` red.
- Symptom: `StorageEpochStoreIdentityTest.reusedFsstTableKeepsOversizedEditsInlineLikeThePlainTwin`
  failed under `hashKind(NONE)` only. The plain transaction committed 120 page frames and no
  document overflow carriers; the epoch-rotating twin committed 123 frames with three carriers and a
  data-file frontier 320 bytes further out. Values were equal. Impact: three values that fit inline
  once FSST-encoded were stored raw in overflow pages: extra pages, extra reads per lookup, and a
  store layout that depended on when epochs happened to rotate. Reachable on `main`, where `NONE` is
  already the REST server's default.
- Trigger: FSST with a symbol table reused from an earlier revision; a plain transaction with a
  bounded intent log (storage epochs) or explicit `asyncFlushStorageEpoch()`; an edit to a page whose
  newest committed fragment holds all 1,024 slots, then a rotation, then another edit to the same page
  that makes a value larger than the inline ceiling raw but not once encoded.
- Root cause: rotation was not invisible to the FSST binding and to the inline-or-carrier decision.
  Two invariants were broken:
  1. **The owned-fragment replay did not rebuild the page that the first copy-on-write built.** The
     first copy-on-write reads through `getPageFragments`, which stops at a newest committed fragment
     that holds every slot. The combine then sees one fragment and propagates its table binding. Once
     an epoch had spilled the page, `getOwnedPageFragments` returned the whole committed chain and
     `combineOwnedRecordPagesForModification` replayed the combine over all of it. A multi-fragment
     combine decompresses every string and leaves the page unbound, and the owned fragment's own
     compressed slots were decompressed too. The next oversized edits on that now-unbound page were
     correctly kept raw and became carriers. At commit, the revision table is handed to unbound pages
     only when the commit's unbound pages yield `MIN_SAMPLES_FOR_TABLE` (64) samples. The page's delta
     yielded three, so no table was distributed and the spilled page was committed unbound with three
     raw carriers. The plain twin never re-reads the page, so it stayed bound and the commit encoded
     the edits inline.
  2. **Epoch materialization judged pending strings by their raw size.** The commit runs
     `compressStringValues` before `processEntries`. `KeyValueLeafPage#materializePendingRecords`,
     which the writer calls when it stages a live leaf's carriers at an epoch or adopts a bulk leaf,
     went straight to `processEntries`. A raw oversized value on a page already bound to its table
     therefore became a canonical raw carrier. Under spilling epochs that carrier has a spill key, and
     the commit restores and re-encodes it. Under the async-flush import mode it is staged into the
     data file with a durable key, which `restoreOverflowRecords` deliberately skips on a table-bound
     page, so the carrier persisted.

  The hash kind only decides which pages a rotation catches mid-edit. Under `ROLLING` the page also
  holds the array and document-root records whose hashes every edit rewrites. It was in the live log
  and still bound at the commit, so the defect stayed hidden. A hashless record is smaller, yet it
  spilled where a larger one stayed inline: that reversal pointed at a defect rather than a threshold.
- Why tests missed it: the identity suite is the only plain-versus-rotating FSST comparison, and it
  inherits the JVM's hash default (`ROLLING`), under which the geometry never re-read the page. No
  test compared what a page looked like after an owned replay with what the first copy-on-write had
  produced, even though the replay's own documentation promised they matched. No test compared the
  inline-or-carrier decisions of the async-flush import mode with a plain transaction.
- Regression test: `StorageEpochFsstInlineDecisionTest`, run with `./gradlew
  :sirix-core:storageEpochStoreIdentityTest`:
  `aRotatedFullPageKeepsItsTableAndItsEditsInline` (`NONE`, `ROLLING` and `POSTORDER`) for invariant
  1, `anAsyncFlushEncodesPendingStringsBeforeJudgingTheirSize` (`NONE` and `ROLLING`) for invariant
  2, and the seeded differential property
  `plainAndRotatingTransactionsMakeIdenticalInlineDecisions`. The property draws values whose encoded
  sizes straddle the inline ceiling, under a reused table, with and without async flushes, and compares
  values, carrier sets, table bindings and, for spilling epochs, every persisted page frame.
  `StorageEpochStoreIdentityTest` is unchanged and passes under `-Dsirix.hashType=NONE` and `ROLLING`.
  Evidence that each test fails without its fix is in the PR.
- Lesson: a replay or rebuild must reproduce what the original read, not only how it combined it.
  Every route into a size-based placement decision must apply the same encoding as the canonical
  route. Differential tests must pin the configuration that changes geometry (here the hash kind)
  instead of inheriting a JVM default. See [Lessons](../LESSONS.md).

## Verification evidence

Fails before, passes after. One fix was reverted at a time on the fixed head; the suites were run
with `-Dsirix.hashType=NONE`:

| Engine | Identity regression | Full-page rotation | Async flush | Property trials |
| --- | --- | --- | --- | --- |
| fixed | passes | passes (`NONE`, `ROLLING`, `POSTORDER`) | passes (`NONE`, `ROLLING`) | 10/10 pass |
| replay fix reverted | 120 vs 123 frames | decision differs under all three | passes | 7/10 fail |
| encode-first fix reverted | passes | decisions equal | 6 raw carriers under both | both import-mode trials fail |
| both reverted | 120 vs 123 frames | fails | fails | 6/10 fail |

The replay fix alone repairs the reported failure. The encode-first defect is independent and shows
in the async-flush import mode.

Mutation testing: a focused PIT 1.25.5 run limited to the changed methods of `KeyValueLeafPage`,
`VersioningType`, `NodeStorageEngineWriter` and `BulkAdoptionDiagnostics`. The covering tests were
`StorageEpochFsstInlineDecisionTest`, `KeyValueLeafPagePendingFsstMaterializationTest`,
`StorageEpochStoreIdentityTest`, `AsyncFlushOwnedFragmentChainTest` and
`AdoptedOverflowCarrierStagingTest`, run with the identity task's deterministic codec setting.
The PR lane cannot select them, because it inherits the standard test task's excludes.
On changed lines: 53 killed, 0 without coverage, 6 survived. The first pass left 11 survivors and
18 uncovered mutants; the tests that killed them are the page-level unit test, the id-only
resolution test, the rebuilt-container assertions and the page created inside the transaction.
Each remaining survivor is equivalent:

- `KeyValueLeafPage.compressPendingStringRecords`, both `originalValue.length > 0` boundaries: an
  empty value encodes to `null`, because `encodeOrNull` declines anything shorter than
  `MIN_COMPRESSION_SIZE`.
- `VersioningType.committedFragmentsTheFirstCopyOnWriteRead`, `chain.size() > 1` changed to `>= 1`:
  truncating a one-fragment chain returns an equal list.
- `VersioningType.ownedFragmentCopier`, both `null` returns: `copySlotPreservingMetadata` treats a
  `null` copier exactly like an inactive one. The fallback copier is inactive whenever the owned
  fragment has no table, and its only other trigger is the unreachable state below.
- `VersioningType.acceptsBinding` forced to `true`: it differs only for a rebuilt page bound to a
  different table than the owned fragment. The live page binds one table for life and the replay
  reproduces the first copy-on-write's binding, so that state cannot arise. Were it to arise,
  `setFsstSymbolTableId` would refuse the rebind.

Write-path cost: a temporary JMH benchmark (not committed) timed one `NONE`-hash edit transaction
against a reused table on a full page and its neighbour: 81 oversized edits in three passes, then a
commit. One arm adds a spilling epoch per pass; the control never rotates. Fixed and base
(`650263ffe`) jars alternated over six rounds, one fork per arm per round, each round on an idle
machine pinned to the performance cores, with 15 warm-up and 40 measured single-shot iterations.

| Arm | Base, per-round means (ms/op) | Fixed, per-round means (ms/op) | Medians |
| --- | --- | --- | --- |
| No rotation (control) | 40.8, 46.4, 47.0, 42.7, 42.6, 31.1 | 43.1, 40.8, 39.8, 39.0, 37.2, 36.0 | 42.7 to 39.4: parity |
| Spilling epoch per pass | 113.6, 119.0, 131.1, 124.4, 121.7, 97.0 | 71.2, 83.6, 69.6, 63.2, 74.6, 74.8 | 120.3 to 72.9: 39 % faster |

The rotating path saves the decompress-on-merge of the committed chain and the raw carrier pages.
The non-rotating path changes only by the extracted pending-record loop.
