# Work-budget tests

A work-budget test runs one load or query and fails when the engine does materially **more work**
for it than it should: more leaves read, a slower route taken, more pages left pinned. It measures
work in the units the engine already counts, never in time.

They exist because a change in the amount of work is invisible to a suite that checks results. The
answer stays exact; it just comes from a slower route. Three such changes reached a 100M benchmark
before any test noticed: a projection load whose pre-commit spill was gated on the wrong writer
capability pinned every page until the off-heap arena was exhausted; a sorted view over an optional
aggregate field walked its key range twice before declining; and a native-image build setting cost
two queries about 150 ms each.

## Two layers: these tests are half the story

Performance regressions are guarded in two layers, and this is only the first.

1. **Work budgets (this package).** They run in the ordinary suites on every CI machine, are exact
   on any hardware, and say which path grew. They can only see a regression that changes the
   *amount of counted work*. A fixture kept here purely as a recorded measurement may carry
   `@Tag("heavy")`, which drops it from the cross-platform lanes only; the bound it guards must
   also be guarded by a lane-scale fixture that runs everywhere.
2. **A timed baseline harness** (working name `sirix-perf-baseline-harness`), run on one known
   machine under the benchmark campaign's measurement protocol, against recorded baselines:
   ClickBench's 43 queries, the 100M JSONBench numbers, and later the bitemporal benchmark. It is
   deliberately **not** part of these suites or of this repository's CI.

The second layer exists because a budget is blind to anything that makes the same work slower: a
slower call path, worse generated code, a native-image build setting, cache or allocator behaviour,
I/O latency. Do not read a green budget suite as "performance did not regress". It means the engine
did not start doing more work on the guarded paths, which is a narrower claim.

## Last week's regressions, and which budget catches each

| Regression | Would a budget have caught it? |
|---|---|
| The 100M projection load **died on `MEMORY_MAPPED`**: the pre-commit spill was gated on the writer's ability to *reclaim* an aborted tail instead of its ability to write ahead of the commit, so nothing spilled and every pinned page held its frame until the arena ran out (fixed in PR 1214) | **Yes.** `ProjectionLoadPinnedPageBudgetTest[MEMORY_MAPPED]`: with the old gate it spills 0 pages and peaks at 557 pinned pages, against 537 spilled and a peak of 62. |
| A **sorted view over an optional aggregate field walked its key range twice** before declining; a reviewer caught it by reading (fixed in PR 1216) | **Yes.** `ProjectionQueryWorkBudgetTest`, "a range holding a row without the aggregate declines the sorted view after one walk": with the double walk back it reads 3 data leaves, against 0. The same test fails on the tempting wrong fix, declining the whole view, because a clean range of that view must still be served. |
| **Q2 and Q3 lost about 150 ms each** on the 100M JSONBench after a CI repair moved two FFM downcall adapters from build-time to run-time initialization in native images | **No, and no work budget can.** The engine did exactly the same work; each native call went through a slower path. Only the timed layer, on a real native image, measures that. `NativeImageDowncallConfigTest` keeps the *remedy* (the opt-in build argument) from silently rotting, which is a different and smaller thing; see below. |

The remaining budgets guard paths those measurements rely on rather than a regression of that week:
the count-only group-by answered from the value-count summary (JSONBench Q1's route), the filtered
group-by on column slices (Q2 and Q3's), and the coalesced batch read, whose source records an
earlier regression of exactly this kind (an unsorted batch turned 9 MB of segments into 355 MB of
reads).

## Why there are no wall-clock assertions

Deliberately none, and please do not add one. A time threshold on a shared CI runner is flaky, so
it gets widened until it means nothing; and when it does fail it cannot say what changed. A work
counter reads the same on a laptop and on a loaded three-core runner, and a broken budget names the
path that grew. Timing belongs to the second layer above, on one known machine under a protocol.

For the same reason a budget test must not depend on anything a CI machine varies:

- **no timeouts as assertions**, and no sleeps;
- **no core-count dependence**: check how a path picks its parallelism before bounding it (the
  sorted-view summary lanes open one worker per 1 024 leaves of the range, so a ten-leaf fixture is
  serial everywhere);
- **no platform defaults**: name the storage backend (`BasicJsonDBStore` defaults to
  `MEMORY_MAPPED` on 64-bit Linux and macOS, `FILE_CHANNEL` on Windows);
- **no cache luck**: go cold first (`Databases.clearGlobalCaches()`, `ProjectionIndexCatalog.clearCache()`,
  `ProjectionIndexRegistry.clear()`). A warm buffer cache reads zero leaves whatever the route does.

If a figure is only stable on one platform, do not assert it. Capture it anyway: it shows up in the
failure table and tells the reader where the work went.

## What is here

| Test | Path | Fails when |
|---|---|---|
| `sirix-query` `ProjectionQueryWorkBudgetTest` | count-only group-by | it is no longer answered from the value-count summary the build maintained (three build paths), or the summary answers wrongly |
| | filtered group-by | it leaves the sliced route: whole-projection materialization (`eagerFallbacks`) or the generic pipeline |
| | grouped top-K, clean range | the sorted view stops serving it, or reads data leaves, or reads more summaries than the range has leaves |
| | grouped top-K, range holding a row without the aggregate | the view walks the range twice before declining, **or** declines ranges of the same view whose rows all carry a value |
| `sirix-query` `ValidTimeSliceWorkBudgetTest` | direct, folded bitemporal and plain-FLWOR valid-time slices | exact counts construct objects or read timestamp fields; first-item demand materializes more than one object; an empty closed candidate set enumerates interval/posting references or reads objects from unrelated inexact intervals; selective first/last matches expand unrelated postings (fixture-scale CI, opt-in 100,000-row evidence fixtures) |
| `sirix-query` `ProjectionLoadPinnedPageBudgetTest` | projection bulk load, `FILE_CHANNEL` and `MEMORY_MAPPED` | the pre-commit spill drains nothing, or the intent log's pinned region grows with the load |
| `sirix-core` `BatchedSegmentReadWorkBudgetTest` | batched page read (column fill) | the batch stops coalescing, is not sorted by file offset, or covers a region more than once |
| `sirix-core` `BloomOpenChunkFetchWorkBudgetTest` | Bloom prune, open chunk of referenced tails | the open chunk's tails are paginated by the sealed-block fetch window again, costing one read transaction per window instead of one for the whole chunk |
| `sirix-core` `JsonDiffArrayPositionWorkBudgetTest` | update-diff sidecar, array positions (on the default commit path) | an element's index is resolved by its own walk over the array prefix, a head insert touches an untouched suffix, **or** streaming append commits rewalk previously committed prefixes instead of consuming transient ingest positions (measurement: `docs/UPDATE_DIFF_INGEST_POSITIONS.md`) |
| `sirix-core` `JsonHashingWorkBudgetTest` | ROLLING hash/count maintenance after a skipped-root append | hash repair walks the unchanged array prefix or hashes only the final inserted root; 16/4096-element prefixes both require 18 record reads, one existing boundary child and three new root writes |
| `sirix-core` `HOTHistoricalBlobReadWorkBudgetTest` | warm first/last blob lookups at revision 65 of 130, all four versioning types (also revisions 1 and 130) | native eight-byte key probes read suffixes a byte at a time, or inline blobs probe the overflow-reference map; a referenced blob also proves that provenance still resolves |
| `sirix-core` `JsonIdentityReplayWorkBudgetTest` | identity import: append/no-op/deep/sparse epochs, all four versioning types | replay walks unchanged prefixes or numeric key gaps, restages unchanged identities, repeats shared ancestor proofs, reads presentation sidecars, or stops pruning identical durable regions; capture includes cursor/storage calls and commit-time path-cache initialization |
| `sirix-core` `JsonValidTimeIdentityWorkBudgetTest` | valid-time identity import beside 16/4096 unrelated direct fields, all four versioning types | nested/scalar or non-bound insertion/removal edits rescan unchanged bounds; cold interval hits, exactness and membership remain correct, with a real bound update proving the scan counter is live |
| `sirix-core` `ProjectionIdentityImportWorkBudgetTest` | indexed identity import: append/prepend and remove empty outer groups beside 16/4096 retained rows, all four versioning types | boundary links queue retained row removals/insertions or allocate their order labels; cold row data, sorted memberships, labels and key/numeric segment offsets must be preserved, with a one-row insertion as a positive control |
| `sirix-core` `JsonDiffBookkeepingWorkBudgetTest` | R8 pending inserts reordered by subtree moves | keyed updates become scans of the growing pending map; diagnostics count entry visits through map views as well as keyed operations |
| `sirix-core` `IndexCatalogueResolutionWorkBudgetTest` | index-catalogue lookup of a writer (every commit re-instantiates one) | a commit lists the `indexes/` directory, which holds about one catalogue file per revision, to find its writer's definitions; the fixtures also read every revision's definitions back, because a session that answers from memory can answer wrongly where the listing cannot |
| `sirix-core` `WriterListenerRetentionBudgetTest` | writer retirement across commits | revision-cached index listeners retain superseded writers: 130 listeners at 64 commits on the baseline versus two at 64 and 256 commits, then zero after close (measurement: `docs/WRITER_HEAP_RETENTION.md`) |
| `sirix-query` `NativeImageDowncallConfigTest` | native-image configuration | see below |

The original budgets were checked **by mutation**: the defect it guards was put back, the test was seen
to fail with the expected counter, and the source was restored. The measured healthy and broken
figures are in each test's comments.

Projection identity boundary mutation evidence (2026-10-06): restoring the raw left/right-link
comparison failed all 16 versioning/size/direction cases on queued row edits: 32 for 16 retained
rows and 8192 for 4096 rows. Emitted label buffers were 160 and 44536 bytes respectively; record
visits ranged from 922–1115 and 605518–622379. The fixed append/prepend and removal captures used
zero row edits, zero label bytes and 190–268 record visits across both sizes. The positive indexed
insertion control queued one row and allocated a 10-byte label. Raw logs and XML for this run live
under `build/replay/review-boundary-results/` in the active worktree.

Valid-time identity mutation evidence (2026-10-06): restoring broad object/parent scheduling
failed all eight versioning/width cases on the first nested append, inspecting 38 fields at
width 16 and 8198 at width 4096. The fixed nested/scalar and non-bound insertion/removal edits
inspected zero fields in all 32 captures. Direct bound updates inspected 38 and 8198 fields,
proving the counter is connected. This guards listener scans, not total replay work: distinct
field-path width still affects other epoch record/path-state work. Raw logs and XML live under
`build/replay/review-valid-time-results/` in the active worktree; no new timing evidence is claimed.

### The native-image guard, and what it cannot catch

`NativeImageDowncallConfigTest` keeps the configuration behind the 150 ms from drifting. It asks the
build rather than reading it: `bundles/sirix-query/build.gradle` defines the argument once, adds it
to the main image, and hands the test what Gradle itself evaluated. So it checks that the opt-in
produces exactly one argument naming the two holders and the default produces none; that the main
image really initializes the two holders early when, and only when, the build asked for it (run the
suite with `-Pnative.preinitializeDowncalls=true` to check the other state); that the smoke-test
image, which CI builds on the LTS toolchain, never does; that the documented `-P` switch is the
property the build really consults, so a rename cannot leave it silently doing nothing; that the
argument names two classes that exist and still hold nothing but their call signature, so
initializing them at build time is legal; and that no shared `native-image.properties` initializes a
holder early, by name, outer class or package. It asserts on nothing a document says: prose is
reworded without changing a build, and a test that breaks on that points at the wrong file.

What it **cannot** catch, stated plainly because it is the larger part:

- **whether a native image is fast.** It builds no image and times nothing. It would not have
  measured the 150 ms, and it will not measure the next loss of that kind;
- **whether an image builds at all** on a given GraalVM: the option is toolchain-dependent, and only
  building with that toolchain tells;
- **whether the binary someone measured was built with the option.** A benchmark binary built
  without `-Pnative.preinitializeDowncalls=true` is slower by that margin and nothing here notices;
- **any other build setting** with a performance effect (optimization level, PGO profile, GC,
  `-march`). It guards these two holders because they are the ones that already cost something.

All four belong to the timed layer.

## The counters

Almost nothing in this package counts anything. Each `WorkCounter` reads a figure the engine already
maintains, so a budget quotes the same numbers an investigation would:

- `EngineWorkCounters`: HOT leaf loads and fragments walked, coalesced read runs / span bytes /
  fallbacks / singletons, projection payload materialization (`lazyLoads`,
  `chunkMaterializations`, `eagerFallbacks`), intent-log promotions, index-catalogue directory
  listings, native HOT suffix lanes, overflow-reference map probes, and valid-time interval and
  posting references emitted by ordered-store scans, posting lookups and compressed posting chunks read.
  The valid-time figures guard empty and selective positive stabs that used to expand whole-array
  membership and verification postings. Only what a budget captures is
  listed: a catalog entry nothing reads is one more thing
  to keep true, and a *gated* one nothing asserts is worse than dead, because capturing it aborts
  the test wherever its gate is off.
- `EngineWorkCounters.REPLAY`: cursor/storage record visits (nested delegations count), path-summary cursor
  steps including writer reinitialization, created document identities, detached staged document records,
  memoized ancestry hops, attempted presentation sidecar reads, authoritative indirect/leaf resolutions,
  and pending-diff keyed operations/entry visits. `sirix.replay.workDiag` is static-final and off in
  production; the core test fork enables it, and captures require that gate to be live. The sidecar
  zero budget has a positive read control. These totals include lifecycle work during the capture;
  they do not represent unique records, allocated bytes, or physical disk reads.
- `EngineWorkCounters.REPLAY_PROJECTION_ROWS` and `REPLAY_PROJECTION_LABEL_BYTES`: old row removals
  and final row insertions queued by an identity epoch, and bytes allocated for its final row labels.
  The zero budgets for empty boundary changes have a positive indexed insertion control. These use
  the same `sirix.replay.workDiag` gate; label bytes count only the emitted label buffers, not total
  JVM allocations.
- `EngineWorkCounters.REPLAY_VALID_TIME_BOUND_FIELDS`: direct children inspected by the valid-time
  listener while reconstructing bounds. It shares the `sirix.replay.workDiag` gate and counts scans
  of old and final objects. Unrelated identity edits require zero; a direct bound update must scan
  both versions as a positive control. Full replay record/path counters remain separate diagnostics.
- `QueryWorkCounters` (`sirix-query`): the served-route counters, named as the benchmark runners
  print them on `# served:` (`groupAggregates`, `groupSummary`, `groupSliced`, `sortedGroupBys`,
  `predicateScans`, ...). What each route reads is section 7.3 of
  `docs/SEGMENT_PROJECTION_INDEXES.md`.
- Probes, for work the engine exposes through a test seam instead of a counter. They live in the
  package that owns the seam and restore whatever they displace: `SortedViewReadProbe` (summary
  reads against data-leaf reads, the only way to tell a sorted view's two walks apart) and
  `IntentLogEpochProbe` (async-flush rotations, spill batches, spilled pages, peak pinned pages of a
  load; a rotation is not always a spilling epoch, so non-vacuity floors go on the spill batches).
  `ArrayPositionCacheProbe` observes the ordinal maps and walk stacks after serialization: their
  actual primitive backing-array capacities catch eager preallocation even when sibling moves
  and entry counts stay small. Payload bytes exclude JVM-dependent headers; the arrays only grow
  during a serialization, so the retained payload also bounds its peak. A cache-observation floor
  makes a disconnected probe fail rather than report a vacuous zero allocation.
  `IngestArrayPositionProbe` reads the writer's transient ingest-ordinal map the same way, but it
  must be read **live**, inside the capture: commit releases that map, so a bound taken afterwards
  reads zero whatever the load allocated. Pair it with a second capture that must read non-zero.
- A counting decorator, where the path under budget already takes the thing it walks as a
  constructor argument, and adding an engine counter would put one on a cursor move. It counts only
  what the test itself hands in, so there is no global state and nothing to restore - but a decorator
  reads zero when the route stops going through it, so give its bound a floor, or a second capture
  on the same seam that must read non-zero. Today these include
  `JsonDiffArrayPositionWorkBudgetTest`'s `JsonResourceSession` wrapper,
  `BloomOpenChunkFetchWorkBudgetTest`'s `CountingFetcher`, and
  `ValidTimeSliceWorkBudgetTest`'s decorated real JSON cursor. The counting fetcher decorates the
  segment fetcher the prune already takes as an argument and carries its floor on the referenced
  payloads it requested. The valid-time cursor intercepts both transaction-time and revision-based
  document opening and requires observed reads on item demand.

**Gated counters.** Counters on a hot path are compiled away behind a `static final` flag, so a test
cannot switch one on for itself. The module's `test` block provides the property and the capture
asserts it (`WorkCounter.requireLive()`), because a switched-off counter reads zero and zero
satisfies every upper bound. For HOT work that is `sirix.hot.mergeDiag`, provided in both
`bundles/sirix-core/build.gradle` and `bundles/sirix-query/build.gradle`. Those test blocks also provide
`sirix.validTime.scanDiag`; `ValidTimeSliceWorkBudgetTest` requires zero interval/posting references
for empty answers and counts both kinds on nonempty answers through the same gated seam.
Identity replay additionally
uses `sirix.replay.workDiag`, enabled by the core test fork.

**Adding a counter to the engine.** Only when a path a test must guard has none. Keep it off the hot
path: gate it as `VersioningType` gates its merge counters if it sits on a per-record or per-page
path, and leave it unconditional, as `AbstractReader.regionChunkHits()` argues, only where each
event already costs a system call. Then list it in the catalog and in the diagnostics appendix of
`docs/SEGMENT_PROJECTION_INDEXES.md`.

## Adding a budget test

1. **Name the regression** in a sentence. If you cannot, there is nothing to budget.
2. **Build a fixture at fixture scale** (seconds, not minutes) on a schema of your own; nothing here
   may special-case a benchmark. Scale the engine's threshold down rather than the data up when a
   path only engages at scale.
3. **Make the result checkable.** Compare with the interpreter
   (`SirixCompileChain.createWithJsonStoreWithoutAutoWiring`) and shape the data so it is an oracle:
   no tied counts or tied minima, or the order of the answer is not defined. A budget may bound
   work; it may never buy it with a wrong answer.
4. **Go cold, capture, assert:**

   ```java
   final WorkCapture.Captured<String> query = WorkCapture.of(QueryWorkCounters.ROUTES)
                                                         .and(EngineWorkCounters.HOT_LEAVES)
                                                         .call(() -> evaluate(chain, context, QUERY));
   assertEquals(expected, query.result());
   query.work().assertExactly(QueryWorkCounters.GROUP_SUMMARY, 1, "what regression this guards")
               .assertBetween(EngineWorkCounters.HOT_LEAF_LOADS, 1, 8, "and this one");
   ```

   Use `assertExactly` only where the path is deterministic (a route is taken or it is not). Bound
   everything else, and give a ceiling a **floor** wherever the operation must do some of that work:
   a counter nobody increments any more reads zero, which every ceiling allows.
5. **Prove it by mutation.** Put the defect back, watch the test fail on the counter you expected,
   restore the source. Make sure the module really recompiled: a stale class file makes a restored
   source look broken. A budget test that was never seen to fail is decoration.
6. Mark the class `@Isolated`. The counters are process-wide.

To see the figures while choosing a bound, print every capture:

```bash
./gradlew :sirix-query:test --tests 'io.sirix.query.budget.*' -Dsirix.workBudget.print=true -i
```

## Changing a budget

A budget changes only **deliberately**. When one breaks, the default assumption is that the engine
regressed, not that the bound is wrong.

- Never widen a bound to make a red build green.
- The commit that changes a bound must say **why the work legitimately changed**, and justify the
  new bound with **evidence**: the printed capture before and after, and the figure the guarded
  defect produces, so the new bound still sits between the two. Update the measured figures in the
  test's comments in the same commit.
- A change that makes a path cheaper should **tighten** its bound. A bound left loose after an
  improvement stops guarding it.
- Deleting a budget needs the same justification as widening it.
