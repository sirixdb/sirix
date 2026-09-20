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

## Why there are no wall-clock assertions

Deliberately none, and please do not add one. A time threshold on a shared CI runner is flaky, so
it gets widened until it means nothing; and when it does fail it cannot say what changed. A work
counter reads the same on a laptop and on a loaded three-core runner, and a broken budget names the
path that grew. Timed measurement against recorded baselines is a separate layer, run on one known
machine under a measurement protocol; it is not part of the ordinary suites.

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
| `sirix-query` `ProjectionLoadPinnedPageBudgetTest` | projection bulk load, `FILE_CHANNEL` and `MEMORY_MAPPED` | the pre-commit spill drains nothing, or the intent log's pinned region grows with the load |
| `sirix-core` `BatchedSegmentReadWorkBudgetTest` | batched page read (column fill) | the batch stops coalescing, is not sorted by file offset, or covers a region more than once |
| `sirix-query` `NativeImageDowncallConfigTest` | native-image configuration | see below |

Every one of these was checked **by mutation**: the defect it guards was put back, the test was seen
to fail with the expected counter, and the source was restored. The measured healthy and broken
figures are in each test's comments.

### The native-image guard, and what it cannot catch

`NativeImageDowncallConfigTest` keeps the configuration behind the 150 ms from drifting: the opt-in
property adds exactly the documented builder argument, single-quoted; that argument names two
classes that exist; each still holds nothing but its call signature, so initializing it at build
time is legal; and no shared `native-image.properties` or build script initializes a holder early
outside the opt-in, by name, outer class or package, which the GraalVM LTS line rejects.

It does **not** build a native image, so it cannot tell whether an image built with the option is
fast, or builds at all on a given GraalVM. Only a timed run of a real image can.

## The counters

Nothing in this package counts anything. Each `WorkCounter` reads a figure the engine already
maintains, so a budget quotes the same numbers an investigation would:

- `EngineWorkCounters`: HOT leaf loads and fragments walked, coalesced read runs / span bytes /
  fallbacks / singletons, column-only chunk reads, projection payload materialization
  (`lazyLoads`, `chunkMaterializations`, `eagerFallbacks`), frame-slot allocations and releases,
  intent-log promotions.
- `QueryWorkCounters` (`sirix-query`): the served-route counters, named as the benchmark runners
  print them on `# served:` (`groupAggregates`, `groupSummary`, `groupSliced`, `sortedGroupBys`,
  `predicateScans`, ...). What each route reads is section 7.3 of
  `docs/SEGMENT_PROJECTION_INDEXES.md`.
- Probes, for work the engine exposes through a test seam instead of a counter. They live in the
  package that owns the seam and restore whatever they displace: `SortedViewReadProbe` (summary
  reads against data-leaf reads, the only way to tell a sorted view's two walks apart) and
  `IntentLogEpochProbe` (epochs, spilled pages, peak pinned pages of a load).

**Gated counters.** Counters on a hot path are compiled away behind a `static final` flag, so a test
cannot switch one on for itself. The module's `test` block provides the property and the capture
asserts it (`WorkCounter.requireLive()`), because a switched-off counter reads zero and zero
satisfies every upper bound. Today that is `sirix.hot.mergeDiag`, provided in both
`bundles/sirix-core/build.gradle` and `bundles/sirix-query/build.gradle`.

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
