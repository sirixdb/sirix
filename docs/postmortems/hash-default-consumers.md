# Bug: hashless resources lost diff safety and planner size signals

- Fix / issue: the node-hashes-default-off change for Sirix 1.0.
- Symptom: a caller could force the hash shortcut on a resource without stored hashes, causing changed subtrees to compare equal. Query size heuristics also saw every hashless document as empty, disabling large-document optimization routes.
- Trigger: create a resource with `hashKind(NONE)`, edit it across revisions, and invoke a diff with `DiffOptimized.HASHED` and `hashKind(ROLLING)`. Independently, ask the query statistics provider for the size of a populated hashless document.
- Root cause: `DiffFactory.Builder` defaulted to `ROLLING` independently of the resource, and `AbstractDiff` trusted that setting. Stored descendant counts share the node-hash storage gate, but four planner size heuristics assumed the count was unconditional. Changing the Java default to `NONE` exposed both assumptions on the default path.
- Why tests missed it: public diff callers normally disabled the shortcut themselves. Query result tests could not detect degraded planner decisions because the fallback execution still produced correct values. Some hash-dependent fixtures also compared two absent counts (`0 == 0`).
- Regression test: `DiffFactoryHashKindTest` covers forced and default hash kinds; `JsonDiffHashEquivalenceTest` and `XmlDiffHashEquivalenceTest` compare revision histories with hashes on and off. `DocumentNodeCountTest` requires a positive size signal and tests both the stored-count and node-frontier branches. Removing the resource-authoritative guard or replacing the fallback with the stored count violates those assertions. Run the corresponding `:sirix-core:test` and `:sirix-query:test --tests` filters.
- Lesson: gate an optimization at the resource that makes it valid, and inventory all fields gated by a configuration change. Assert a planner's decision inputs independently of result correctness. See the two hash-default entries in [Lessons](../LESSONS.md).

Mutation evidence: actual PIT kills all three `DocumentNodeCount.estimate` mutants, both
`ResourceConfiguration.defaultHashType` mutants and the resource-authoritative `AbstractDiff`
constructor guard. The builder's new default assignment has no mutation under PIT's default
mutators; its behavior is asserted for both hash kinds.

Four surviving mutations in `DiffFactory.Builder` are outside the changed logic. All affect
the existing revision-argument checks before the new hash-default assignment:

| Method | Line | Mutation | Justification |
|---|---|---|---|
| `Builder` constructor | 226 | boundary `newRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 226 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |
| `Builder` constructor | 228 | boundary `oldRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 228 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |

A wider class-level run also mutates legacy traversal loops and unrelated configuration methods.
Its two memory errors and one timeout are incomplete analysis of those methods, not killed mutants.
The follow-up excludes those unchanged methods and retains the changed constructor and resolver.

## Follow-up: deleted keys are not a sampling population

- Fix / issue: review finding R1 in the node-hashes-default-off change.
- Symptom: histogram collection returned `null` and registration returned `false` for a surviving price value after a large subtree was deleted from a hashless resource.
- Trigger: create `{"discard":[100000 numbers],"price":[7]}`, delete `discard`, commit, close all handles and reopen. Only three live nodes remain, but the node-key frontier still exceeds 100000; using that frontier with a 10000-value sample gives stride 5 and skips every live node.
- Root cause: a historical upper bound suitable for planner size gating was reused as a current sampling population. The sampling invariant is that only a stored live descendant count may increase the stride; an unavailable count must retain stride 1. Both `collect` overloads and both `collectAndRegister` overloads, for latest and explicit revisions and for hashed and hashless resources, reach the shared `sampleValues` boundary. The four planner size heuristics still use `DocumentNodeCount.estimate` because overestimates there do not discard samples.
- Why tests missed it: `DocumentNodeCountTest` covers positive estimates without deletion history, while `HistogramCollectorTest` previously built histograms manually rather than exercising collection from a persisted resource.
- Regression test: `HistogramCollectorTest.deletedSubtreeDoesNotHideSurvivingSample` uses NONE and ROLLING resources and the collector's supported named-array shape. After cold reopen it requires exactly one sample, one distinct value and an exact most-common-value entry of 7 with frequency 1, then requires latest and explicit-revision registration to retain that histogram. Missing-field collection must still return `null` and refuse registration. The oracle is the known single surviving input value, independently of either count source. Restoring the frontier-based sampler must fail the NONE arm while the ROLLING control remains valid.
- Verification: all 13 focused tests in `HistogramCollectorTest` and `DocumentNodeCountTest` passed. A controlled historical-source mutation replacing the stored-count read with `DocumentNodeCount.estimate` failed the NONE arm on the missing histogram and passed the ROLLING control; restoring the fix passed both. The collector now matches its pre-change source, so no unrelated formatting adds a whole-class mutation baseline. Run-local logs and JUnit XML are under `build/review-r1/`; no complete suite, lint or other pipeline phase was run.

  Focused command (preceded by a free-disk check; run with over 60 GB available):

  ```sh
  /home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh ./gradlew :sirix-query:test \
    --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx3g \
    -PtestHeapMin=256m -PtestHeapMax=3g \
    --tests 'io.sirix.query.compiler.optimizer.stats.HistogramCollectorTest' \
    --tests 'io.sirix.query.compiler.optimizer.stats.DocumentNodeCountTest' \
    -x :sirix-query:bitemporalLoaderAllocationTest \
    -x :sirix-query:temporalNodeReaderLifetimeGenericTest \
    -x :sirix-query:translatorThresholdIndependentTest \
    -x :sirix-query:translatorThresholdLegacyTest
  ```
- Lesson: a historical key frontier cannot substitute for a current sampling population; keep approximate size signals out of filters that can drop all valid observations. See [Lessons](../LESSONS.md).
