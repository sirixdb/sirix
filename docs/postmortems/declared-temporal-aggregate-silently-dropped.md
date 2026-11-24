# Bug: a declared temporal aggregate was silently not created

- Fix / issue: this change; found while diagnosing the intermittent
  `DerivedIndexMaintainerTest` failures on the GraalVM 25i5 toolchain acceptance.
- Symptom: `createIndexes` reported success for a temporal-aggregate declaration whose hidden
  derived resource could not be installed. The resource then served the exact fallback for the rest
  of its session, `getDerivedResource()` stayed `null` for good, and the only trace was one
  `"Derived resource unavailable for {}; queries retain exact fallback"` log line. A caller that
  declared an index and got no exception had no way to learn the index does not exist.
- Trigger: any failure inside `LocalDatabase.openDerivedResource` while a caller is declaring a
  temporal aggregate — in the regression test an authorization policy that may create `records` but
  not the hidden `records$derived`; in the field a transient page-decode failure during the derived
  resource's bootstrap commit.
- Root cause: `LocalDatabase.enableDerivedIndexes` was reached from two directions and treated both
  the same. Opening a resource that *may* carry derived indexes is opportunistic — every query
  retains the exact fallback, so an unusable derived store must not stop the open. Declaring a
  temporal aggregate is the opposite: installing the derived resource IS the caller's request. One
  `catch (RuntimeException unavailable)` around the shared body absorbed the declaration's failure
  as if it were an optional open-time step, so `TemporalAggregateIndexes.build` never saw it and
  never marked the transaction rollback-only.
- Why tests missed it: every existing test drove the declaration on a resource whose derived store
  could be created, so tests only exercised fallback during optional open-time activation.
  No test asserted what a *failing* declaration does, and the log line is not an oracle.
- Regression test: `DeclaredTemporalAggregateFailureTest` —
  `aDeclarationWhoseDerivedResourceCannotBeInstalledFails` fails before the fix with "Expected
  io.sirix.exception.SirixUsageException to be thrown, but nothing was thrown" and passes after;
  `theSameDeclarationSucceedsWithoutTheDenial` passes on both arms and pins that the injected
  denial, not the declaration itself, is what the first test detects. The failure is injected
  through the production `ResourceAccessScope` policy seam, so it is deterministic.
- Lesson: an optional fallback must be chosen by the caller's intent, not by the method that
  happens to host it; see [Lessons](../LESSONS.md).

## Note: this is not the cause of the `DerivedIndexMaintainerTest` flake

The flake that led here is a cipher round-trip failure under the GraalVM 25i5 compiler, reproduced
with no SirixDB code at all (7 mismatches in 10,000,000 Tink streaming round-trips with the Graal
compiler on, 0 in 10,000,000 with `-XX:-UseJVMCICompiler`, and 0 in 10,000,000 each on GraalVM
25.0.3 and Temurin 25.0.5.1). This swallow is why that failure surfaced as an unrelated
`expected: not <null>` assertion three lines later instead of as the storage error it was.
