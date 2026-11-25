# Bug: two reported resource-ownership leaks, neither still in main

- Fix / issue: [#1407](https://github.com/sirixdb/sirix/issues/1407) (LZ4 decompression leaks its
  staging buffer and arena when heap staging fails) and
  [#1408](https://github.com/sirixdb/sirix/issues/1408) (native image: confined serializer arenas
  are never closed). No production logic changed: this pull request adds the regression tests and
  the CI lane that make both invariants fail loudly, and corrects the two comments that described
  the old ownership.
- Symptom: neither was ever observed. #1407 would retain the output frame and one confined staging
  arena per failed decode of a heap-backed page image, turning a recoverable read failure into a
  native-memory leak for the life of the process. #1408 would retain one confined arena per
  serialized disposable page in a GraalVM native image, where `SharedArenas.close(Arena)` releases
  nothing.
- Trigger: #1407 — a failure of the staging allocate-and-copy on the scoped decompression path,
  i.e. an `Arena.ofConfined()` creation, an arena allocation or a `MemorySegment.copy` that throws,
  reachable only under genuine native-memory exhaustion. #1408 — any write in a native image (or
  under `-Dsirix.arena.strategy=auto`), because every disposable page serialization creates a
  serializer-local confined writer table.
- Root cause (both, same shape): a resource whose owner is implicit. #1407: pre-[#1346](
  https://github.com/sirixdb/sirix/pull/1346) `FFILz4Compressor.decompressScoped` allocated the
  output frame, then created a confined arena and staged the heap input into it — all three
  statements *before* the `try` whose `finally` released them, so every failure inside that window
  escaped with both resources held. #1408: the arena of a `RegionTable` created by
  `newConfinedWriterTable()` is confined, while the shared arena of an ordinary table is released
  through `SharedArenas.close(Arena)` — a method that is deliberately a no-op under the `auto` and
  `global` strategies. Routing the confined arena through it releases nothing and reports success.
- Why they are gone: #1346 moved the staging onto the allocator's scratch pool *inside* the try,
  leaving nothing between `ALLOCATOR.allocate` and `try`, with a `finally` that returns the output
  frame and the staging lease exactly once on every exit; the path has no arena at all any more.
  #1408's `closeConfinedWriterTable()` has called `arena.close()` directly since the confined writer
  table was introduced (`c11786241`). What remained was a comment claiming that arena "is released
  by close()/closeLastOwnership() through SharedArenas.close(Arena)" — the misreading #1408's third
  acceptance item asks to remove — and the absence of any lane that runs the invariant under the
  arena strategy a native image uses.
- Why tests missed it: #1407's leaking window has no injectable seam (three JDK calls), and the
  issue's own constraint forbids adding one on the success path; what *was* untested is the release
  that makes the enclosing `finally` correct — dropping the staging-lease close from it leaves 64
  staging buffers lent after 64 recoverable failures and no test noticed. #1408 was invisible by
  construction: `RegionTableTest.confinedWriterTableClosesNativePayloads` passes with the confined
  arena routed through the shared-arena path, because the JVM default strategy closes shared arenas
  for real. Only the strategy a native image resolves at startup can tell the two apart, and no CI
  lane ran it.
- Regression test:
  - `io.sirix.cache.Lz4OwnershipFailureTest.decodeFailureAfterHeapStagingReturnsTheStagingBufferAndTheOutputFrame`
    — heap input (the decoder's source being native proves the staging path ran), 64 injected decode
    failures, then exactly-once accounting: one staging borrow per call, nothing left lent, no stale
    release, 64 frame allocations and 64 frame releases, no live frame, unchanged committed/active
    bytes. Fails with the staging-lease release removed from the `finally`:
    `no staging buffer may stay lent ==> expected: <0> but was: <64>`.
  - `io.sirix.cache.Lz4OwnershipFailureTest.decodeFailureWithoutHeapStagingReturnsTheOutputFrame`
    — the same accounting for a native input, which borrows no staging buffer at all: the window
    before any staging buffer exists.
  - `io.sirix.page.pax.ConfinedWriterArenaStrategyTest` in the new `sharedArenaAutoStrategyTest`
    JVM — asserts the lane really runs `AUTO`, that `SharedArenas.close` leaves a shared arena alive
    there (so the hazard is real), and that 256 create-and-release cycles of a confined writer table
    each reclaim their arena synchronously and retain nothing. With `closeConfinedWriterTable()`
    reverted to `SharedArenas.close(local)` it fails at `cycle 0: close must reclaim the confined
    arena`, while the whole of `RegionTableTest` still passes 12/12 under the default strategy.
  - `io.sirix.io.SharedArenaStartupPolicyTest` gains the `auto-strategy` case: strategy, both
    diagnostic flags, and that `close(Arena)` leaves an auto arena alive and readable.
  - Commands: `./gradlew :sirix-core:sharedArenaAutoStrategyTest` and
    `./gradlew :sirix-core:test --tests 'io.sirix.cache.Lz4OwnershipFailureTest'`.
- Read latency (#1407's fourth acceptance item): nothing on the read path changed — the only edits
  to `FFILz4Compressor` are a comment — so the success path's instruction sequence is unchanged by
  construction and there is no candidate to measure against a control.
- Native-image confirmation (#1408's second acceptance item): the new lane covers the JVM path a
  native image takes, because `SharedArenas` resolves `AUTO` from the image's own marker property.
  `docs/NATIVE_IMAGE.md` records how to confirm it in the image itself with the existing write
  smoke.
- Lesson: a resource whose release goes through a strategy may be released by nothing; prove the
  owner in a lane that runs the strategy that releases least. See `../lessons.d/`
  (the [fragment convention](../../changelog.d/README.md)).
