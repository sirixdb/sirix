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
- Why tests missed it: #1407's tests injected decoder failures only after staging succeeded, leaving
  staging-allocation failure untested. A fresh process with a bounded allocator can reject an
  oversized heap-staging request after the output frame has been allocated, without adding a seam
  to the read path. In both decoder modes, 64 such failures must return 64 output frames exactly
  once, leave scratch ownership and committed/active bytes unchanged, and permit a following valid
  read. The existing post-staging decode failures additionally require every acquired staging lease
  to return exactly once. #1408 was invisible by
  construction: `RegionTableTest.confinedWriterTableClosesNativePayloads` passes with the confined
  arena routed through the shared-arena path, because the JVM default strategy closes shared arenas
  for real. AUTO and GLOBAL expose the difference, but neither had confined-owner coverage.
- Regression test:
  - `io.sirix.cache.Lz4OwnershipFailureTest.stagingAllocationFailureReturnsTheOutputFrameWithoutRetainingNativeMemory`
    — a positive 50000-byte output size and a heap input larger than the fork's 16 MiB allocator
    budget force the staging allocation itself to throw. Both `lz4OwnershipSafeTest` and
    `lz4OwnershipFastTest` run 64 failures with unchanged native memory, zero staging acquisitions
    and exactly-once output-frame returns, followed by a valid round trip. The counterfactual for
    each arm removes `ALLOCATOR.release(buffer)` from `decompressScoped`'s compressed-path
    `finally` (line 640). Both arms rejected that missing release on the first failure:
    `a refused staging borrow must return its frame ==> expected: <0> but was: <1>`.
  - `io.sirix.cache.Lz4OwnershipFailureTest.decodeFailureAfterHeapStagingReturnsTheStagingBufferAndTheOutputFrame`
    — heap input (the decoder's source being native proves the staging path ran), 64 injected decode
    failures, then exactly-once accounting: one staging borrow per call, nothing left lent, no stale
    release, 64 frame allocations and 64 frame releases, no live frame, unchanged committed/active
    bytes. Fails with the staging-lease release removed from the `finally`:
    `no staging buffer may stay lent ==> expected: <0> but was: <64>`.
  - `io.sirix.cache.Lz4OwnershipFailureTest.decodeFailureWithoutHeapStagingReturnsTheOutputFrame`
    — the same accounting for a native input, which borrows no staging buffer at all: the window
    before any staging buffer exists.
  - `io.sirix.page.pax.ConfinedWriterArenaStrategyTest` in the SHARED, AUTO, diagnostic GLOBAL and
    diagnostic skip-close JVMs — asserts each lane's strategy, that `SharedArenas.close` leaves a
    shared arena alive under AUTO and GLOBAL, and that 256 create-and-release cycles of a confined
    writer table each reclaim their arena synchronously and retain nothing. With `closeConfinedWriterTable()`
    reverted to `SharedArenas.close(local)` it fails at `cycle 0: close must reclaim the confined
    arena`, while the whole of `RegionTableTest` still passes 12/12 under the default strategy.
  - `io.sirix.io.SharedArenaStartupPolicyTest` covers SHARED and AUTO startup, including both
    diagnostic flags and observable shared-arena closure (or continued readability under AUTO).
  - Commands: `./gradlew :sirix-core:lz4OwnershipSafeTest :sirix-core:lz4OwnershipFastTest
    :sirix-core:sharedArenaSharedStrategyTest :sirix-core:sharedArenaAutoStrategyTest
    :sirix-core:sharedArenaDiagnosticGlobalTest :sirix-core:sharedArenaDiagnosticSkipTest
    :sirix-core:confinedWriterArenaLeakCheck -PtestHeapMin=512m -PtestHeapMax=2g --max-workers=1
    --no-daemon --console=plain -Dorg.gradle.jvmargs=-Xmx2g --continue`.
  - Focused run (2026-11-25): 17 tests passed, two existing strategy-specific generation tests
    skipped; no ownership test skipped. Both LZ4 forks passed all four tests. The AUTO harness
    reported `cycles=4096 payloadBytesPerCycle=65536 allocatedBytes=268697600
    retainedPayloadBytes=0`. The missing-frame-release control failed in both decoder modes;
    routing the confined close through `SharedArenas.close(local)` failed in AUTO and GLOBAL at
    cycle 0, and the harness rejected it with `cycle 0: confined arena retained after release`.
    Each control exited 1; the restored-source focused run exited 0. These controls verify the
    resource-lifetime oracle, without changing production logic in the delivered change.
- Read latency (#1407's fourth acceptance item): nothing on the read path changed — the only edits
  to `FFILz4Compressor` are a comment — so the success path's instruction sequence is unchanged by
  construction. The native-build read-latency comparison against a control and its verdict must
  still be reported in the pull request; unchanged production logic alone does not replace that
  measurement.
- Native-image configuration (#1408's second acceptance item): see the
  [arena lifetime harness](../NATIVE_IMAGE.md#arena-blocker--resolved-iosirixiosharedarenas) for the
  policy, accounting contract and command. This harness exercises the native-image policy on the
  JVM; it does not execute a native image.
- Lesson: a resource whose release goes through a strategy may be released by nothing; prove the
  owner in a lane that runs the strategy that releases least. See `../lessons.d/`
  (the [fragment convention](../../changelog.d/README.md)).
