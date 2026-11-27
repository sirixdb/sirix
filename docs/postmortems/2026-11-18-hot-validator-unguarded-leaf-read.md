# Bug: the HOT structural validator read leaf keys from frames it did not hold

- Fix / issue: PR #1454, in its own commit; https://github.com/sirixdb/sirix/issues/1458. The defect is older than that PR: the
  validator, the writer and the failing test are byte-identical to its base `f4e081c7`.
- Symptom: a commit under cache pressure ends with `IllegalStateException: HOT published structural
  splice is malformed (first: I5-leaf-constancy — child[4] stored partial 0x21 is not a subset of
  dense PEXT 0x20 of subtree key 800400000000)`, or the same from the derived-index maintainer,
  which then marks the resource's derived maintenance poisoned. I8 and I12 variants exist. The trie
  and the stored bytes are sound: reading the same leaf again finds nothing wrong. Expected: the
  defense-in-depth validation after a structural publication never fails on a sound trie.
- Trigger: `BitemporalStorageEpochEvictionTest` (record and fragment cache budgets of 1 MiB) on the
  `macos-latest / query` lane of workflow `44205724463`, and once before on `windows-latest / query`
  in workflow `44156426448`. On Linux, 2 of 20 runs of the unchanged test logged the macOS
  background message byte for byte when the JVM had no native LZ4, no bundled codec library,
  128-bit vectors and three CPUs. With `FrameSlotAllocator.setPoisonOnReleaseForTesting(true)`
  6 of 6 runs logged it, with the raw leaf writer as well as the encoded one.
- Root cause: `HOTMalformedSubtreeDetector` checked `isClosed()` once when it resolved a leaf and
  then read `getKey`, `getFirstKey` and the last key from the leaf's off-heap slot memory without a
  guard and without the stamp protocol. `AbstractHOTIndexWriter` gives it the one-argument
  `resolveHOTPageForTraversal`, which returns a cache-backed leaf unguarded. The source resource
  and its `$derived` resource share one global page cache, so each writer thread evicts the other's
  leaves; `HOTLeafPage.close()` releases the frame as soon as the guard count is zero, the allocator
  hands the frame out again, and the detector reads another page's bytes as keys. Broken invariant:
  a reader of leaf slot memory holds the leaf, or validates a stamp and discards the read
  (`docs/HOT_INDEX_SPECIFICATION.md` §4.9.2 to §4.9.4). The leaf was closed during the read 8 to 15
  times in every measured Linux run; it surfaces only when the frame is reused before the read ends.
- Why PR #1454 exposed it: not established by measurement. Encoded leaves take frames sized to
  their payload instead of 64 KiB, and the codec's scratch leases come from the same size classes,
  so a released frame is plausibly reused sooner. In the 40 most recent CI runs on main, the one
  failure of the `macos-latest / query` lane was a different test.
- Why tests missed it: the detector's tests resolve swizzled pages that nothing evicts.
  `HOTStructuralChildEvictionTest` evicts on the writing thread at one hook, between child
  resolution and integration, not during validation. `BitemporalStorageEpochEvictionTest` has the
  right interleaving, but a released frame keeps its bytes until it is reused, so the read usually
  returns the old, correct keys; and the test does not assert on the derived-index maintainer, so
  its bounded case passed in the failing CI run while that thread logged the error.
- Regression test: `HOTMalformedSubtreeDetectorTests`, run with `./gradlew
  :sirix-core:test --tests io.sirix.index.hot.HOTMalformedSubtreeDetectorTests`. A hook
  the detector calls on each retained leaf evicts that leaf between "retained" and "read", with
  released frames zero-filled; the fixture's leaves share a prefix with their sibling, so a zeroed
  leaf reads as a bare prefix. With the guard removed and the hook kept, 8 of its 13 cases fail:
  eviction at leaf reads 0, 3 and 5 reports a sound two-leaf trie malformed (I8 and I5), the three
  real defects (I5, I12, and I5 below an indirect child) are misreported, an unretainable leaf is
  judged, and the concurrent evictor case fails on its first walk. With the fix all 13 pass, and the
  unchanged eviction test logged no malformed-splice error in 30 runs with zero-fill and 20 without.
  All 29 PIT mutants on the changed lines are killed.
- Correction: the detector takes a guard before it reads a leaf's keys and releases it in a
  `finally`, so it pins one leaf at a time. A leaf that is closed or being torn down when the guard
  is asked for is resolved again, at most eight times, and then skipped like an unresolvable page.
  No invariant check changed.
- Not fixed here: the HOT writer reads sibling leaves through the same unguarded resolver at about
  30 other sites, two of which copy every key and value of such a leaf into new leaves; and the
  validation step's size pre-check reports a leaf evicted since resolution as
  `UNRESOLVABLE_REFERENCE`. Neither was seen to misread in instrumented runs. They need a
  writer-wide rule for how long a resolved leaf is held and are tracked in https://github.com/sirixdb/sirix/issues/1458. The query and
  read side was checked and holds a guard or validates a stamp at every site found.
- Lesson: [a closed check is not a lifetime](../LESSONS.md#a-closed-check-is-not-a-lifetime).
