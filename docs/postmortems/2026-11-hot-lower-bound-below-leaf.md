# Bug: a HOT lower bound for an absent key could land in a later leaf

- Fix / issue: [#1427](https://github.com/sirixdb/sirix/issues/1427). Introduced by
  [ef977de78](https://github.com/sirixdb/sirix/commit/ef977de78c887970de3d6b7599cc20b103a80b15)
  (2026-05-06); present in the tags `sirix-1.0.0-alpha6` to `sirix-1.0.0-beta7`.
- Symptom: `HOTTrieReader.lowerBound` and `upperBound` returned a position past the true bound for
  some absent search keys, so every consumer that only moves forward from the seek lost the keys in
  between, without an error. A CAS `xs:string` index over the titles `b0000` to `b1999` answered
  `title >= "a9999"` with 200 of the 2,000 titles, starting at `b1800`; 53 of 77 tried bounds were
  wrong through the range filter, the `GREATER_OR_EQUAL` filter and the two-sided range filter alike.
  Every `HOTRangeCursor` starts from this seek, and so do the logical chunk walk and the writer's
  delta-slot re-walk; besides the index readers, range cursors are opened by `HOTIndexWriter`,
  `HOTLongIndexWriter`, `HotOrderedStore`, `ProjectionIndexHOTStorage`, `ChangeLogStorage`,
  `TemporalAggregateStorage`, `TemporalAggregateSummaryLog` and `DerivedRevisionMapping`. The
  defect is in the read path: it never damages a trie, and a fixed reader answers correctly over what
  is stored. Which of these consumers pass bounds with this geometry, and whether a write path ever
  acted on an incomplete scan, was not audited. A second,
  louder symptom on the same line of reasoning: a search key equal to a leaf's only key followed by
  `0x00` bytes threw `IllegalStateException: msdb of equal keys`.
- Trigger: one-byte keys, a BiNode on bit 6 over the leaves `{18, 19}` and `{1a, 1b}`.
  `lowerBound(12)` returned `1a`; `range(12, END)` returned `[1a 1b]`; `range(12, 19)` returned
  nothing. 12 of the 256 one-byte probes were wrong. No key distribution is exempt: over consecutive
  8-byte integers starting at `2e72ba0184ce0000`, `lowerBound(2e72ba0184cdffff)`, the key just below
  the first one, returned `2e72ba0184ce0140`. Clustered keys make it frequent: on bulk-built tries,
  10 to 31 % of the probes made by flipping one bit of a stored key landed late.
- Root cause: PEXT routing reads only the discriminative bits of the nodes on the path. An absent key
  that differs from the stored keys in a more significant bit no node discriminates on is routed by
  its lower bits, possibly into the middle of a subtree it sorts entirely below. The seek's fast path
  returned `(leaf, insertionPoint)` whenever `insertionPoint < entryCount`, which is sound for a
  position strictly inside the leaf, whose neighbours on both sides are entries of that leaf, and
  unsound for position 0: that the key sorts below the routed leaf says nothing about the leaves
  before it. The code comment and the specification both described the fast path as "strictly
  inside"; the condition did not say so. The mirrored case, a key routed into a leaf it sorts
  entirely above, already took the walk to the affected subtree, which is why only one direction was
  wrong. The exception had the same shape: the walk asked for the bit that separates the search key
  from the leaf's first key without first establishing that one exists.
- Why tests missed it: `HOTTrieReaderPextSeekTest` probed stored keys, the empty key, the last key
  followed by a zero byte and one gap key per leaf boundary, each of which is routed to the leaf it
  belongs next to. `HOTStructuralPropertyTest` used `SearchMode.EQUAL` and one unbounded scan. No
  test compared a seek for an arbitrary absent key with an independent sorted reference, and no
  generator produced a key that differs from the stored keys only in a bit the trie does not
  discriminate on.
- Regression test: `HOTTrieReaderBoundContractTest` (the four-key trie and further hand-built
  geometries, each checked against every one- and two-byte probe, plus torn reads and unresolved
  siblings), `HOTTrieReaderCasRangeRegressionTest` (the CAS index above, 77 bounds, five filter
  forms) and `HOTTrieReaderBoundPropertyTest` (fixed seeds of `HOTTrieReaderBoundOracle`:
  `lowerBound`, `upperBound` and `range` against a sorted reference over bulk-built and writer-built
  tries). With `HOTTrieReader.java` swapped back to `650263ffe`, 21 of the 23 invocations of these
  classes fail: wrong bounds (`lowerBound(12)` on the leaf of `1a`), dropped rows
  (`title >= "a9999"`) and the exception for zero-continued keys. The two that pass guard behaviour
  the old walk already had: the sibling comparison at bit 0 and the report of a read that fails on
  stable bytes. With the fix all pass. Each test asserts that its probes have the geometry it claims
  (routed below or above the leaf, bound in an earlier or later one), so a change of leaf boundaries
  cannot leave it green without reaching the case; the fixed seeds reach the defect's geometry with
  70,779 of their 807,233 probes.
- Lesson: a seek must be compared with a sorted reference for keys that are absent in every way a key
  can be absent, including a stored key with one bit flipped, and a condition has to state the case
  its comment argues for. See [Lessons](../LESSONS.md).

Run the regressions with `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTTrieReaderBound*'
--tests io.sirix.index.hot.HOTTrieReaderCasRangeRegressionTest`. Fresh seeds are explored by
`io.sirix.property.HotBoundSeekOracleRandomTest`.

## What the fix does

The seek contract, corrected walk and sibling-resolution work are specified in the
[HOT specification §4.3](../HOT_INDEX_SPECIFICATION.md#43-lower-and-upper-bound).

## Not changed

`DiscriminativeBitComputer.computeDifferingBit` reports `minLen * 8` for two keys of which one
extends the other, which is exact only when the extension continues with a 1 bit. CAS and VALIDTIME
delta slots guarantee that; NAME keys do not (specification §7.2 item 1). The walk compares sibling
first keys with it, so the lower-bound contract is stated for the key model of §2.5 and that open
question is unchanged.
