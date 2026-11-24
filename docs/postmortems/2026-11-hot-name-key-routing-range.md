# Bug: NAME keys whose discriminative bytes exceed the routing range

- Fix / issue: [#1391](https://github.com/sirixdb/sirix/issues/1391); branch
  `fm/sirix-hot-long-name-keys`
- Symptom: a NAME index built over a family of field names that share a long head and differ again
  far past it came out malformed. The structural validator reported 405 violations on a 601-name
  fixture, 35 of them `I6-pext-routes-to-leaf` ("PEXT descent for stored key … landed in leaf 2
  which does not contain it"), and inserting one more member of the family then failed with
  `IllegalStateException: HOT could not construct an invariant-clean incremental frontier for an
  otherwise valid branch insert`. Reached one name at a time instead — the index created before the
  document — plain shredding of the same family failed on the writer's own published-splice
  validation. Point reads happened to stay correct, because the NAME chunk walk seeks through
  `HOTTrieReader.lowerBound` and corrects by key comparison rather than trusting the routing
  descent; that is one read route's accident, not a guarantee.
- Trigger: 600 JSON field names of the shape `"x"*250 + d + "y"*63 + ddd`, `d` over two values and
  `ddd` over 300, plus one short name. The family's discriminative bytes are 250 and 314 to 316, so
  a node routing on both spans more than one eight-byte window and has to name byte 314.
- Root cause: HOT names a discriminative key byte in one of two persisted widths. `SINGLE_MASK`
  persists one base byte position and an eight-byte window mask; `MULTI_MASK` persists one
  extraction position per discriminative byte **in a single byte**. `HOTBulkBuilder.assembleIndirect`
  chose `MULTI_MASK` for any span of eight bytes or more and then wrote
  `extractionPositions[idx] = (byte) bytePos`, so a byte position above 255 was reduced modulo 256:
  the node routed on key byte 58 instead of 314, inside the head every name of the family shares.
  CAS and VALIDTIME never meet that geometry, because they truncate their encoded value to
  `MAX_STRING_VALUE_BYTES` (inside 256 bytes), and the guard that refused an out-of-range bit was
  written for those two kinds only. NAME keys are serialized names of unbounded length and had no
  bound at all, so both the extraction position and — for a family sharing a 4990-byte head — the
  node's signed 16-bit leading-bit field overflowed silently (bit 39925 stored as -25611).
- Why tests missed it: the NAME suite already covered long names. It covered the two shapes that do
  not reach the defect: one long name (a single key needs no routing node), and 513 names sharing a
  300-byte head and differing in the last three bytes — whose discriminative bytes lie inside one
  eight-byte window, so the node is `SINGLE_MASK`, whose base position is an `int` in memory and
  unsigned 16-bit on the wire. No fixture put two discriminative regions far apart, which is what
  forces the `MULTI_MASK` choice. The structural validator would have caught it immediately, and
  `I6` did once the fixture existed; nothing asked it about a NAME index over long names. The
  property test generates names, but short ones.
- Regression test: `NameIndexLongSharedPrefixRoutingTest` — the bulk build, the same family
  maintained incrementally, and names at and past the routable bound, each asserting that the index
  read equals a document scan, that the HOT invariants hold, and that it all survives a cold reopen.
  `NameKeySerializerRoutableBoundTest` pins the bound and the truncation rules;
  `NameIndexRecheckWorkBudgetTest` pins that a name inside the bound reads no record to re-check it,
  with the long-name control as its nonzero floor. All three fail on the parent commit: the first on
  405 invariant violations, the others because neither the bound nor the re-check exists there.
  Run: `./gradlew :sirix-core:test --tests 'io.sirix.index.name.NameIndexLongSharedPrefixRoutingTest'
  --tests 'io.sirix.index.hot.NameKeySerializerRoutableBoundTest'
  --tests 'io.sirix.budget.NameIndexRecheckWorkBudgetTest'`.
- Lesson: see [LESSONS.md](../LESSONS.md) — a field whose width the key length can exceed is a
  bound the format owes a statement, and a layout that cannot name a position must split over levels
  rather than reduce the position (see the [fragment convention](../../changelog.d/README.md)).
