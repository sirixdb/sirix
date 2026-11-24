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

## Review follow-up: truncation identity and build accounting

- Symptom: a complete 4088-byte local name and its extension shared postings, while a filtered
  read needlessly re-checked the complete name. An in-bound 4082-byte URI with local name `a`
  was shortened, and a supplementary character at the namespace cut could hide truncation from
  filtered reads. Recursive assembly also reported fewer indirect pages than it allocated.
- Root cause: a shortened expanded name was indistinguishable from a real complete name; a
  length heuristic could neither separate those representations nor account for UTF-8 width at
  the cut. The fixed four-byte local reservation disagreed with the full-name size predicate.
  Build accounting assumed every assembly produced exactly one indirect page.
- Correction: only over-bound names reserve a trailing marker that cannot end UTF-8. Exact
  reads use the original size decision; filtered walks inspect that marker through the existing
  raw-key entry interface. Namespace reservation uses the first local character's encoded width.
  Every indirect allocation goes through the build's counting allocator, including nested levels.
- Regression oracles: FMT-305 requires whole storage and zero re-check reads for complete names,
  and exact postings for longer names. The JSON fixture now shares the full prefix across
  boundary-1, boundary, boundary+1 and divergent tails; exact and filtered reads equal the document
  scan after build and cold reopen. The namespace fixture exercises build, incremental maintenance,
  rename and historical reopen, including supplementary characters at the cut. Serializer cases
  cover one- through four-byte local characters and escaped namespace NULs. The work budget
  counts only long-name candidates during filtered reads and no boundary-name exact re-checks.
  A four-leaf bulk build requires two inner branch pages and one root, independently of the
  reported count, and compares both reported counts with an independent walk of the returned tree.
- Why tests missed it: the original boundary fixtures used different leading characters for
  complete and truncated names, and the budget had only an ordinary short name beside the long
  control. Neither exercised a namespace ending near the cut or checked nested allocation totals.
- Vision check: exact revision-local results and unchanged ordinary-name re-check cost preserve
  the vision's result and counted-work contracts; the 4096-byte bound and persisted field widths
  stay unchanged. No new default is introduced.
- Verification: the review phase runs the focused serializer, boundary-read, namespace, work-budget
  and bulk-builder tests after all edits; baseline counterfactual and mutation validation remain
  with the outer executor's dedicated validation phases.
