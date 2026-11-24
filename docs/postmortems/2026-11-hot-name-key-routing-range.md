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

## Review follow-up: NAME read and serialization cost

- Symptom: each long-name candidate constructed a temporary `QNm`, so lookup allocation grew with
  bucket size. Every ordinary serialized component also took a scalar UTF-8 counting pass before
  its existing ASCII check/copy or UTF-8 encoding, despite already being known to fit.
- Root cause: matching consumed materialized name objects rather than dictionary components;
  truncation's counting helper was also used on the complete-name path. The record-read budget
  observes neither temporary object allocation nor redundant character work.
- Correction: exact matching compares URI/local strings; filtered matching prepares component
  lookup maps at the first truncated bucket and shares them across that read's candidate buckets.
  Reads over complete keys never prepare these maps. The candidate loop resolves
  one record, preserves missing-name/URI handling and compares strings without constructing a name.
  Complete URI and local components pass their full character lengths to the unchanged writer;
  serialization and length reporting only count a bounded prefix when truncation is required.
- Regression oracles: FMT-305 and the per-node allocation rule require exact expanded-name
  membership with no temporary name per candidate, prefix ignored. The clean-fork allocation test
  rejects 4096 candidates on exact, include and exclude paths within 1024 bytes per read, with an
  escaping `QNm` loop proving that the meter sees per-candidate names. An independent dictionary
  fixture verifies URI/local differences, prefix aliases, stale cached names, missing records,
  non-name records and missing local/URI dictionary entries, together with one record per candidate.
  Existing serialization boundary, namespace, differential-read and record-budget tests remain the
  behavior oracle for removing the redundant complete-component scans.
- Lesson: match the components already available at a storage boundary, and prepare lookup state
  at read scope; a helper for a rare overflow path must not impose its full traversal on every key.
- Vision check: these corrections preserve result identity and the existing record-read bound while
  removing candidate-sized garbage and redundant encoding work. No default or stored layout changes.
- Verification: focused NAME/serializer tests and the new clean allocation-fork test run after the
  complete edit round; counterfactual mutation validation remains with the outer executor.

## Review follow-up: one encoding decision and no duplicate sizing API

- Symptom: a complete 2000-character ASCII field still took a scalar UTF-8 counting pass before
  its ASCII check/copy, and exact lookup repeated that count after serialization. Namespace
  components had the same cost. The public `serializedLength` helper duplicated the serializer
  even though only test fixtures still called it.
- Root cause: the three-bytes-per-UTF-16-character estimate could prove small names fit but could
  not decide long ASCII names. Moving the count into `isTruncated(QNm)` preserved the traversal.
  A separately computed truncation decision also gave the read path work the encoder had already done.
- Correction: ASCII is checked once and copied by the existing direct writer; a non-ASCII component
  is UTF-8 encoded once, and its produced bytes supply both the length and the bytes copied into
  the key. Only an overflowing encoded component runs the prefix-budget walk. Namespace escaping
  retains its existing NUL accounting and backward expansion. The HOT reader captures the serialized
  marker before lookup, then passes that boolean to component-wise matching; the NAME facade no
  longer applies a second re-check. Candidate caching stays below the exact re-check, so names
  sharing a stored key cannot contaminate each other's cached results.
- Removal: `serializedLength`, the name-based truncation predicate and the character-counting size
  helpers are gone. Fixtures assert actual serialized bytes and lengths. `maxSerializedLength` and
  the caller's buffer-sizing contract are unchanged.
- Evidence: serializer cases compare complete long ASCII, two/three/four-byte UTF-8 and escaped-URI
  inputs with an independently framed JDK UTF-8 oracle. Direct HOT NAME lookup and the NAME facade
  are both compared with the document scan, including shared boundary prefixes and cold cached
  lookups. Earlier boundary, namespace, record-read and allocation tests remain selected.
  Those budgets cannot observe redundant character counting: the traversal argument is the code
  path itself, which has no whole-component UTF-8 size loop and uses encoding output to decide
  truncation. No timing claim is inferred from those budgets.
- Lesson: consume metadata produced by encoding at the read boundary; do not reconstruct it from
  the original value or preserve a second implementation merely for test expectations.
- Vision check: exact revision-local answers, the 4096-byte bound and ordinary-name record-read
  cost stay unchanged; redundant traversal and the duplicate public sizing API are removed.

## Review follow-up: candidate lifetime and the writer read surface

- Symptom: a cold XML dictionary reconstruction could release the candidate page before the
  re-check fetched its URI key. Separately, `HOTIndexWriter.get` returned both nodes from a shared
  marked bucket even though `HOTIndexReader.get` returned exact postings.
- Root cause: a borrowed page-backed record crossed a dictionary call that may itself read records;
  the public exact-read policy was also enforced on one wrapper rather than its writer sibling.
- Correction: the shared matching loop snapshots all three name fields before dictionary access.
  The writer captures the serialized marker before chunk assembly and invokes the same exact
  re-check as the reader. Candidate assembly and caches retain all postings.
- Surface audit: `NameIndex.openIndex` routes single-name requests through exact lookup, filtered
  bucket scans through shared matching, and unfiltered scans through all candidates. `iterator`,
  both `range` overloads, `iteratorFrom` and `iteratorTo` are explicitly serialized-bucket cursors;
  they preserve raw marker metadata for their NAME consumer's residual. Raw trie/physical mutation
  reads likewise have no original-name answer to give. The writer has no other public posting read;
  its `index` returns its supplied mutation input. Long-key readers/writers are PATH APIs, and
  interval/temporal/projection adapters use non-NAME codecs. The full contracts and caller inventory
  are in `HOT_INDEX_SPECIFICATION.md` section 2.3.2. No additional NAME prefix API exists.
- Regression oracles: a real page-backed XML element's native arena is closed by the local-name
  dictionary read; exact, include and exclude matching must still use the original URI, and a direct
  post-close URI getter is the positive witness that the frame is invalid. A real JSON document
  supplies distinct node keys for two long names sharing a marked key; writer and committed reader
  lookups must return only each name's own key, including after an ordinary lookup reuses scratch.
  The record budget now covers writer reads of short, boundary-1 and boundary names at zero and
  shared long names at exactly one read per candidate. Candidate cursor tests exercise all four
  cursor methods, both range overloads and both directions/inclusivities, then apply the shared
  residual against a document-scan oracle before commit and after cold reopen.
- Why tests missed it: the dictionary stub never invalidated its returned record, and the shared
  writer mutation fixture used only short names. The earlier boundary test exercised the reader
  and NAME facade, leaving the supported writer get path outside its long-name fixture.
- Vision check: both fixes restore exact answers under cold reconstruction and writer-side reads
  without changing stored fields, normal-name re-check cost or the candidate-cache contract.

## Review follow-up: multi-level assembly and exact heights

- Symptom: bulk-building 32 balanced five-character prefixes, with discriminators at byte 100
  and tails at byte 300, produces a full height-2 root above height-1 parents. Extending one
  leaf from 200 through 513 entries introduces the distant tail bit. A fold could turn its
  parent into two levels while the root retained height 2; commit did not repair the metadata.
- Root cause: the assembler's output changed from one compound node to a possibly nested tree,
  but integration's preflight checked fold order and capacity without checking the mask union's
  representability. Its publication arm assumes a flat fold, including inside capacity cascades.
- Correction: the shared integration preflight refuses an unnameable mask-plus-bit union at every
  visited level. Existing overflow and branch callers then use the complete structural frontier,
  which consumes actual output heights and re-encodes ancestors before returning. No deferred
  height repair or new propagation mechanism is introduced.
- Caller audit: a search for every production `assembleIndirect` call, followed through the
  enclosing split/add/integrate primitives to their writer callers, finds only bulk construction,
  incremental primitives and the writer's migration/frontier/ancestor rebuilds. The complete
  inventory and each shape argument are recorded in `HOT_INDEX_SPECIFICATION.md` section 2.6.
  Unchanged or reduced masks cannot introduce nesting; branch new-bit unions already have route
  preflight; bulk/frontier paths consume nested shapes. Root/intermediate BiNodes have only one
  bit. Commit folding independently refuses unnameable unions before materialization.
- Regression oracle: section 2.6 requires every stored height to equal its actual distance from
  leaves at successful mutation boundaries. The reported 32-prefix/byte-100/byte-300 sequence
  checks the fixture's full root and spare parent, extends through tail 512, and independently
  walks every height immediately before and after overflow, after commit and after cold reopen.
  A second variant fills the parent with 30 nearby 512-entry branches before overflowing the original
  leaf to cover capacity refusal. Both run the
  structural validator and compare postings with a document scan. The earlier fixture reached
  overflow at a root or a tree without a full ancestor and therefore missed the stale height.
- Vision check: exact live and historical metadata preserves deterministic correct routing and
  subsequent height-based decisions; the change adds only a bounded mask check to the existing
  preflight. There is no new default, persisted field or whole-subtree repair.
- Verification: focused routing, integration, serializer, NAME re-check and allocation checks run
  after the fix round. Counterfactual and mutation validation remain with the outer executor's
  dedicated validation phases.
