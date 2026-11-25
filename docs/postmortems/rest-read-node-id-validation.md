# Bug: REST reads answered an unknown or malformed `nodeId` with the whole resource

- Fix / issue: [#1387](https://github.com/sirixdb/sirix/issues/1387); REST-203 in
  [the REST chapter](../../spec/REST-api.md#rest-203--serialization-revision-selection).
- Symptom: `GET /db/res?nodeId=abc` and `GET /db/res?nodeId=999999` returned 200 with the
  entire resource. With hashes on, the `ETag` was computed at whatever position the cursor was
  left on, so it named neither the requested node nor the whole document. `openapi.yaml`
  documents 404. A client fetching or paginating a subtree could not tell a bad key from a
  successful full read, and could cache the wrong ETag and later send it as `If-Match`.
  With a `query`, an unknown key surfaced as 500 rather than 404. `HEAD` on a resource
  without hashes answered 200 for any key; with hashes, a non-numeric key was a 400 only by
  accident of `NumberFormatException` extending `IllegalArgumentException`.
- Trigger: any JSON or XML resource; `GET` or `HEAD` with `nodeId` set to a non-integer, to a
  key that does not exist, or to a key that exists only in some of the selected revisions.
- Root cause: three independent gaps on the read path, all violating "a missing node request
  MUST fail rather than serialize another cursor position" (REST-203):
  1. `AbstractGetHandler` parsed the parameter with `toLongOrNull()`, so a malformed value
     became `null`, which means "no nodeId": the whole resource.
  2. Nothing checked that the key existed. `JsonSerializer` and `XmlSerializer` start at the
     document root when they cannot move to their start key, and the ETag helpers called an
     unchecked `moveTo` and read `hash` wherever the cursor stayed.
  3. `moveToOrNotFound`, the helper the query, update and delete paths use, threw
     `IllegalStateException`. `routerFailureHandler` deliberately does not map that type to a
     client error, so a caller error became a 500.
- Why tests missed it: every REST test used valid node keys from known fixtures. No test sent
  a key that was absent or malformed, and none compared a subtree's ETag with the node's own
  hash. The query path's comment described the hazard, but only for the cursor, not for the
  status the helper produced.
- Regression test: `ReadNodeIdValidationTest` in `sirix-rest-api`
  (`./gradlew :sirix-rest-api:offlineTest --tests 'io.sirix.rest.crud.ReadNodeIdValidationTest'`).
  For JSON and XML, with hashes `NONE` and `ROLLING`: valid key (subtree body, the node's own
  hash as ETag, differing from the whole-resource ETag), unknown keys including negative and
  `Long.MAX_VALUE` (404, error body, no ETag), malformed keys including an overflowing number
  and the empty string (400 naming `nodeId`), a key present in revision 2 only, requested at
  revision 1 and over the range 1..2 (404), the same cases through `query`, and `HEAD`
  agreeing with `GET`. 24 cases: 18 fail on the parent commit, all 24 pass with the fix, and the
  whole offline REST suite (222 tests) passes.
- Lesson: see [Lessons](../LESSONS.md): validate request-addressed keys at the boundary, parse
  strictly, and give "not found" an explicit HTTP status rather than a generic exception type.

## Fix

- `nodeId` is parsed with `requireLongParam` before any I/O on `GET` and `HEAD`: 400.
- Before serializing, the GET handler moves a read-only cursor to the key in every selected
  revision; the first failure is a 404, so nothing is serialized and no ETag is written.
- `moveToOrNotFound` throws `HttpException(404)`. This also turns the query path's and the
  update and delete paths' unknown-key 500 into the documented 404. The missing `If-Match`
  half of [#1386](https://github.com/sirixdb/sirix/issues/1386) is unchanged.
- The ETag helpers use `moveToOrNotFound` instead of an unchecked `moveTo`, so they can never
  hash another position even if a future caller skips the pre-check.
- `HEAD` checks the key whether or not the resource keeps hashes.

Cost: one read-only transaction per selected revision for requests that carry `nodeId`, once
per request. Requests without `nodeId` are unchanged.

## Follow-up: DELETE widened a malformed or misplaced `nodeId`

- Symptom: `DELETE /db/res?nodeId=abc` returned 204 and removed the whole resource with its
  history. `DELETE /db?nodeId=2` returned 204 and removed the database; `DELETE /?nodeId=…`
  removed every database. A parameter meant to narrow the delete widened it.
- Root cause: the same lenient `toLongOrNull()` parse in `AbstractDeleteHandler`, where "no
  nodeId" means "delete the resource". The database and server-wide routes never looked at
  `nodeId` at all.
- Fix: `requireSubtreeNodeId` parses strictly (400) and rejects a `nodeId` without a resource in
  the path (400). `DeleteHandler` calls it before any destructive step, including closing open
  databases; `AbstractDeleteHandler` calls it again for direct callers. An unknown key is a 404
  through `moveToOrNotFound`, as above.
- Regression test: `DeleteNodeIdValidationTest` (JSON and XML; malformed, misplaced, unknown and
  existing keys; asserts the database, resource, revision count and node are unchanged after
  every refusal). 4 of 8 cases fail on the parent commit with 204 where 400 is expected; all 8
  pass with the fix, and the offline REST suite (230 tests) passes.

## CI follow-up: the mutation tooling test read the live dispositions file

- Symptom: after the first reviewed dispositions were added to `.github/mutation-dispositions.toml`,
  `Python mutation tooling` failed in
  `test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero` (`1 != 0`). The PR mutation
  lane itself passed.
- Root cause: the test ran `pr_mutation.py run` with the default `--root`, which is the repository
  checkout, so it judged the repository's real dispositions file against an empty selection. With
  no mutated classes every new entry is "unconfirmed" and fails the run. The test only passed
  because that file had held no entries since the lane was introduced.
- Fix: the test passes `--root` set to its temporary directory, so it judges only its own inputs.
- Regression: the same test, which fails on `3dd98238` and passes with the fix; the whole tooling
  suite (156 tests, 99 % branch coverage) passes.
- Lesson: see the isolation lesson in [Lessons](../LESSONS.md).

## Review follow-up: pagination, query validators and error encoding

- Symptom: record pagination ignored `nodeId`, so a first-object request could return its
  sibling while hashing the first object. Queries returned no context-node ETag, and malformed
  parameters containing decoded control characters produced an invalid JSON Error envelope.
- Root cause: the record serializer conflated the resource root and continuation cursor;
  the format-specific query serializers bypassed the resource serializer's ETag handling;
  the shared error boundary escaped only quotes and backslashes by hand.
- Why tests missed it: tests checked node selection without record pagination, checked query
  success without its ETag, and supplied malformed parameters containing only printable text.
- Correction: separate the record subtree root from the continuation cursor, validate cursor
  ancestry in every selected revision before serialization, attach the selected context hash
  after successful query evaluation at the shared query boundary, and serialize Error objects
  with the JSON encoder.
- Regression tests: `ReadNodeIdValidationTest.recordPaginationStaysInsideSelectedSubtree`
  compares parsed HTTP bodies with explicit fixture values for initial, limited, continuation,
  exhausted, historical, metadata and whole-resource pages. It rejects sibling, ancestor,
  missing and revision-partial cursors. `queryReturnsSelectedContextETagForEveryResultMode`
  checks JSON/XML, hashes on/off, ordinary/paginated results, transformed queries, historical
  revisions and query failure. Malformed-key tests on GET and DELETE include decoded controls.
  `RequestFailureHandlingTest.errorMessageRoundTripsJsonEscapesAndUnicode` round-trips every JSON
  control character and non-ASCII text through the actual HTTP failure boundary (REST-209).
- Verification: all 44 focused REST cases and 22 focused core serializer cases passed across
  the focused runs, including the corrected query-syntax and scalar fixtures. The core cases
  include `JsonRecordSubtreeScopeTest` (character and byte outputs), `JsonRecordSerializerTest`
  and `JsonSerializerStartNodeRegressionTest`. Pre-fix execution was not run because this review
  phase requires all fixes before verification. Mutation and broader budget gates remain with
  the outer executor.
- Lesson: selecting a node must constrain every serializer and pagination cursor, and query
  response metadata must be tested independently of body success. Encode protocol strings with
  the protocol serializer. See [LESSONS.md](../LESSONS.md).
- Vision check: preserves node identity and historical scope and fails closed rather than
  returning another subtree; introduces no new default. Existing budget and mutation gates
  remain owned by the outer validation phases.

## Review follow-up: named structural pagination roots

- Symptom: selecting the field `arr` from `{"arr":[1,2,3]}` with a two-record limit returned
  `[1,2]`, dropping its name. Named object roots and their metadata lost their names too.
- Root cause: fused named structural nodes satisfy the cursor's object/array predicates as well
  as its field predicate. Moving record serialization to an arbitrary root exposed those nodes
  to wrapper emission that assumed unnamed containers. Parent metadata made the same assumption.
- Why tests missed it: subtree tests covered unnamed objects and arrays plus scalar fields, but
  no field whose value was an object or array.
- Correction: preserve the named-container wrapper before and after initial-page child emission;
  emit the selected subtree's named parent as `key` at the shared metadata boundary for both
  initial and continuation pages. Reuse the sink's quoted-name path and the existing JSON escape
  function. Whole-resource pagination keeps its existing envelope.
- Regression tests: `JsonRecordSubtreeScopeTest.namedRecordRootsRetainNamesInPlainAndMetadataOutput`
  derives explicit page values from REST-203's examples and checks both character and byte
  output, named object/array/scalar roots, record limits, empty containers, escaped Unicode
  names, every metadata mode, continuation metadata and revision envelopes.
  `ReadNodeIdValidationTest.namedRecordRootsRetainFieldNamesAndETags` exercises the HTTP route
  for those three field types, hashes on/off, all metadata modes and continuation pages.
  These reject unnamed `[1,2]`, unnamed object bodies, or a missing parent `key`.
- Verification: the single focused Gradle run passed all 24 serializer and 32 REST node-selection
  cases, including both new parameterized regressions. No pre-fix test run was made under this
  phase's verification-order constraint. Mutation and broader budget checks remain with the
  outer executor.
- Lesson: a fused node's structural role does not replace its field identity; test named and
  unnamed roots independently in every wire representation. See [LESSONS.md](../LESSONS.md).
- Vision check: retains the selected node's identity and historical result, with no new default.

## Review follow-up: valid node kinds across every read branch

- Symptom: `GET ?nodeId=0&query=1` failed with 500 for a JSON document that passed existence
  validation. XML attributes and namespaces also failed on serialization, and unhashed HEADs
  returned an empty ETag header instead of omitting it.
- Root cause: existence validation accepted every stored kind, but JSON query binding assumed
  every cursor denoted a JSON value. The item factory had no document item, and the REST handler
  duplicated collection registration in a list of concrete value classes. XML serialization
  assumed every result was directly representable as standalone XML; the query serializer
  explicitly rejected attributes. HEAD conflated an absent ETag with an empty string. The query
  paginator assumed every `Sequence.iterate()` enumerated query results, but JSON arrays iterate
  members; its serializer also assumed at least one item would be written before close. Comment
  delimiter constants contained presentation spaces inside the comment value.
- The complete matrix additionally exposed array members being mistaken for query-result items,
  invalid JSON when no result was serialized, and comment delimiters padding the stored value.
  The first focused run failed on those observable cases; collecting assertions across requests
  identified all array kinds affected rather than stopping at the first failing target.
- Correction: represent JSON document contexts with their own stored item anchored at key 0,
  register the query collection once for all item kinds, emit attributes/namespaces through
  explicit REST XML representations at the shared serializer, and return no HEAD ETag without
  hashing. Non-REST XML serialization retains its restrictions. Treat every Item as one result
  at the pagination boundary, initialize an empty JSON envelope on close, and remove comment
  delimiter padding at the shared XML boundary.
- Regression: `ReadNodeKindMatrixTest` derives exact subtree values from REST-203 and worked JSON
  and XML fixtures. It exercises document roots, named and unnamed containers, all named and
  unnamed scalar kinds (including integer, long, decimal and large-integer values), and XML
  elements, attributes, namespaces, text, comments and processing instructions. Attributes and
  namespaces cover both prefixed and unprefixed forms. Every target is read through ordinary GET,
  identity/constant/node-key queries, paginated versions of those queries, and HEAD with hashes
  on/off; JSON also covers record pagination. The node-key query rejects substitution of child 1
  for document 0. Assertions check parsed public bodies, unchanged subtree scope and the hash
  independently read from the selected stored node; XML comparisons discard only REST metadata
  and formatting whitespace, retaining expanded names and user attributes.
- Verification: focused Gradle runs passed the complete node-kind matrix, existing read-selection
  tests, query pagination lifecycle tests, JSON/XML query serializer tests, and XML/SAX serializer
  and escaping tests. The matrix executes all requests before collecting assertion failures, so
  every node-kind/branch combination runs even when another combination fails. Initial runs
  reproduced the comment-padding and array-pagination defects; the final matrix passes after
  their fixes, including valid empty windows. Mutation and broader budget gates remain with the
  outer executor.
- Lesson: validation and every downstream consumer must accept the same node-kind domain. A
  stored document identity cannot be substituted with its JSON payload. See [LESSONS.md](../LESSONS.md).
- Vision check: preserves identity, exact historical scope and fail-closed behavior; no stored
  format or default setting changes.

## Review follow-up: empty document revisions

- Symptom: deleting the only top-level JSON node leaves document key 0 readable, but GET returned
  an empty payload; identity queries and multi-revision record responses contained `"revision":`
  without a JSON value.
- Root cause: the item factory correctly preserved document identity after R5, while serializers
  still assumed every valid document root had a payload child. The unlimited emitter skipped
  document nodes, the record emitter returned without writing a value, and the limited emitter
  continued without finishing the already-open revision wrapper. The earlier node-kind matrix
  covered populated documents but omitted the populated-to-empty state transition.
- Correction: all three serialization boundaries emit the agreed JSON `null` for an empty
  document. The limited path also closes that revision before continuing. This applies before
  REST buffering and header generation, so direct reads, query items, record pages, metadata
  options and revision histories share the same rule without response-body fallbacks.
- Regression: `ReadNodeKindMatrixTest.deletingOnlyTopLevelNodeLeavesReadableNullDocument` creates
  `{"a":1}`, deletes its top-level object through HTTP, verifies revision 2 retains key 0 and
  no payload child, then checks plain/limited/record reads, ordinary and paginated identity
  queries, historical revision 1, mixed revision envelopes, ETags on/off, HEAD, and rejection
  of the deleted key. Null document items remain distinct from an excluded query-result window.
  `JsonEmptyDocumentSerializationTest.emptyRevisionsEmitNullInEveryOutputMode` independently
  asserts literal `null` and a present null revision field across character/byte output, all
  metadata modes, all limit delegation options, record serializers, direct limited query
  wrappers and populated-empty-populated histories. The oracle is REST-203's explicit empty
  document rule; empty output and an absent revision value are wrong behaviors these tests reject.
- Verification: focused runs passed 60 existing read/serializer cases and all four new
  parameterized empty-document cases. The new history fixtures use finite depth and node limits
  that include their populated payloads, so those payloads have an independent unchanged-value
  oracle. Pre-fix execution was omitted under this review phase's ordering rule. Mutation and
  broader budget checks remain with the outer executor.
- Lesson: cover valid node kinds across lifecycle states, especially deletion of the last child;
  every opened revision wrapper still requires a value and a closing delimiter. See
  [LESSONS.md](../LESSONS.md).
- Vision check: keeps committed empty and historical revisions readable without substituting a
  different node or changing storage format; the explicit null representation follows the
  captain's decision for this review.

## CI follow-up: serializer contracts and mutation selection

- Symptom: Linux, macOS, ARM and Windows core lanes reported the same five failures. The
  shared XML fixture still expected spaces around the stored comment `foo`, and both empty
  transitions in the copy/replay fixture still expected an empty string.
- Invariant: serialized comments preserve stored text, and every empty JSON revision emits
  `null`, including the first edited revision, repeated empty commits, copied histories and
  replayed histories. REST-203 and the accepted R5/R6 decisions supply these oracles.
- Root cause: the earlier fixes updated production serialization and focused tests but omitted
  these existing consumers of the same output contracts. Update the single XML expected-document
  fixture and both empty-state expectations; retain all allocation-identity and replay assertions.
- Regression evidence: the focused pre-change Gradle run reproduced exactly the five CI failures
  (`PICommentTest.testPI` and the four empty cases of
  `JsonBulkInsertDiffRegressionTest.initialRevisionCopyPreservesEditedAllocationIdentity`).
- Mutation invariant: selected tests must execute each changed public behavior without depending
  on a service the mutation lane does not start. The broad package fallback for
  `RequestFailureHandling` selected Keycloak integration suites; name its existing HTTP error
  regression `RequestFailureHandlingTest` so the real selector chooses it directly. Core XML
  tests now cover standalone attributes/namespaces in prefixed/default and REST/non-REST forms;
  query tests cover the same representations, populated/empty document contexts and closing an
  unused result serializer. Associate document-context tests with the item factory as well.
- Record-root tests assert the builder's documented refusal of negative and absent keys without
  output, followed by ordinary root serialization, in both byte and character output. Remove the
  redundant metadata root-key predicate: after document unwrapping its payload cannot be an
  object field. Remove the document constructor's duplicate kind assertion: its sole caller is
  the factory's `JSON_DOCUMENT` branch. Neither removal changes admitted inputs or results.
- Trace: valid record roots still move to the selected node before emitting their existing
  wrappers; invalid roots fail before output. Document roots still unwrap once, and named
  field roots still emit `key`. Factory dispatch still constructs a document only for key 0;
  the item repositions its cursor to that identity before serialization, for empty and populated
  revisions alike. Error tests still issue HTTP requests against the real failure handler.
- External compatibility conflict: the separate `sirix-python-client` checkout unconditionally
  indexes HEAD `etag` for hash-disabled resources. Its nine failures conflict with the accepted
  R5 decision and REST-203 rule that hash-disabled HEAD omits ETag. This repository cannot fix
  that client's code, and restoring a synthetic ETag would undo the recorded decision.
- The final REST mutation pass also exposed the absent-parameter sibling: DELETE without
  `nodeId` must retain resource, database and global delete scopes. The new
  `DeleteNodeIdValidationTest.absentNodeIdPreservesResourceDatabaseAndGlobalDeleteScopes`
  checks status and real persisted removal at each scope for JSON and XML.
- Reader ownership is observable cost, not an equivalent close-time change. The read suite now
  decorates the real engine session at the REST/database boundary and records its public
  `activeTrxCount()` **before** parent cleanup. Its independent zero-reader oracle covers GET,
  queries, HEAD, revision preflight and record cursor validation, successes and refusals, in
  both formats/hash modes. Responses still come from the real handlers and serializers; no
  expected results come from mocked storage. The final PIT run kills all three removed-close
  mutations, including the formerly dispositioned HEAD close; remove that obsolete disposition.
- Four additional dispositions identify individual Kotlin null assertions on transactions whose
  API guarantees a non-null reader or throws. Review their exact identities and producer
  invariants in `.github/mutation-dispositions.toml`; update the existing HEAD session assertion's
  instruction identity. No cleanup/cost mutation is accepted as equivalent. The local checker
  also needs the unchanged CLI classes compiled to validate its pre-existing Backup entries;
  CI already restores these from the all-module Build job.
- Verification: the core Work budgets command from `docs/VERIFICATION.md`, plus the focused
  serializers, original regressions and `JsonModelBasedOracleTest`, passed 394 main-task cases
  and their allocation/startup/epoch forks with `-PtestHeapMin=512m -PtestHeapMax=3g`.
  The default workstation-sized heap was killed with exit 137 in a prior broad run; no bound
  or test case was relaxed. Query work budgets plus serializer/document tests passed 350 cases;
  the MCP Work budgets command passed all 3 selected cases. The broader REST run passed 52
  cases; after adding the reader probe and absent-parameter cases, this focused command passed
  all 42 read/delete cases:

  ```bash
  ./gradlew :sirix-rest-api:offlineTest \
    --tests 'io.sirix.rest.crud.ReadNodeIdValidationTest' \
    --tests 'io.sirix.rest.crud.DeleteNodeIdValidationTest' \
    -PtestHeapMin=512m -PtestHeapMax=3g
  ```

- Complete changed-line PIT results: core 38 killed / 38; query 11 killed / 11; REST 49 killed,
  1 timeout-detected and 9 individually reviewed equivalents / 59. No uncovered or unaccepted
  changed-line mutant remains. The REST-only helper rerun exited zero, and
  `pr_mutation.py dispositions --check` passed with all three complete reports and censuses.
  The mutation-tooling Python suite passed all 266 tests. Local command logs and reports are
  retained under the ignored `build/ci-phase-evidence/` directory.
- Vision check: keeps historical output exact and the mutation evidence honest, without changing
  the storage format, hash default, cost bounds or the accepted HTTP contracts.
