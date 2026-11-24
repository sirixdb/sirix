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
  `RouteErrorMappingTest.errorMessageRoundTripsJsonEscapesAndUnicode` round-trips every JSON
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
