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
  (`./gradlew :sirix-rest-api:test --tests 'io.sirix.rest.crud.ReadNodeIdValidationTest'`).
  For JSON and XML, with hashes `NONE` and `ROLLING`: valid key (subtree body, the node's own
  hash as ETag, differing from the whole-resource ETag), unknown keys including negative and
  `Long.MAX_VALUE` (404, error body, no ETag), malformed keys including an overflowing number
  and the empty string (400 naming `nodeId`), a key present in revision 2 only, requested at
  revision 1 and over the range 1..2 (404), the same cases through `query`, and `HEAD`
  agreeing with `GET`. 24 cases; 19 fail before the fix, all pass after.
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

## Not changed

`DELETE` still parses `nodeId` with `toLongOrNull()`. A malformed value therefore reads as
"no nodeId", which on that route deletes the whole resource. That is the same defect class with
a worse outcome, and is reported separately rather than folded into this read-path fix.
