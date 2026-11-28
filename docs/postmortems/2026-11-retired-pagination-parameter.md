# Bug: retired pagination parameter ignored silently

- Fix / issue: [#1385](https://github.com/sirixdb/sirix/issues/1385); REST-213 in `spec/REST-api.md`.
- Symptom: a client sending `lastTopLevelNodeKey` with `nextTopLevelNodes` received the first page on
  every request. Pagination never advanced and no error told the client why; a loop that stops on an
  empty page never terminated.
- Trigger: `GET /{database}/{resource}?nextTopLevelNodes=N&lastTopLevelNodeKey=K` against any server
  after the parameter was renamed to `startNodeKey`; the released Python, TypeScript and Kotlin
  clients, the published contract and the website documentation all still used the old name.
- Root cause: a rename inside a commit about something else changed the one `queryParam` read and
  nothing else. Query parameters the handler does not know are ignored by design, so the old name
  turned into "no cursor" instead of a failure. Nothing tied the parameter's name to the clients that
  send it.
- Why tests missed it: every pagination test used the new name, so no test could notice that the old
  one stopped working; the schema carried no statement about the retired name, and the clients live in
  other repositories with their own suites that never ran against the server.
- Regression test: `RetiredPaginationParameterTest` (offline, `./gradlew :sirix-rest-api:offlineTest --tests
  'io.sirix.rest.crud.json.RetiredPaginationParameterTest'`). Pages a 20-element array through
  `startNodeKey`, and asserts the old name gets 400 naming `startNodeKey` alone, with the new name,
  with any value and without pagination. The refusal tests fail on the previous head (200, first
  page).
- Mutation evidence: PR #1513's complete PIT report in run `46130158907` killed all four
  behavioral mutants. The two survivors remove Kotlin-generated assertions that the
  `queryParam` result is non-null, at the retired-name guard and the new-name cursor read.
  Vert.x's [queryParam contract](https://vertx.io/docs/apidocs/io/vertx/ext/web/RoutingContext.html#queryParam(java.lang.String))
  returns a list even for an absent name, so neither assertion can fail for an admitted
  request. The initial change omitted dispositions for these equivalent mutants, leaving
  the mutation check red despite the behavior tests detecting every behavioral change.
  `.github/mutation-dispositions.toml` now records both exact identities and their contract
  arguments; `pr_mutation.py dispositions --check` judges them against that complete report
  and census. Keep the behavior tests' request oracles rather than adding invalid null-returning
  contexts solely to distinguish compiler assertions.
- Lesson: renaming a request parameter is a contract change; the same change either keeps the old name
  with a deprecation or refuses it loudly. An ignored-by-design input must never be the failure mode
  of a rename ([lesson](../LESSONS.md)).
