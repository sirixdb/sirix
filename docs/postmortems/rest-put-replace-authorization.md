# Bug: REST PUT replaced an existing resource with only the CREATE role

- Fix / issue: [#1477](https://github.com/sirixdb/sirix/issues/1477), found while fixing #1384.
- Symptom: `PUT /:database/:resource` onto an existing resource removed and recreated it,
  discarding its whole history, for any caller holding CREATE.
  Query store/load functions now refuse taken names unconditionally with `SIRIXDBF0006`;
  resource replacement remains a REST PUT operation.
- Trigger: a caller with CREATE but without MODIFY or DELETE PUTs a resource name that exists.
- Root cause: the route authorizes one role per HTTP method, and PUT means both "create" and
  "replace". `createAndWriteResource` learned which case it was in only after the route's
  check, when `createResource` reported the name taken, and then removed the resource without a
  further check. REST-210 specified replace semantics (new history, new incarnation) but not who may
  replace.
- Fix: REST-210 states that replacing needs CREATE + MODIFY + DELETE, checked before removal, and
  query store/load functions instead refuse taken names without writes. `AbstractCreateHandler.createAndWriteResource`
  checks MODIFY and DELETE on the database once `createResource` reports the name taken, before
  `removeResource`. A refused PUT answers 403 and leaves the resource unchanged. Creating a new
  resource still needs only CREATE. The decision is a plain function, so it is tested offline.
- Why tests missed it: REST create handlers had no offline tests, and the Docker-backed suites PUT
  as an admin holding every role.
- Regression test: `CreateOrReplaceResourceAuthorizationTest` (offline) covers CREATE alone,
  CREATE+MODIFY, CREATE+DELETE and database-scoped roles. Each is refused with 403 and the
  two-revision resource is unchanged. With all three roles the replacement starts a new history,
  and a missing resource is created with CREATE alone. Before the checks, all four refusal cases
  failed: the resource was replaced.
- Review follow-up: per-call lifecycle holds left authorization stale between creation and
  shredding, and XML authorization refusal occurred outside upload cleanup. One resource hold now
  covers creation, the complete write and failed-write cleanup on one worker thread, including
  suspension; XML upload cleanup encloses transfer, authorization and writing.
  `creationAndCleanupHoldAuthorizationThroughSuspension` serializes racing JSON/XML replacements,
  `aFailedWriteRemovesOnlyItsOwnNewResource` checks cleanup ownership, and
  `XmlCreateUploadCleanupTest` checks refused PUT upload deletion and unchanged persisted XML.
- Lesson: see [Lessons](../LESSONS.md).
