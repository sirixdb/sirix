# Bug: resource bootstrap waited for callbacks under admission monitors

- Fix / issue: review-bootstrap-retirement-monitor-cycle; continuation of [database dispatch](database-notification-dispatch.md).
- Symptom: creating a second resource timed out after its bootstrap commit when an active notification tried to read database metadata. Retained-resource removal had the same monitor/wait ordering.
- Trigger: pause a callback for resource A, create or remove B on another thread, then let the callback call existsResource(A) or reopen its existing session.
- Root cause: resource bootstrap and retained shared-session cleanup inherited a database-wide retirement wait beneath DatabaseHandle and LocalDatabase monitors. The callback needed those admission monitors to finish. Eager derived initialization could also create or rebuild a hidden resource while session-open monitors remained held.
- Why tests missed it: prior notification tests exercised close/truncation and rejected lifecycle reentry, but never combined a permitted metadata read with another resource's bootstrap/removal or eager derived rebuild.
- Regression test: DatabaseNotificationLifecycleTest.resourceLifecycleQuiescesBeforeMetadataMonitors covers JSON/XML, direct/handle wrappers, creation/removal and pre/post/listener scopes. resourceLifecycleRefusalPreservesFilesAndOwnersForRetry covers timeout/interruption before mutation, retained readable owners and successful retry. eagerDerivedRebuildQuiescesOutsideSessionAdmission covers both session-open wrappers and actual hidden-resource rebuild. Bounds are deadlock guards; the success oracle is metadata/navigation, persisted empty resource state and released retained ownership.
- Lesson: establish notification quiescence before every outer admission monitor on a path that can close a session; include implicit bootstrap/rebuild work behind read-looking session-open wrappers. See [Lessons](../LESSONS.md).

Cold creation/removal acquires retirement before its mutation gate and monitors. Session registration
remains serialized; eager derived initialization runs after those monitors are released and establishes
its own outer retirement scope. Independent callback metadata reads stay permitted. No row-loop,
writer mutation or dispatcher delivery semantics change. Prior handle-cycle, bounded-close, ownership,
failure/suppression and interruption regressions remain applicable; earlier evidence is historical.

The ordinary-path retrace preserves two earlier behaviors: warm session opens do not repeat eager
derived setup, and removal rejects still-registered user ownership before retirement. A cold admission
result carries the existing derived-source decision outside the monitors; no persistent lifecycle
state is added. The existing delayed-release test checks the early rejection and registration fence;
a blocked-callback warm-open control checks that ordinary opens do not acquire retirement.

Fresh verification: 1,320 selected cases passed with zero failures/errors and five configured skips;
69 auxiliary cases had zero failures/errors and two skips. All 232 notification cases, 69 derived
maintainer cases and documented work-budget selections passed. Independently compiling the two
starting-HEAD classes reproduced retirement timeouts in all 24 metadata matrix cases; all 24 pass
on the fixed classes. The five previously compared hot class files remain byte-identical. See
[exact counts, source hashes and limits](../../data/sirix-1-0-lifecycle-fences/bootstrap-monitor-order-verification.json).
