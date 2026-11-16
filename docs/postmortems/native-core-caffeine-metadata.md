# Bug: core-only native images cannot construct Caffeine nodes

- Fix / issue: [I2](https://github.com/sirixdb/sirix/issues/1437).
- Symptom: a shared image compiles and loads from C, but JSON database creation
  fails with `MissingReflectionRegistrationError: PSMS()` before it can be opened.
- Trigger: GraalVM 25.0.3, `sirix-core` runtime classpath only, fresh JSON database.
- Root cause: Caffeine 3.2.4 `NodeFactory.newFactory` looks up a generated node's
  no-argument constructor with a MethodHandle. Core metadata registered `FACTORY`
  fields for those node classes, which do not have such fields. The cache classes
  (`SS...`) use static `FACTORY` fields; node classes (`PS...`) use constructors.
  Query/REST's constructor registrations masked the ownership error on their
  larger classpaths. Weighted and asynchronous caches also require `PSMW`, so
  registering only the first failing `PSMS` constructor leaves another gap.
- Why tests missed it: JVM tests do not enforce closed-world reflection;
  executable native tests included query/REST metadata. There was no core-only
  C shared-library regression.
- Regression test: `tests/native/run-smoke.sh core OUTPUT` builds with core-only
  metadata and performs create/write/reopen/read plus all size/weight,
  sync/async, statistics/listener cache combinations. `SMOKE.md` records the
  specification and independently expected integer 42. This change's local
  evidence summary (commands, results and tool versions) is in the PR description;
  baseline and mutation results, CI artifact coverage and the I3 publication
  follow-up are in the
  [validation record](../../tests/native/SMOKE.md#validation-record-2026-11-16).
- Lesson: [own native reachability in the smallest supported module](../LESSONS.md).
