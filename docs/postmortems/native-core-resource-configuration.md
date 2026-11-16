# Bug: core native image cannot reopen its own resource configuration

- Fix / issue: [I2](https://github.com/sirixdb/sirix/issues/1437).
- Symptom: after fixing Caffeine node construction, creating a resource in the
  core-only C image fails with `ClassNotFoundException: io.sirix.node.NodeSerializerImpl`.
- Trigger: create a JSON database, create its first FILE_CHANNEL resource; the
  bootstrap session immediately deserializes the configuration just written.
- Root cause: `ResourceConfiguration.deserialize` loads the persisted serializer
  class name with `Class.forName`, enumerates its public constructors and invokes
  the default one. That reachability was owned only by query/REST metadata.
  Built-in byte-pipeline handlers likewise reconstruct classes/constructors from
  persisted configuration and need core-owned metadata.
- Why tests missed it: the JVM can load reachable classes dynamically, and
  query-enabled native builds carried the serializer's registration. The first
  Caffeine failure prevented the earlier core probe from reaching this path.
- Regression test: `tests/native/run-smoke.sh core OUTPUT` creates and reopens a
  real resource using core-only metadata. The metadata-only intermediate build
  reproduced this failure after the Caffeine correction; its stderr and native
  build log are retained with the I2 evidence.
- Lesson: [round-trip persisted configuration in the minimal native image](../LESSONS.md).

After core-owned registrations, the final core-only C image completed two fresh
create/resource/write/close/reopen/read cycles, each returning integer 42. Its
19-entry runtime classpath contained no query or REST artifacts. This is the
failing-before/passing-after regression for the persisted serializer lookup;
the test does not claim arbitrary user-provided serializers are reachable.
