# I2 shared-image regression specification

These test-only Graal entrypoints are not libsirix's public C ABI. They establish
Linux x64 feasibility; C++/Rust/Python, XML parity and the remaining NATIVE
conformance requirements remain release work in I3–I10.

Written before the fixture/metadata changes for issue #1437:

| Requirement | Observable test and independent oracle |
| --- | --- |
| NATIVE-101 | Build separate core-only and query shared images. Execute C11 with an empty environment and PATH=/nonexistent, reject JAVA_HOME, libjvm mappings/dependencies and any exec after the initial C executable. No query/REST artifacts or metadata may enter the core classpath. |
| NATIVE-201 | Create a fresh JSON database, reject duplicate create without overwriting, write one explicit commit, close, reopen and read the stored integer 42. A missing open must fail without creating a database. |
| NATIVE-205 | Insert `{"answer":42}` with Commit.NO, explicitly commit, read after all writer/database handles close. |
| NATIVE-301 | Query-enabled image evaluates `jn:doc(...).answer` through Brackit after reopen; the C caller requires integer 42. |
| NATIVE-102,406 | Capture live thread identities/stacks after logical close. The C caller invokes isolate teardown and requires zero. An external timeout is a hang watchdog, not a latency assertion. An unresolved external lifecycle defect must remain an explicit I4 blocker with reproducible diagnostics. |
| Core metadata ownership | Construct and use all core cache configurations (size/weight, synchronous/asynchronous, with/without statistics and removal listener), invalidate and run maintenance. Check lookup value and maximum size/weight eviction behavior. Run in core-only image. |

Error and mutation checks: missing input path; duplicate create; missing database
open; remove PSMS and PSMW constructor registrations separately in copied core
classpaths and require native execution to fail with the matching
MissingReflectionRegistrationError. Metadata
mutations are tested with Graal's real consumer, not source-text assertions.
Repeat successful create/write/reopen/shutdown in fresh processes. No forced
process exit inside an entrypoint can count as teardown success. Diagnostic
thread dumps and exec traces are preserved in the supplied output directory.

A fresh checkout must build every project artifact named in the exported runtime
classpath; the export cannot depend on a JAR left by an earlier test run.

Existing CLI/REST native executable recipes remain unchanged. Native metadata
is packaged by its owning core module; smoke-only Java sources and flags remain
under tests/native. No production read/load/query hot path is changed by this
metadata correction, so it adds no per-record allocation or work.

Cache inventory (Caffeine 3.2.4): strong size-bound caches in resource sessions,
index controllers, revision metadata and name/path caches use PSMS nodes;
weighted verdict/dictionary/projection/valid-time caches and asynchronous
`StorageType` revision metadata use PSMW nodes. PageCache also enables a removal
listener and statistics (SSLSMS cache). The test explores the Cartesian product
of those flags (16 shapes), including SSSMS/SSSMW statistics-only variants.
Existing historical expiration-related metadata is retained; its node entries
receive the same correct no-argument constructor registration. No weak/soft
reference cache or arbitrary user-supplied Caffeine configuration is promised.

### Snapshot executor terminal shutdown

Native diagnostics must identify actual teardown laggards. The pre-fix core
write leaves `sirix-snapshot-append-1/2` waiting on ArrayBlockingQueue.take.
GraalVM 25.0.3 PlatformThreads.tearDown shuts down recognized standard daemon
pools, but this custom subclass with a wrapped worker is not recognized.
The standard file-channel timer and clock sweepers terminate successfully.

Before changing production shutdown logic, derive these tests: after writer
admission has stopped and writers are closed, close the append coordinators,
wait for their active/queued tasks (which may still use serialization workers),
and only then close the serialization pool. A latch-held append task must still
be able to submit serialization work while shutdown waits. Both pools must be
terminated before the hook returns. Repeated terminal close must be harmless.
This hook is terminal for its isolate, not a restartable runtime implementation;
I4 owns admission, user-handle checks, allocator cleanup and repeated isolates.

### Validation record (2026-11-16)

Oracle GraalVM 25.0.3+9.1, gcc 13.3.0, Linux x64. Builds use two threads and
a maximum 4 GiB native-builder heap. These are local regression results, not
a claim that the hosted CI job or full public-ABI conformance has run.

The durable CI evidence location is the native-smoke job artifacts of the CI
run: `native-library-smoke-linux-x64`, uploaded by the `smoke` job in
[the native-library workflow](../../.github/workflows/native-library-smoke.yml).
The workflow currently archives normal build/loader logs, thread censuses,
ELF dependencies and process traces. It does not yet run the PSMS/PSMW
constructor omissions or the omitted-shutdown controls, so their mutation
reports and teardown-laggard diagnostics are not yet archived there. The local
results below do not establish that those CI artifacts have been produced.

| Check | Observed result |
| --- | --- |
| Core-only C image | Two fresh-process cycles passed: all 16 cache shapes, persisted 42, no libjvm mapping/dependency/access or child exec, shutdown and teardown both zero. |
| Query C image | Two fresh-process cycles passed the same checks plus JSONiq integer 42. |
| Native shutdown omission | Both same-library controls completed data work with 42, then hit watchdog exit 124 with snapshot append laggards; the query control also returned JSONiq 42. |
| JVM lifecycle regressions | 18 tests passed across shutdown order, append backpressure and file-channel storage. |
| Core metadata omissions | Separate PSMS and PSMW constructor omissions both rejected by native execution with the matching `MissingReflectionRegistrationError` and exit 92; both mutations killed. |
| PIT shutdown order | Both generated close-call removal mutants killed (2/2); no surviving/uncovered mutants in that helper scope. |
| Work budgets | Core: 325 passed. Query: 322 passed, 5 existing large cases skipped unless `SIRIX_VALID_TIME_LARGE_BUDGET=true`. MCP: 3 passed. |

The two production forwarding methods contain only the terminal-hook call.
Deleting either forwarding call has the same observable effect as the native
omitted-hook control above. Their actual call chain is exercised by the normal
C consumer; they are not included in the helper-only PIT count.

ShellCheck, actionlint, Bash syntax, the no-Graal skip path and final core
Spotless checks passed. The GitHub workflow remains to be exercised by the
shipping pipeline.
