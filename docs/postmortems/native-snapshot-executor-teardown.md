# Bug: custom snapshot append workers prevent native isolate teardown

- Fix / issue: [I2](https://github.com/sirixdb/sirix/issues/1437); full terminal
  runtime conformance remains I4/NATIVE-406 work.
- Symptom: closing every transaction/resource/database does not let
  `graal_tear_down_isolate` return after a write, even without query evaluation.
- Trigger: core-only shared image, create FILE_CHANNEL resource, write/commit,
  reopen/close, tear down the isolate. The pre-fix diagnostic exits via the
  external 60-second watchdog (124), not a successful teardown return.
- Root cause: `NodeStorageEngineWriter` prestarts static `SnapshotAppendExecutor`
  workers. This custom ThreadPoolExecutor subclass wraps the executor's Worker
  runnable to mark a ThreadLocal. Its idle workers ignore interrupts until the
  executor enters shutdown. GraalVM 25.0.3 `PlatformThreads.tearDown` recognizes
  only direct Worker targets whose executor class is exactly ThreadPoolExecutor
  or ScheduledThreadPoolExecutor (and standard ForkJoinPools). It deliberately
  does not call arbitrary overridden shutdown methods. Sirix's wrapper and
  subclass fail both recognition checks, leaving the workers attached forever.
- Why tests missed it: JVM exit does not wait for daemon workers; executable
  native tests did not tear down a shared isolate. Ordinary database close
  intentionally leaves process-wide infrastructure warm.
- Regression test: `SnapshotExecutorShutdownTest` holds an append task at a latch
  while terminal shutdown starts, then has running and queued append tasks use
  the still-live serialization pool. The result must be 42, both pools terminated,
  and all admissions returned. The C smoke invokes the real shared-pool hook
  after all data work and requires isolate teardown to return zero. Omitting the
  hook in a C harness is the native lifecycle mutation.
- Lesson: [custom executors need explicit native terminal shutdown](../LESSONS.md).

Diagnostic evidence from GraalVM's `-R:TearDownWarningSeconds=5` names only
`sirix-snapshot-append-1` and `sirix-snapshot-append-2` as laggards, blocked in
`ArrayBlockingQueue.take -> ThreadPoolExecutor.getTask -> runWorker`, with
`interruptedStatus: false`. The file-channel reaper, common pool, cleaner and
clock sweepers appeared in the pre-teardown thread census but were not laggards;
changing those services would not fix this defect. The installed GraalVM
`lib/svm/builder/svm.src.zip`, `PlatformThreads.java:581–615`, supplies the exact
recognition rules. See also the [Graal C API shutdown guidance](https://www.graalvm.org/jdk25/reference-manual/native-image/native-code-interoperability/C-API/).

`StorageEngineWriterFactory.shutdownSharedExecutors()` is an explicit terminal
hook: the runtime owner must stop writer admission and close all transactions
first. It drains append coordinators before closing the serialization pool they
can still submit to. It adds no work, allocation or synchronization to write,
read or query hot paths. Ordinary database close and CLI/REST builds are unchanged.

This is not a complete native runtime owner. I4 still must enforce outstanding
handle/context checks, own admission, retire caches, release allocator mappings,
and test repeated isolate cycles. The hook is terminal for the current engine;
opening more writers in that JVM/isolate afterward is unsupported. Hot unloading
remains unsupported. Do not infer full NATIVE-406 conformance from process exit.

See the [validation record](../../tests/native/SMOKE.md#validation-record-2026-11-16)
for the final core/query C results, same-library shutdown-omission controls and
PIT drain-order results. The omission controls reproduce the non-return both
with and without query evaluation.
