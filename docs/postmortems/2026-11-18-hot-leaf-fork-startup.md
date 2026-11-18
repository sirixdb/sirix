# Bug: narrow-vector allocation fork fails before tests on Temurin

- Fix / issue: PR #1454, all four core jobs in workflow `44200615632`
  at `3673c1f41a97b045fced3c3c7dee61ce2a25f675`.
- Symptom: `hotLeafNarrowVectorAllocationTest` exits before executing tests:
  `UseJVMCICompiler` is experimental and the unlock option must precede it.
- Trigger: start the dedicated 128-bit C2 allocation fork on Temurin 25.
- Root cause: the task disables JVMCI without first unlocking experimental VM
  options. GraalVM accepted the option locally, hiding Temurin's startup contract.
  Invariant: the fork must start on supported JDK 25 runtimes with experimental
  options unlocked before disabling JVMCI, while retaining synchronous C2
  compilation, the 128-bit vector limit and every existing allocation bound.
- Scope: one shared task configuration serves Linux hot-property/remaining and
  macOS/Windows core jobs, for both available-native-LZ4 and missing-native-LZ4
  execution. The normal-width allocation task does not disable JVMCI and needs
  no change. There is no production codec or lookup change.
- Why tests missed it: the existing executable allocation regression ran locally
  on GraalVM, not the CI Temurin runtime; no tests execute when startup fails.
  Retained macOS and Windows normal-width reports both show eight lookup cases
  at zero bytes and codec operation 10 at raw=encoded=0, confirming the previous
  production vector-width fix.
- Regression test: execute the existing `:sirix-core:hotLeafNarrowVectorAllocationTest`
  on checksum-verified Temurin 25.0.5+8 (the CI version). Its real worker startup
  is the launcher acceptance check; `HOTLeafLookupAllocationTest` supplies the
  independent unsigned-key-order oracle and exact zero-allocation assertion;
  `HOTLeafCodecAllocationTest` retains its strict raw/encoded bounds. Repeat with
  native LZ4 loading blocked, and run `:sirix-core:dictionaryReadViewAllocationTest`
  at normal vector width. No source-text test is used.
- Correction: unlock experimental options immediately before disabling JVMCI in
  the shared narrow-vector task's ordered JVM arguments.
- Retrace: standard test fork options are copied first; the narrow task then
  appends synchronous compilation, unlock, JVMCI-disable and vector-limit options.
  Gradle launches a real C2 worker, discovers both suites and runs their unchanged
  correctness/allocation checks. Both Linux lanes and both cross-platform core
  callers retain their task dependency; the normal-width sibling keeps its
  existing arguments. Native-codec availability still selects the existing
  zero-run-only or zero-run-plus-LZ4 cases. No branch, helper or caller becomes dead.
- Lesson: [verify ordered VM options on the CI runtime](../LESSONS.md#verify-ordered-vm-options-on-the-ci-runtime).

## Verification contract

Before correction, the existing narrow task must fail to start on Temurin.
After correction it must execute and pass the strict lookup and codec checks,
with and without native LZ4. Deleting the unlock or moving it after JVMCI-disable
must reproduce startup failure: these are configuration mutations, outside PIT's
Java bytecode scope. Evidence is retained under `build/ci-repair/round7/`; every
Gradle/JVM command uses the prescribed `heavy.sh` wrapper.

This is an interim CI repair. The measured-margin follow-up, native PGO campaign
and final validation remain pending; this result is not readiness acceptance.

## Evidence and local verification limitation

- The retained full CI log reproduces the original startup rejection in all four
  jobs. The retained macOS/Windows normal-width XML reports have no failures;
  each has eight zero-byte lookup cases, and codec operation 10 reports zero
  bytes in both arms. Native LZ4 is absent there, so its parameter is skipped.
- Downloaded Temurin 25.0.5+8 into the worktree and verified its published SHA-256
  (`68982529869257d0ee6b824ec5aa270ff8b118bb438670da0b97ebdab178d7fb`).
- Local verification was attempted through the required `heavy.sh` wrapper on
  2026-11-18, approximately 01:00–01:10 Europe/Berlin. The active exclusive
  `fm-timing` lock prevented any Gradle worker or verification JVM from starting.
  The queued runner was cancelled before handoff; shared infrastructure was not
  changed. Disk remained above 80 GB free.
- The normal/narrow forks, missing-native-LZ4 repeat and both configuration
  mutations remain **unexecuted locally**. The scripts and logs under
  `build/ci-repair/round7/` preserve the exact pending commands. The outer executor
  must verify actual Temurin worker startup and the unchanged allocation bounds;
  this phase does not claim a local pass or readiness acceptance.
