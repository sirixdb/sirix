# Bug: background maintenance contaminates beacon opening work

- Fix / issue: PR #1352, remaining-core CI check 133710933151.
- Symptom: `derivedOpenReadsOneBeacon` expected two opening beacon reads but CI
  observed three. Controlled local scheduling observes seven on both the PR head
  and its main baseline. This is a real failing work assertion; there is no
  observed difference in the core source or configuration between those heads.
- Trigger: open a source with a committed derived store lacking a durable mapping.
  The newly registered maintainer rewinds that store while the opening test samples
  its process-global beacon counter.
- Root cause: the observer mixes synchronous opening work with independent worker
  reads. The derived rewind legitimately verifies current beacons; those reads must
  not be attributed to the opener.
- Why tests missed it: ordinary targeted runs usually sample before maintenance.
  Class isolation excludes other tests, but does not stop a database-owned worker.

## Specification before implementation

`AbstractReader.beaconReads()` reports the calling thread's attempted beacon reads.
Each verified-slot attempt adds one, including corrupt primary attempts before
secondary fallback. Reads on another thread do not change the caller's count.
No decoding, compatibility, recovery or storage behavior changes.

The same invariant applies to every current observer consumer in
`InterruptedFirstCommitRecoveryStoreFeaturesTest`: JSON/XML physical opens,
logical session reuse, standalone tool opens, parent/derived opens, and read-only
salvage with either a valid primary or secondary-only recovery. Explicit revision
refresh, bootstrap checks, rewind and teardown still count their actual attempts
on whichever thread performs them, through the shared verified-slot boundary.

Derived regression: perform real beacon probes first on the test thread, then on
a joined executor thread, then again on the test thread. Assert independently
specified deltas of one for healthy stores and two for a zeroed primary, for both
FILE_CHANNEL and MEMORY_MAPPED fixtures, following the primary-then-secondary
crash-window contract in [Disk format](../DISK_FORMAT.md). Assert that the worker
leaves the opener's count unchanged. This rejects a shared counter, a missing
increment, and counting only successful beacons. Existing opening bounds remain
unchanged.

- Regression test: `AbstractReaderBeaconWorkTest.beaconWorkIsLocalToCallingThread`
  failed in all four cases before the fix and passed in all four after it. The
  unchanged 81-case feature suite, 14-case interrupted-first-commit suite, storage
  opening/corruption suites and core work-budget block also passed: 454 selected
  tests, no failures or skips. Commands and mutation evidence follow below.
- Lesson: keep work observers scoped to the operation owner when the operation
  starts independent asynchronous maintenance; see [Lessons](../LESSONS.md).
- Evidence: retained `build/ci-beacon-investigation.md` and round-nine logs under
  `build/ci-beacon-round9-*`. Earlier conclusions remain as historical evidence;
  this round is explicitly authorized to repair the failing gate.

## Trace after the correction, before verification

`readVerifiedBeacon` increments the calling thread's primitive counter before any
framing, checksum or feature decoding. `readCommittedUberPageReference` still tries
primary then secondary on corruption, and still refuses unsupported features
without falling back. `readOpeningUberPage` and `readUberPageReference` retain their
respective compatibility behavior. Bootstrap checks and revision probes call the
same increment boundary and keep their existing error handling.

The source and derived synchronous opens therefore add two on the opener; worker
rewind adds only to that worker. JSON/XML opens, tool opens, session reuse and
read-only salvage retain their exact bounds. No caller needs a new branch or
parameter, and no storage path or helper becomes unreachable. Counter storage is
allocated once per participating thread, not per node, page or beacon attempt.

## Local evidence

All Gradle/JVM commands used
`/home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh`, with disk checks above
the 60 GB floor, two Gradle workers, a 1 GB Gradle heap and 2 GB ordinary test heap.
PIT used the existing bounded configuration (two minions with 3 GB heaps and a
1536 MB main heap). No benchmark, publication, pipeline-control or hosted rerun
command was executed; replacement hosted verdicts remain the outer executor's
responsibility.

- Original controlled schedule on unchanged main and PR head: both failed both
  storage backends with expected two / actual seven. Logs:
  `build/ci-beacon-round9-main-before.log` and
  `build/ci-beacon-round9-branch-before.log`. Ordinary unmodified main/head runs
  passing and the previous 1,000-case main repetition remain recorded in
  `build/ci-beacon-investigation.md`; they do not negate the observed failures.
- The same controlled observation schedule after the fix passed both unchanged
  `derivedOpenReadsOneBeacon` cases: `build/ci-beacon-round9-scheduled-after.log`.
- Deterministic new test on the original implementation: four failures, exactly
  at the cross-thread isolation assertion, in
  `build/ci-beacon-round9-named-regression-before.log`.
- After the fix: `build/ci-beacon-round9-after.log` and
  `build/ci-beacon-round9-test-totals.txt`. Selection:
  `:sirix-core:test --tests 'io.sirix.io.AbstractReaderBeaconWorkTest'
  --tests 'io.sirix.access.InterruptedFirstCommitRecoveryStoreFeaturesTest'
  --tests 'io.sirix.access.InterruptedFirstCommitRecoveryTest'
  --tests 'io.sirix.io.IOStorageOpeningTest'
  --tests 'io.sirix.io.UberPageCorruptionTest'
  --tests 'io.sirix.budget.*'
  --tests 'io.sirix.index.projection.*WorkBudgetTest'
  --tests 'io.sirix.access.trx.page.RefusedOverflowLeafTableTest'`.
- `:sirix-core:spotlessJavaCheck` passed after applying only the new test's required
  wrapping. No existing Java file was reformatted.
- Unsampled PIT for `io.sirix.io.AbstractReader`, using the four new regression
  cases, generated 110 mutants in 36 seconds. All three on changed lines were
  **KILLED**: line 39 (`lambda$static$0`, null counter initializer), line 43
  (`beaconReads`, return zero), and line 618 (`readVerifiedBeacon`, replace addition
  with subtraction). The original shared-counter implementation is also rejected
  by all four before-fix regression cases. Reports:
  `build/ci-beacon-round9-pit/mutations.xml` and
  `build/ci-beacon-round9-pit-summary.txt`; command:
  `:sirix-core:pitest -PprMutationModule=sirix-core
  -PprMutationTargetsFile=build/ci-beacon-round9-targets.txt
  -PprMutationTests=io.sirix.io.AbstractReaderBeaconWorkTest
  -PprMutationReportDir=build/ci-beacon-round9-pit`.
  Whole-class totals are 27 killed, 23 survived and 60 uncovered. The latter 83
  mutate unchanged reader logic outside this counter repair; this narrow test is
  not a complete reader correctness suite, and those results are not claimed as
  equivalent mutants or waived changed-line failures. No changed-line survivor
  or uncovered mutant remains.

The feature-bit and malformed-beacon diagnostics also occur in passing fault
tests; the supplied CI verdict identifies a counter assertion failure. There is
no observed format/conflict-resolution interaction: base and head have identical
core trees and runtime-test configuration. The repair addresses the demonstrated
measurement defect rather than hypothesizing a data-format change.

## Sonar follow-up: intentional thread-lifetime counter

The analysis of `edfd6c4072c0cfeb6374bc3e22280043aa1772e1` failed only
`new_reliability_rating`: C (3), with A (1) required. Coverage was 95.8%,
duplication 0%, and security and maintainability passed. The only reported BUG
was `java:S5164` on `BEACON_READS`; the three older code smells do not fail this
gate. CI job 133754503560 reports `QUALITY GATE STATUS: FAILED` after analysis.

The invariant remains a cumulative count of attempted beacons on the calling
thread, with one bounded primitive holder per participating thread and no retained
application resources. It applies to primary and secondary attempts (including
failures), repeated probes, physical and logical opens, tool opens, derived-store
opens, bootstrap, refresh, rewind and teardown, for both channel and mapped
readers. All counted attempts meet at `readVerifiedBeacon`; direct raw slot reads
remain outside this diagnostic contract.

S5164 assumes a shorter ownership scope than this diagnostic has. The static key
holds a `long[1]` per participating thread: it cannot retain a reader, page,
session, resource, class-loader object or user payload. The array lives until
thread termination; closing one reader must not reset the thread's cumulative
counter. Removing it after each read would lose counts and allocate another
holder. Document that ownership and suppress only S5164 on the field, following
`RecordPageSaltSource.SOURCES`; no runtime logic or analyzer policy changes.

Acceptance checks are the existing four-case `AbstractReaderBeaconWorkTest`, the
unchanged feature/opening/corruption suites and exact core work budgets. Their
oracle is the disk-format primary-then-secondary contract: healthy probes add
one, corrupt-primary probes add two, and a joined worker leaves the caller's
count unchanged. These reject resets, success-only counting and shared state.
The failing-before regression consumer for this follow-up is Sonar itself;
source-text assertions about a suppression would not prove runtime behavior.
Compare compiled instructions before and after to verify unchanged allocation
and lifetime behavior; there is no changed executable logic requiring new PIT
mutants.

After the correction, `readOpeningUberPage` and `readUberPageReference` still call
`readCommittedUberPageReference`, which increments before I/O and decoding at
`readVerifiedBeacon`. Corruption still tries secondary; unsupported features
still propagate without fallback. All callers retain their previous results,
exceptions and count deltas. The field-only annotation changes no branch,
parameter, helper or caller assumption and leaves no unreachable code.

Verification: the same 454 selected core tests above passed with no failures,
errors or skips, including all four beacon isolation/failure cases and unchanged
core work budgets. `:sirix-core:spotlessJavaCheck` and `git diff --check` passed.
`javap -c -p` output for the compiled `AbstractReader` is byte-for-byte identical
before and after; the prior three killed counter mutants therefore still cover
the same executable logic. Evidence: `build/ci-sonar-counter/verification.log`,
`test-totals.json`, `before.bytecode.txt` and `after.bytecode.txt`. All JVM commands
used `heavy.sh`; free disk stayed above 60 GB. The public Sonar gate and PR API
responses in that directory tie the failing gate to the exact head above.
Replacement hosted Sonar analysis remains the outer executor's responsibility;
this phase did not publish or upload an analysis.
