# Bug: failed format opens retained ownership and skipped header validation

- Fix / issue: review findings R1 and R2 on `fm/sirix-fmt-one-format`.
- Symptom: a refused derived-resource open left a session the caller could not close; retry
  bypassed the refusal and removal failed. Separately, a stalled identity read could produce
  a compatibility proof without checking the file header.
- Trigger: open a valid-time source whose derived sibling declares `binaryEncoding=V7`, retry,
  then remove the source on the same handle; or return zero from the first superblock read
  of either persistent file while retaining compatible pages.
- Root cause: resource acquisition preceded fallible completion without an ownership unwind,
  and cached sessions skipped completion. The identity reader treated no progress as though
  the zero-filled destination represented bytes read from disk.
- Why tests missed it: refusal tests closed the database immediately and never retried or
  removed on the same handle; stalled-read tests covered envelopes but not file identities.
- Regression tests: `FormatRepairBoundaryTest.failedDerivedOpenReleasesOwnershipAndRepeatsRefusal`
  and `identityDiscoveryRefusesReadsWithoutProgress`, both persistent backends. The first
  requires repeated named refusal, unchanged files and successful removal; the second uses
  real files and a stalled read boundary, requires incomplete-input refusal and unchanged files,
  and includes successful ordinary discovery before injection. These derive from JAPI-103
  and FMT-401 rather than internal call counts. A pre-fix execution was not run because this
  review round requires all fixes before its single focused verification.
- Verification command: `./gradlew :sirix-core:test --tests io.sirix.access.FormatRepairBoundaryTest --tests io.sirix.access.InterruptedFirstCommitRecoveryTest --tests io.sirix.access.DatabaseOwnershipTest --tests io.sirix.budget.FormatDiscoveryWorkBudgetTest -PtestHeapMin=512m -PtestHeapMax=2g --max-workers=2`.
- Focused verification result: 119 selected test cases passed with no failures, errors or skips,
  including both new regressions on FILE_CHANNEL and MEMORY_MAPPED and the existing format
  discovery work budget. Gradle reported `BUILD SUCCESSFUL`; output is in the worktree-local
  `review-fix-verification.log`. Mutation testing remains owned by the outer validation phase.
- Lesson: [ownership and progress](../lessons.d/format-open-ownership-and-progress.md), collected
  into [LESSONS.md](../LESSONS.md).
