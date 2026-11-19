# Bug: commit folds reread evictable boundary references

- Fix / issue: PR #1460, Refs #1452.
- Symptom: both bitemporal storage epoch eviction tests fail at commit on Linux, macOS and Windows; core allocation lanes also fail.
- Trigger: an accepted fold loads multiple clean boundary roots; loading a later root evicts an earlier root and clears its PageReference.
- Root cause: materialization computed height in a second pass over references that are cache hints, not ownership. Invariant: each retained parent child and promoted grandchild contributes its height while its resolved page is available. The same rule applies across repeated folds and height repair. Capture the primitive maximum during resolution and pass it to materialization; do not pin or load descendant keys.
- Why tests missed it: mocked loads kept earlier references resident. The independent allocation control also assumed C2 eliminated its temporary shuffle array on every JVM.
- Regression test: HOTCommitNormalizerTest.foldHeightSurvivesEvictionOfPreviouslyResolvedBoundaryRoots executes normalization with eviction between boundary loads and independently expects one remaining inner level (height two). Existing bounded/unbounded BitemporalStorageEpochEvictionTest compares cold publications. HOTCommitAllocationBudgetTest keeps the exact 240/352-byte fold graphs; its comparison factory is only an allocating control, whose temporary workspace is not part of the fold budget.
- Mutation regressions: the forward ancestor loop boundary mutant is equivalent (it additionally reads the always-null current slot). Scan active ancestors from nearest to root instead; strengthen the cycle regression to require immediate cycle detection, rather than eventual depth rejection. Deserialized inner pages start normalized and structural edits re-arm them; an executable wire round trip kills the other survivor.
- Lesson: consume cache-resolved metadata before another load can invalidate its reference; distinguish required output from temporary allocation in comparison controls. See ../LESSONS.md.

## Expanded mutation audit

The unsampled normalizer run generated 67 mutants, killed 61, with five survivors
and one uncovered mutant (93% line coverage). Dispositions at the final source lines:

- `normalize:81`, return null: unreachable under the exclusive-writer contract;
  both the public entry and recursive child dispatch establish ownership before entry.
- `normalize:86`, `>=` to `>`: defensive invalid-depth case; sound input is bounded
  by the stored-height precheck and height-one blocks do not recurse. Stale malformed
  heights reaching physical depth 64 are outside this suite's coverage.
- `normalize:89`, initial `depth - 1` to `depth + 1`: reads two additional null
  scratch slots on the exercised valid depths, changing work but not cycle detection.
  The depth-63 defensive boundary is not exercised. This is distinct from skipping
  the root ancestor, which the first-back-edge work assertion kills.
- `normalize:108`, `>` to `>=`, and child `+ 1` to `- 1`: normalization only shrinks
  sound children. These mutations can request a redundant height recomputation,
  without changing the final maximum. Extra boundary-load work in this defensive
  branch is not asserted by this suite; no claim of a work-equivalent mutant is made.
- `normalize:126`, `<` to `<=`: a full parent visits candidate slots unnecessarily,
  but preparation refuses every fold because replacing a compound child adds at
  least one slot. No output graph, height, leaf, or publication changes. The extra
  bounded ownership lookups are not asserted by this suite.

These expanded audit dispositions do not suppress or allowlist CI mutants.

## Reproduction and verification commands

```bash
./gradlew :sirix-core:test --tests io.sirix.index.hot.HOTCommitNormalizerTest
./gradlew :sirix-core:dictionaryReadViewAllocationTest
./gradlew :sirix-query:test \
  --tests io.sirix.query.bench.bitemporal.BitemporalStorageEpochEvictionTest \
  --tests 'io.sirix.query.budget.*' \
  --tests io.sirix.query.json.IndexedArrayPositionTest \
  -PtestHeapMin=256m -PtestHeapMax=2g
```

Before the production fix, the eviction regression failed with
`IllegalStateException: HOT structural height requires a resolved child page`.
Afterward the focused suite passed all 17 tests, and the isolated HOT allocation
suite passed all eight tests. The selected core work-budget/fold run passed 368
cases. The first query attempt used the workstation heap default and was killed
with exit 137; the bounded rerun uses existing repository test properties, with
no change to project configuration. CodeFactor's GitHub status reports three
issues, but its detailed report returns HTTP 403 through both HTTP and browser
access; those issues cannot be identified from the status alone.

The bounded rerun passed all 329 selected query tests (including both eviction
regressions), plus the final 17-test normalizer suite. Formatting and
`git diff --check` passed. Raw local logs and mutation reports are retained under
`build/ci-gate-evidence` for this worktree.

The final CI-equivalent sampled blocking PIT run completed successfully: all
seven changed-line mutants were killed, with zero survivors or uncovered
changed-line mutants. The one timeout and other advisory mutants are on unchanged
lines of the selected classes. See `build/ci-gate-evidence/sampled-pit/summary.md`.
