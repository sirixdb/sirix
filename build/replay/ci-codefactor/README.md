# CodeFactor CI repair — PR 1273

Report retrieved for exact target `8ad70fa464e5b943ff967603b45f94fcf454f188` through
`https://www.codefactor.io/Repository/Github/sirixdb/sirix/pull/1273/` (HTTP 200).
The canonical URL returned HTTP 429. `issues.json` captures all 19 provider diagnostics:
17 method-complexity notes and two package-directory mismatches. No checks were weakened.

## Invariants and affected callers

- Package declarations must match the suffix of each retained Java source's directory.
  Both reported sites are addressed: the superseded auxiliary `JsonIdentityDerivedIndexTest`
  is removed (the stronger production-suite test remains); `ReplayLatency` moves byte-for-byte
  to `latency/io/sirix/replaybench/`, and its sole compiler consumer `export.init.gradle`
  uses that path. The reproducer instructions now point to the promoted test.
- Complexity cleanup must preserve all existing ordering, reads, writes, rejection messages,
  work counters and allocation sites. Only contiguous blocks/predicates are extracted.
  Every related site follows the same unchanged predicates:
  - `JsonReplayTransitionValidator.validate` calls `validateParent` for each affected parent:
    missing/deleted parents, fresh/existing parents, child counts on/off, descendant counts
    on/off, append suffixes and general permutations retain their previous paths.
  - `observe` calls `observePriorParent` only when parent/left/right links change; DELETE,
    PUT creation, update and both move directions retain old/new parent contributions and
    the same non-append classification. Its base-parent non-null assumption and scoped
    NullAway suppression remain together.
  - `validateBoundary` calls `validateDeweyBoundary` after parent compatibility and left/right
    reciprocity, before first/last child checks. Root/non-root, first/non-first siblings,
    Dewey enabled/disabled and missing left Dewey rejection retain their original order.
  - `validateChildren` calls `validateRequiredChildren` after final-chain counts are checked:
    general traversal requires every changed child to be visited; append permits unchanged
    children only when their old parent matches. No ancestry/visited state or reads are added.
  - `JsonReplayGraphValidator.validate` calls `validateDeweyOrder` in the existing enabled
    branch, before frame advancement; initial imports, the one-argument validation overload,
    and snapshot/shadow-oracle callers retain ancestry and left-sibling ordering checks.
  - `JsonReplayHistory.importChanges` calls `mapRevisions` only for changed non-root histories:
    full-copy offset zero clones; suffix copies skip pre-boundary events, add revision-one
    availability only for live old identities, then map visible events. Unchanged/deleted
    histories and empty/nonempty mapped results retain their old consumer paths.
  - `JsonReplayPaths.importChanges` calls `logicalPathChanged` after equality/statistics
    comparison, before delete/copy installation. Root/non-root, created/deleted/unchanged,
    renamed/reparented and kind/level changes use the identical logical-root predicate.
  - `JsonValidTimeIndexListener.beginIdentityImport` calls `captureIdentityBounds` after
    object capture and order checking. Both validFrom/validTo fields, unnamed/fused objects,
    create/delete/update/rename/reparent, string/non-string bounds and unchanged values keep
    the same old/new containing-object captures. Deletes retain `captureIdentityNode`.
    `JsonNodeTrxImpl.importRevisionLocked` still starts the listener before relinking and
    completes it after final topology/index notifications; rollback/retry is unchanged.
  - `JsonNodeTrxImpl.replayAdditionalIndexKeys` calls `hasFilteredReplayIndex` before allocating
    scratch sets or moving the cursor. Filtered PATH/CAS and unfiltered/other indexes retain
    identical traversal, namespace selection and DELETE/INSERT notification callers.

  - `insertSubtreeInternal` uses one `insertionBoundary` switch at both original call sites,
    then calls `repairBulkInsertHashes` at the original forest-repair point. Gson, Jackson,
    Brackit Item and LDJSON callers retain all supported first/last child and left/right sibling
    positions, root-token skipping, per-insert auto-commit hashing, forest traversal direction,
    old frontier/boundary stop, selected-root restoration and rollback-only exception handling.
  - `remove` calls `validateRemoval` while holding the original page guard, after its existing
    access/auto-commit check and before resetting `canRemoveValue`. Document-root rejection,
    plain/fused container parents, primitive children, `replaceObjectRecordValue`'s compound
    removal allowance, descendant/root indexing and final right/left/parent cursor selection
    remain unchanged; the helper preserves the two original validation errors.
  - `JsonReplayPageWalk.walk` uses shared `dereferenceAtHeight`, `missingIndirectPage` and
    `childReference` predicates for both old/new tries. Document, path-summary and history
    consumers retain equal/different heights, absent references, lower-height offset-zero
    embedding, same-durable-region early stops, dereference order, missing-page rejection,
    overflow validation, logical bitmap union and all guard/counter calls.

All new helpers have live callers; no replaced boolean flag or unreachable helper remains.
These edits reduce twelve reported complex methods without restructuring their algorithms.
The provider must recompute the report after the outer executor publishes the repaired head.

## Known CodeFactor notes for the PR description

The following five original complexity findings are retained under the author's explicit
small-fix scope. Resolving them would require splitting coordinated import stages, order-planning state, large test fixtures/oracles
or the historical measurement driver;
this round does not restructure those paths:

| Source | Method | Reported complexity |
| --- | --- | ---: |
| `JsonNodeTrxImpl` | `importRevisionLocked` | 39 |
| `ProjectionIndexChangeListener` | `collectIdentityOrderChanges` | 21 |
| `JsonIdentityProjectionEpochTest` | `batchRotationsAndMixedPermutationsKeepOrderedAnchors` | 16 |
| `JsonIdentityValidTimeEpochTest` | `assertEvidence` | 29 |
| `ReplayLatency` | `main` | 21 |

The benchmark's class name and source bytes are unchanged; existing measured manifests,
raw samples, historical latency report and its explicit source/public-diff and wide-move
uncertainty remain untouched. No benchmark campaign runs in this repair.

## Verification

Every build/test JVM uses the captured
`run-review-boundary.sh` inline heavy limiter, private `nm-m2`/`nm-gradle-home`/`nm-tmp`,
2 GiB daemon/test heaps, `--no-parallel`, one admitted command at a time, and the post-admission
1800-second guard under the 2026-10-07T01:40Z cutoff. The private Brackit main JAR's SHA-256
matches published `1.0-alpha10-20261006.152144-93`; each compiler prints its resolved path.
No pipeline control, commits, push, PR edits or other validation phases run here.

- Final configured core/query Spotless checks and Error Prone core main/test compilation passed;
  the actual `compileReplayLatency` Gradle consumer compiled the relocated, byte-identical source.
- Final core selection: 1366 passed, 6 existing skips, zero failures/errors. All
  229 core/projection work-budget cases passed. This includes identity epochs/history, generated
  graphs, cold index/source/copy oracles, missing-Dewey rejection, rollback/retry, all insertion
  directions, Jackson/LDJSON and insertion/removal/hash/public-diff regressions.
- Query budget selection: 32 passed, 5 existing skips, zero failures/errors.
- `commands.txt` captures the commands; final logs, exit codes and JUnit XML are retained
  in this directory. `final-core-summary.json`, `final-query-summary.json` and `summary.json`
  record their outcomes. The initial nine-extraction pass also passed 921 cases; it is
  distinct from the final-source verification above.
- `verified-source-digests.json` identifies the verified source; all digests remained unchanged
  through final checks. `git diff --check` passed. Existing compiler warnings on inherited
  code remain unchanged; extracted helpers introduced no compiler diagnostics.
- No full core/query suite or new measurement campaign is claimed by this CI repair.
  The hosted provider must analyze the published repaired head; the five known complexity
  notes above are the intended PR-description handoff under the approved small-fix scope.

## Follow-up on published head `df1f5fed6` (2026-10-07)

GitHub status `55753146252` is a real failure on exact head
`df1f5fed6f179e305d5bcc6d2816b7a16dc09290`: "1 issue fixed. 9 issues found."
The current report was retrieved with HTTP/1.1 at 01:10Z after HTTP/2 returned 429.
`round4-issues.json` retains all nine diagnostics. Existing issue IDs retain their
original detection revision/locations; this does not make the current status stale.
Both package findings are gone. The five known notes above remain under the approved
small-fix scope; the other four methods receive further contiguous extractions.

The invariant remains identical operation order, rejection messages, cursor state,
rollback behavior, allocations and work counters across all original callers:

- `insertSubtreeInternal`: base-revision recording, inserted-root selection and the
  repeated conditional rollback latch move to small helpers. Gson, Jackson, Brackit Item,
  LDJSON, all four positions, skipped/retained roots and all commit modes retain the same
  sequencing. Validation failures before mutation still leave the rollback latch alone;
  all three exception categories after mutation still set it before propagating the same
  exception. The exhaustive enum switch drops only its unreachable empty default.
- `remove`: the unchanged postorder descendant loop moves to `removeDescendants`, still
  inside the original page guard and lock. Direct removal and compound value replacement
  retain validation, plain/fused/primitive root and descendant index dispatch, stale diff
  purging, history writes, hash repair and right/left/parent cursor selection.
- `observe`: child contributions and link-change parent observation move to separate
  helpers at their original points. DELETE/PUT creation/update, both move directions,
  old/new parents, append/general permutations and count settings retain all original
  map/set operations, reads and allocation sites.
- `validateBoundary`: parent compatibility and both sibling/Dewey boundaries move to
  `validateSiblingBoundaries`, still between document-root and child-boundary checks.
  Root/non-root, first/last/interior siblings, plain/fused kinds and Dewey on/off/missing
  retain the same rejection order, including missing-left-Dewey rollback/retry.

Each ordinary path and its callers was retraced before verification. All helpers have
live callers; no alias, replaced parameter, fallback or unreachable branch remains.
This follow-up adds no tests, assertions, work bounds or production behavior.

Local verification uses `round4-run.sh`, which preserves the captured inline heavy
limiter, private homes, read-only dependency cache, 2 GiB heaps, `--no-parallel` and
post-admission deadline guard. Its per-command timeout is configurable (900 seconds
by default) so checks can finish before 01:40Z. The resolved private Brackit JAR still
matches published `1.0-alpha10-20261006.152144-93` and the SHA-256 recorded above.
Final outcomes and source identities are recorded in `round4-summary.json` and
`round4-source-digests.json`; command logs and JUnit XML remain in this directory.
Final scoped core/query Spotless checks and Error Prone main/test compilation passed.
The final core selection passed 1,401 cases, including all 229 core/projection budgets,
with 11 existing skips and zero failures/errors. Query selection passed 32 cases with
five existing skips and zero failures/errors; its generic reader-lifetime dependency
also passed. Source digests remained unchanged through verification. All JVM commands
finished before 01:40Z; no full suites or benchmark campaign ran in this follow-up.
Publication, hosted reanalysis, PR-description updates and other phases remain with
the outer executor. The retained five notes are not claimed resolved or waived by CI.
