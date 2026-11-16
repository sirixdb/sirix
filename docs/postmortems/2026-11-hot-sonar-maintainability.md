# Bug: HOT changes failed the Sonar maintainability gate

- Fix / issue: PR #1449; SonarCloud analysis of `e7d661cb5`.
- Symptom: both `Run Sonarqube` and `SonarCloud Code Analysis` failed because
  new-code maintainability was B (required A). Coverage was 94.1%; the reliability,
  security, duplication and coverage conditions passed.
- Trigger: analyzing the tightened HOT cardinality guards and bound-seek change
  reported four methods above cognitive complexity 15 and nine test-style issues.
- Root cause: structural checks and seek stages shared large method bodies. Tests
  also included setup calls inside exception assertions, assertion helpers named
  `run...`, and legacy sort/map assertion forms. The random tests already delegated
  real sorted-reference assertions; Sonar did not recognize their helper names.
- Why tests missed it: runtime tests establish behavior, whereas the maintainability
  gate checks method complexity and assertion structure. Passing one does not imply
  passing the other.
- Regression checks: execute the existing `HOTIndirectCardinalityTest`,
  `HOTRebuildFootprintTest`, `HOTMalformedSubtreeDetectorTest`, bound contract/oracle
  tests and MCP integration tests; also run the [work-budget commands](../VERIFICATION.md#running-the-layers).
  These independently check I10, precise footprint/refusal counters, structural
  defect identity, sorted-reference bounds/ranges and persisted revert revisions.
  They reject skipped guards, incorrect directional seeks and incorrect revision
  metadata. Run the same Sonar Java analyzer (8.46.0.59609) against the before/after
  methods to reproduce and verify the complexity failure. Local evidence lives in
  `build/ci-analysis/` (untracked).
- Lesson: separate cold validation stages without changing their order, work
  ceilings or allocation profile, and name assertion helpers for the contract they
  actually verify. Never split a hot kernel (per seek or per insert) to satisfy the
  analyzer: keep its measured shape and suppress S3776 on that method alone with a
  reason. See [Lessons](../LESSONS.md).

## Resolution: hot kernels kept, cold detector split

The first CI repair split all four methods. Three of them run on every seek or insert:
`HOTTrieReader.lowerOrUpperBound` (the range-seek path whose shape this PR measured
with JMH), and the writer's `measureBoundedRebuildPage` and `nodeStructurallyMalformed`.
Splitting them changes what C2 inlines, which no measurement covered, and Sirix puts
latency ahead of a maintainability grade. Those three methods are therefore restored
byte for byte to their `e7d661cb5` source and carry a method-level
`@SuppressWarnings("java:S3776")` with a one-line reason, the convention used for the
other hot loops in `sirix-core`. Only `HOTMalformedSubtreeDetector.firstViolation`, a
cold validation path, stays split. The sections below describe the first repair; the
statements about the writer and the seek apply to that intermediate state only.

## Behavior and caller trace

The refactoring must preserve every result and failure priority, including empty
or corrupt pages, unresolved/repeated references and exhausted budgets. No new
per-element objects, state, fallback or retry mechanism is introduced.

- Rebuild preflight still registers each visited page once, measures leaves without
  materializing keys, and validates fanout before resolving children. Both root
  preflight and the shadow-proof child's preflight use the same helpers; recursion
  increments depth once and stops before resolving the first page beyond the cap.
- Writer publication guards still check I10, stored partials, child resolution/MSBs,
  then key ranges. Combo-add, fold, path validation and shadow-proof callers retain
  the same boolean contract. I8/I12 branches returning the same refusal are combined.
- The full detector still reports the first violation in the order
  I10, I3, I4, I7, I11, I8/I12, I5. Its walk still stops at the highest malformed node.
  The positive-fanout check makes the old `n > 0` partial-check condition redundant.
- Both lower and upper bounds still validate each leaf-read batch before using it.
  Exact matches and in-leaf insertion points keep their fast path. Root-leaf,
  prefix/zero-continuation, left/right sibling walks, exhaustion and iterator path
  updates pass through the same statements in extracted stages. Stable corruption
  still throws; a reclaimed leaf still returns the retry signal to the wrapper.
- Exception-test setup is evaluated before the throwing operation. Both oracle
  entry points and every caller use assertion names. Sorting remains natural order;
  MCP assertions retain all three returned metadata checks.

The four original complexities were 31, 39, 66 and 32. After the first repair their
entry methods measured 9, 3, 5 and 10; every extracted helper measured at most 15.
After the resolution above only the detector (66 -> 5) remains split; the other three
keep 31, 39 and 32 under a narrow suppression.
CodeFactor's separate detail page returned HTTP 403, so its single issue could not
be inspected or independently confirmed fixed.

## Focused mutation evidence

This run predates the resolution: the `AbstractHOTIndexWriter` rows name helpers that
no longer exist, and only the detector row applies to the shipped code.

PIT 1.25.5 targeted the four refactored methods and all extracted helpers, using
`VOID_METHOD_CALLS`, `TRUE_RETURNS`, `FALSE_RETURNS` and `NULL_RETURNS` to test
stage delegation and refusal/result propagation. It generated 55 mutations:
37 killed, zero surviving covered mutations, and 18 without coverage. The latter
are justified individually below for this behavior-preserving CI refactor.
Reachable uncovered fault paths are residual validation debt, not claimed
equivalent or covered. Existing conditional logic was retained rather than
expanded to address that debt in a maintainability-only fix.

For `AbstractHOTIndexWriter`, `false` replaces a refusal return or `void` removes
`RebuildFootprint.reject`; for the detector, `null` removes the I12 defect.
The XML/HTML reports are in `build/ci-analysis/pitest/`.

| Class / method / line | Mutation | Justification |
| --- | --- | --- |
| `AbstractHOTIndexWriter.malformedChildRanges:3589` | `false` | Inherited child-refusal propagation; the selected valid-tree fixtures do not supply an invalid child. The delegation and refusal value are unchanged. |
| `AbstractHOTIndexWriter.malformedChildRanges:3597` | `false` | Inherited I8/I12 publication refusal; the selected writer fixtures do not publish interleaved ranges. Both old branches returned true and their combined predicate has the same short-circuit order. |
| `AbstractHOTIndexWriter.malformedChildReference:3615` | `false` | Inherited null-reference refusal; legal factory fixtures provide references. The check was moved intact. |
| `AbstractHOTIndexWriter.malformedChildReference:3619` | `false` | Inherited unresolved-page refusal; selected writer fixtures resolve their children. The check was moved intact. |
| `AbstractHOTIndexWriter.malformedChildReference:3624` | `false` | Inherited I11 publication refusal; selected writer fixtures have valid child MSBs. The full detector has an independent I11 regression; this writer branch was moved intact. |
| `AbstractHOTIndexWriter.malformedStoredPartials:3569` | `false` | Inherited I3/I7 publication refusal; selected writer fixtures have sorted partials. Detector regressions cover duplicate and unsorted partials; this writer branch was moved intact. |
| `AbstractHOTIndexWriter.malformedStoredPartials:3576` | `false` | Inherited I4 publication refusal; selected writer fixtures start at partial zero. The detector has an independent I4 regression; this writer branch was moved intact. |
| `AbstractHOTIndexWriter.measureBoundedRebuildChildren:641` | `void` | Inherited null-child refusal; legal factory fixtures provide references. No check or resolver operation was removed. |
| `AbstractHOTIndexWriter.measureBoundedRebuildChildren:646` | `void` | Inherited unresolved-child refusal; the selected footprint fixtures resolve their children. No check or resolver operation was removed. |
| `AbstractHOTIndexWriter.measureBoundedRebuildLeaf:599` | `void` | Inherited malformed-metadata refusal; selected real leaves have nonnegative metadata within MAX_ENTRIES. The entire metadata read/check batch was moved intact. |
| `AbstractHOTIndexWriter.measureBoundedRebuildLeaf:604` | `void` | Unreachable for valid leaves: at most 32 admitted leaves times 512 entries equals the entry cap. The earlier leaf-count refusal runs before a 33rd leaf contributes entries. |
| `AbstractHOTIndexWriter.measureBoundedRebuildLeaf:610` | `void` | Inherited side-reference cap; selected fixtures do not exceed 8,192 references. Counter arithmetic and the refusal were moved intact. |
| `AbstractHOTIndexWriter.measureBoundedRebuildLeaf:619` | `void` | Inherited reconstructed-byte cap; selected fixtures do not exceed 8 MiB. Byte arithmetic and the refusal were moved intact. |
| `AbstractHOTIndexWriter.measureBoundedRebuildPage:556` | `void` | Unreachable from these callers: depth starts at zero and the 63-page cap is reached before a descent can exceed MAX_PATH_DEPTH (64). |
| `AbstractHOTIndexWriter.measureBoundedRebuildPage:560` | `void` | Inherited closed-page refusal; selected fixtures keep pages open until measurement finishes. Check order is unchanged. |
| `AbstractHOTIndexWriter.measureBoundedRebuildPage:570` | `void` | Redundant boundary for current callers: both recursive child traversal and the shadow-proof caller stop at the page cap before calling; root measurement resets the counter. |
| `AbstractHOTIndexWriter.measureBoundedRebuildPage:581` | `void` | Inherited unsupported-page refusal; the selected fixtures contain HOT leaves/indirects. Dispatch order is unchanged. |
| `HOTMalformedSubtreeDetector.firstRangeViolation:278` | `null` | Inherited I12 detector refusal; selected detector fixtures cover I8 but not overlapping ranges with ascending first keys. The complete range loop and first-defect ordering were moved intact. |

## Local verification environment

The initial default-heap run was terminated with exit 137, without an assertion
failure. The completed checks use the existing `testHeapMin=256m` and
`testHeapMax=2g` overrides, `sirix.allocator.maxSize=512M`, one worker and isolated
test classes. A temporary init script restricts class discovery to the same
command-line test selectors. These are local execution settings; no project
configuration, work-budget ceiling or wall-clock assertion was changed.

Formatting checks passed for both changed modules. Core finished 412 cases and
MCP finished 36 cases with no failures, errors or skips. Query finished 332 cases,
including five skips, with no failures or errors. The skips are the pre-existing
100,000-row valid-time cases gated by `SIRIX_VALID_TIME_LARGE_BUDGET=true`.
The total is 775 passing cases.
The complete run finished successfully; results are recorded in
`build/ci-analysis/verification-final.log` and `test-summary.json`.
