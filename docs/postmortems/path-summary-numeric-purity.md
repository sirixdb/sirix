# Bug: numeric summaries can hide earlier strings

- Fix / issue: PR #1352 review R1.
- Symptom: sum/min/max over `[10,"bad",20]` return 30/10/20 instead of the generic
  evaluator's type failure, including after reopening the resource.
- Trigger: insert the mixed field values in one commit with default path statistics
  and no projection. A string followed by a numeric value loses its byte-statistics marker.
- Root cause: deferred statistics keep one last-observation kind and one shared count.
  `addLong` overwrites the string kind; `applyDeferredStats` persists numeric statistics
  with all observations but no byte bounds. Numeric bounds, reference equality and absent
  byte bounds are therefore insufficient evidence of numeric purity. The query admission
  guard treated lossy aggregate metadata as a type certificate.
- Invariant: summary value aggregates may run only when authoritative evidence proves that
  every contributor has the required numeric type. This covers sum/min/max, the already
  declined avg, cold and cached execution, and ordinary/deferred/bulk ingestion. Structural
  field counts depend on exact path references, not value statistics.
- Fix scope: remove value-summary admission at the shared query boundary. The existing
  typed scan and generic refusal contract own value/error semantics; exact field-count
  summaries and all existing projection routes remain. Do not change writer kernels,
  storage format, work-budget bounds, or the accepted compiler fallback fixes.
- Why tests missed it: the mixed-string fixture put the string last, retaining the byte
  marker; a pure-integer summary-serving control could not establish type purity.
- Regression tests (specified before implementation): add leading/middle/trailing string
  placements for sum/avg/min/max to `VectorizedReleaseCorrectnessTest`, using its forced
  generic oracle, observed SPI admission/refusal and cold reopen. Require zero value-summary
  servings in observed and ordinary auto-wired execution. Add pure unfiltered integer
  controls with worked expectations 30/15/10/20. Retain the bulk fixture's exact results,
  require value-summary refusal, and positively witness exact structural field-count
  summary serving and its cache. Existing double/integer projection and field-count
  versioning tests must retain their route witnesses and budget assertions.
- Verification plan: after all edits, run one focused round with the original executor
  compiled into a separate baseline classpath, followed by corrected tests and scoped
  mutation checks. The baseline must fail the leading/middle sum/min/max cases; avg and
  trailing-string cases are controls. Every JVM invocation uses the supplied heavy wrapper;
  check at least 60 GB free on `/` before Gradle. No benchmarks or timed measurements.
- Lesson: an aggregate type marker that can be overwritten cannot certify all input types;
  test invalid values at every position and refuse an unprovable shortcut. See
  [Lessons](../LESSONS.md).

## Verification results

- One focused Gradle round compiled the pre-fix executor into a separate baseline
  classpath before running the corrected implementation. Baseline: 12 cases, six
  failures, no errors/skips. Leading/middle strings incorrectly served numeric sum/min/max
  both warm and after reopen; avg and trailing-string controls passed. The failure output
  independently records generic `err:FORG0006`, numeric fast results, missing SPI refusal
  and the summary-serving counter increment. Evidence:
  `build/review-summary-purity/baseline-results/` and `verification.log`.
- Corrected implementation: 195 tests pass, none fail/error/skip. Coverage includes all
  105 release cases, the bulk exact-result/count-summary/cache control, five double
  projection-serving tests, three exactness tests (including integer projection), eight
  structural-count/cache versioning cases, four source-scope differentials, seven fallback
  and storage-error checks, and 62 unchanged query work-budget cases. Value-summary serving
  stays at zero in observed SPI and ordinary auto-wired execution; mixed strings preserve
  the generic errors through actual refusal, and pure integers remain admitted.
- PIT mutates only `tryPathSummaryStats` and `computePathStatsSequence`, with the release,
  bulk-summary and existing versioning suites. Fourteen mutants: 11 killed, three survived,
  none uncovered or timed out. All count-only admission guard mutations are killed.
  XML: `build/review-summary-purity/pit/mutations.xml`. This is focused method evidence,
  not whole-class mutation adequacy. All three survivors are unchanged count-return sites:
  - `tryPathSummaryStats`, line 23955, `NullReturnValsMutator`, instruction index 42:
    returning null after recording the proven-absent field falls through to the existing
    presence-count path, which also returns zero. This cannot turn a value aggregate into
    a summary result because the count-only gate precedes it.
  - The same method, line 23966, `NullReturnValsMutator`, index 81: discarding a cached
    structural count falls through to an exact presence count. Results and the summary
    computation counter are unchanged; the current tests do not distinguish the extra
    scan work. Not claimed work-equivalent, and no budget has been relaxed.
  - The same method, line 23972, `NullReturnValsMutator`, index 105: the computed structural
    count has already been cached and witnessed before the artificial null reaches the
    caller. An exact count fallback supplies this result; the repeat call uses the cache.
    The counter witnesses summary computation, not delivery, so this synthetic extra
    fallback survives. This is retained count-path coverage debt, not an uncovered purity
    decision or a claim that extra scanning is free.
- Path audit: projections still precede summary admission; no value aggregate probes the
  count cache. Refused summaries reach the existing typed aggregate cache/scan, where a
  nonnumeric contributor raises the existing refusal and the retained generic expression
  supplies its value/error. Structural counts still use exact named references, with the
  same absent/ambiguous/no-summary handling and immutable-revision cache. Write-transaction
  projection handling, predicate paths, writer kernels and row/page kernels are unchanged.
  The obsolete numeric-purity helper and unreachable value-statistics branches are removed.
- Every JVM command entered the supplied heavy wrapper. Root free space was 90 GB before
  Gradle. Formatting was limited to the three touched Java files; full repository test,
  lint, publication and CI gates remain with the outer executor. No work-budget/control
  bounds, mutation-selection policy or timestamp allocation tests were changed.
