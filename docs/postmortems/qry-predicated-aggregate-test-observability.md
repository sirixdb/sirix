# Bug: Predicated aggregate acceptance tests observe the wrong boundaries

- Fix / issue: review findings R1–R7 on `fm/sirix-qry-predicated-agg-tests`.
- Symptom: the planned QRY suite could reject a valid warm or short-circuiting route while
  accepting stale result caches, extra anchorless-page reads, an incorrect startup default,
  or loss of routing inside an object expression.
- Trigger: warm P/4P scans; repeated queries with changed literals, paths, flags, functions,
  numeric fields or revisions; repeated refusal; nested scores on filtered records; skipped
  exceptional values; an unaligned anchorless tail; fresh-process startup; composed results.
- Root cause: executor ownership ended before warm-up or cache reuse could be observed;
  fixture predicates excluded competing values; outcome sets treated skipped values as evaluated;
  a baseline-derived allowance hid anchorless work; direct field switching and scalar-only
  evaluation substituted for the actual startup and composed-result boundaries.
- Why tests missed it: scalar answer comparisons did not establish a live warmed scan, retained
  cache ownership, a contributing scope competitor, an exact zero-work boundary, startup property
  initialization, or consumption and route attribution of object members.
- Regression tests: `RegionPredicatedAggregateAllocationBudgetTest` retains its warmed executors,
  clears only result caches and requires serving on each measured scan;
  `RegionPredicatedAggregateVersioningTest` retains owners across each distinguishable query
  sequence and checks route activity; `refusalPublishesNoPartialResult` repeats on one owner;
  `nestedScoresOccurOnMatchingRecords` verifies the scope competitor contributes;
  differential T3/T5/T6 rows allow skipped-value servicing while preserving interpreter equality;
  the work-budget test verifies the tail boundary and requires an exact zero-read delta;
  `RegionPredicatedAggregateStartupTest` crosses three startup settings with both independent
  array-switch settings; `composedObjectServesAdminSum` consumes every member and compares
  full-object serving with the standalone sum and the other members.
- Evidence limits: these are specification-first tests for a route that does not exist yet.
  Route regressions cannot be demonstrated against an implementation in this change. They remain
  labelled planned at QRY step 3, with unavailable bindings confined to `QrySeam`; compilation and
  the existing interpreter checks are the focused executable checks available today.
- Vision check: protects interpreter-equal results and counted query cost, including historical
  revisions, without changing production behavior, defaults, or fixed work/allocation bounds.
- Lesson: test the lifetime and executable boundary named by a requirement, and make adversarial
  fixtures observably distinguish the wrong behavior; see [Lessons](../LESSONS.md).
