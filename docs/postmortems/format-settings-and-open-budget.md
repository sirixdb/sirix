# Bug: settings fallback skipped version types; discovery had no work bound

- Fix / issue: FMT-401, CONV-301; `review-numeric-settings-version` and
  `vision-open-discovery-budget`.
- Symptom: an unreadable derived settings file containing `binaryEncoding` as a number,
  object, array, boolean or null could authorize destructive rebuilding. Separately, a
  correct format gate could grow its cold-open I/O without failing a budget.
- Trigger: put an unexpected field before `binaryEncoding:7` in otherwise compatible
  derived settings, then open the source on either persistent backend. Ordinary settings
  parsing refuses numeric 7, but the fallback skipped every non-string token.
- Root cause: two entry points applied different value validation to the same persisted
  member. The fallback equated a non-string version with no version at all. The shared
  parser now rejects every present value except the exact 1.0 string, without coercing
  object/array/boolean/null tokens into an absent member. Numeric tokens retain their
  value in the diagnostic. Field order does not change the result.
- Why tests missed it: the foreign fixtures used strings; the positive repair matrix even
  treated null as absent. Existing sparse-history budgets count metadata loading, explicitly
  excluding compatibility discovery during normal opens.
- Regression tests: `FormatRepairBoundaryTest.everyPresentInvalidSettingsVersionPreventsDerivedRebuild`
  covers all JSON types, scalar variants, both field orders and both backends, asserting
  the plain refusal and unchanged whole-resource files. Compatible `V0` and genuinely
  absent/unreadable members retain their positive repair tests. The exact failing sequence
  was traced before editing; this phase applies all fixes before its focused test run.
- Work oracle: FMT-102, FMT-106 and FMT-107 define 64-byte identities, 32-byte records and a
  16-entry ring in 4096-byte beacon slots. `FormatDiscoveryWorkBudgetTest` permits one read
  of both identities and one beacon, plus the latest root and the 16-root window: at most
  17 record slots and 17 framed envelopes. The byte allowance uses the persisted frames'
  length fields as the owned byte contract, independently capped at 512 payload bytes for
  each scalar fixture root. It does not derive an allowance from measured discovery work.
- Cost coverage: real normal opens after `Databases.clearGlobalCaches()` exercise 20 and
  80 committed revisions on each explicitly named backend. Test-only channel delegates
  count actual transferred bytes, revision record bytes and envelope-length reads within
  the shared gate; no production instrumentation or timing is added. Repeating the real
  gate inside the same open preserves the returned content but must fail each bound,
  providing a positive control against disconnected or vacuous counters. Later storage
  metadata preload is outside this discovery-specific budget and retains its existing budget.
- Remaining limits: this does not measure latency, test every recovery cost or close the
  `sirix-repair-format-gate` complete-discovery gap. Incomplete-frame rules, proof aliases,
  golden bytes and FMT-402's three gaps are unchanged. Mutation, lint and broader validation
  remain with the outer pipeline; no local evidence here claims those phases passed.
- Lesson: use one value validator across ordinary and repair entry points, and budget new
  cold discovery at its real I/O boundary; see [LESSONS.md](../LESSONS.md).

## Focused verification

The focused `:sirix-core:test` run passed all 178 selected cases with no failures, errors or
skips (exit 0): format repair, ordinary configuration round-trips, session-factory contracts,
bootstrap and feature recovery, both golden suites, version diagnostics, the new discovery
budget, and existing sparse-index/scratch budgets. Unrelated startup tasks were excluded;
the test heap was 256 MiB/2 GiB and `sirix.workBudget.print=true` recorded the captures.
Log: `build/review-settings-open-budget.log`; JUnit XML:
`bundles/sirix-core/build/test-results/test/`.

For both backends at both history sizes, healthy discovery read 9,851 bytes, 17 records and
17 envelopes. Injected duplicate discovery read 19,702 bytes, 34 records and 34 envelopes;
each corresponding budget assertion failed as required by the positive control. These are
counted-work results, with no wall-clock performance claim. The original defect was traced
statically rather than run counterfactually because this phase required all fixes before
focused verification; the added positive control executes actual additional discovery I/O.

## Vision check

Contract class: **restore**, implementing the required FMT-401 refusal boundary and the
maintainer-requested CONV-301 cost guard; no setting, format or public API default is added.
This records the contribution template's review evidence for the outer executor.

| Rule of the vision | Verdict | Evidence from the diff |
|---|---|---|
| The advantages to protect | aligns | The change leaves revision layout and lookup algorithms alone; `FormatDiscoveryWorkBudgetTest` bounds discovery at both history sizes. |
| History is the data model | aligns | `everyPresentInvalidSettingsVersionPreventsDerivedRebuild` requires unchanged resource files on refusal; compatible repair remains exercised. |
| Speed is part of correctness | aligns | The new byte/record/envelope budget and its repeated-discovery control guard cold work on both backends; existing bounds are unchanged, with no latency claim. |
| A fast answer is the same answer | aligns | The shared value reader fails closed; the budget also checks the known committed string returned after open. |
| The specification decides behaviour | aligns | FMT-401 states the any-JSON-type rule; CONV-301 and the budget README register the independent cost bound. Mutation associations connect the behavior tests; the outer pipeline owns mutation validation. |
| One format, one route | aligns | Both settings entry points use one value reader accepting only the exact 1.0 string; persisted encodings and golden pins are untouched. |
| A library first | aligns | Changes stay in core parsing and tests; deployment and application surfaces are untouched. |
| Scope | aligns | The test observes the existing single-node open path; no repair scan or external service is added. |
