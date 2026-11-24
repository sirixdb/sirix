# Bug: impossible nightly dates could hide newer measurements

- Fix / issue: review finding R15 on `fm/sirix-perf-gate-statistics`; CONV-303 and CONV-304.
- Symptom: an impossible date such as `2026-13-01` could enter the append-only ledger and sort after
  valid December dates, making the default trend report repeatedly select stale evidence.
- Trigger: open or append a nightly run whose night has the right number of digits but is not a
  calendar date; record a later valid December run and request the default trend report.
- Root cause: row validation checked the spelling with a regex but not calendar membership.
  The shared row validator now requires a successful standard-library calendar parse whose canonical
  `YYYY-MM-DD` spelling equals the supplied value. The regex is removed. Creation, append, run
  loading and ledger reading all use this boundary, so the trend CLI refuses an existing bad row.
- Why tests missed it: invalid-night coverage used a visibly different date format, while trend
  fixtures generated valid dates. Conditions-record dates already had a calendar check, masking the
  inconsistency between the two sources of dates.
- Regression tests: `RowSchemaTest.test_nightly_dates_are_real_canonical_calendar_dates`,
  `RowSchemaTest.test_invalid_nightly_dates_cannot_be_appended_or_read`,
  `RunDirectoryTest.test_an_invalid_night_is_refused_before_opening_a_run`, and
  `ReportTest.test_a_ledger_with_an_impossible_night_is_refused`.
  The independent oracle is the Gregorian calendar and CONV-303's exact date representation:
  2000 and 2024 have a February 29; 1900, 2026 and 2100 do not; April has 30 days; year zero is invalid.
  Tests also require canonical spelling, valid boundary years, refusal without run or measurement
  files, unchanged existing ledger bytes, no new ledger on invalid append, and trend error status 2
  for an invalid row surrounded by valid December runs. They reject shape-only validation and
  silently accepting compact or week-date spellings.
- Lesson: [LESSONS.md](../LESSONS.md), via `lessons.d/benchmark-night-calendar-validation.md`.
- Vision check: valid measurement identities protect the engine's cost evidence. No acceptance
  floor, precision, pair limit, quantile, allocation rule, nightly constant or engine default changes.

Focused verification: all 126 tests in `test_bench_ledger` and `test_bench_trend` passed in
20.033 seconds. The equivalent command, from `bundles/sirix-query/bench/common`, is
`python3 -B -m unittest test_bench_ledger test_bench_trend`. The same driver then ran the four
regressions with in-memory substitutions of the date predicate: the previous shape-only regex
produced 31 failures; omitting canonical spelling produced 6; accepting invalid parses produced 46;
removing the string guard produced 9 errors. All four mutations were detected, with no survivors.
Repository sources remained unchanged during mutation execution. The focused supplement is in
[MUTATION_DISPOSITIONS.md](../../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md).
