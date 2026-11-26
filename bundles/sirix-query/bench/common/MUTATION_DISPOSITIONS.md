# Mutation dispositions of the benchmark protocol tools

Mutation record for [`timed_gate.py`](timed_gate.py), [`bench_trend.py`](bench_trend.py) and
[`bench_ledger.py`](bench_ledger.py), kept by the rules of the Java lane
([CONV-103](../../../../spec/CONV-conventions.md#conv-103--specification-before-implementation),
[CONV-201](../../../../spec/CONV-conventions.md#conv-201--mutation-disposition-record)): every
surviving mutant is killed by a behaviour test derived from CONV-302 to CONV-304, removed together
with code that had no observable effect, or listed below as equivalent with its reason. No test was
written to kill a mutant for its own sake.

## How the run was made

The whole-module figures below cover the introduction and initial review rounds, which altered
the allocation delta rule, the stop-point check, the conditions lookup, dataset parameters, commit
reconciliation and the rejection of a run. They predate the later focused review supplements below
and do not claim whole-module coverage of those corrections.

Each module was mutated one change at a time, and its own test module was run against every mutant
(`python3 -B -m unittest -f test_<module>`). A mutant is killed when that run fails. The mutations,
applied at every site of the module's syntax tree:

| Operator | Change |
|---|---|
| comparison | `<` ↔ `<=`, `>` ↔ `>=`, `==` ↔ `!=`, `in` ↔ `not in`, `is` ↔ `is not` |
| arithmetic | `+` ↔ `-`, `*` ↔ `/`, `//` → `*`, `%` → `*`, `<<` ↔ `>>` |
| boolean | `and` ↔ `or`, `not x` → `x` |
| constant | a number *n* → *n* + 1, `True` ↔ `False` |
| control | an `if` condition → `False`, `raise` → `pass`, `continue` → `pass`, `break` → `pass` |

String constants are not mutated, so the wording of messages is outside this record. The driver was
a short script over Python's `ast` module and is not part of the repository; any mutation tool with
these operators reproduces the sites. `test_kit_recording.py` took no part in the runs: it drives
the kit scripts and reaches the modules only through their command lines.

| Module | Mutants | Killed | Equivalent |
|---|---:|---:|---:|
| `timed_gate.py` | 254 | 246 | 8 |
| `bench_trend.py` | 112 | 108 | 4 |
| `bench_ledger.py` | 608 | 603 | 5 |

The first runs left 34 survivors in `timed_gate.py`, 22 in `bench_trend.py` and 113 in
`bench_ledger.py`. They were answered in three ways.

- **Tests of a requirement that had none.** Among them: the fence factor of CONV-304; levels
  compared as ratios whatever their size; the strict bounds of the drift rule and the inclusive bound
  of the stop rule, at floating-point inputs that reach them exactly; rejected allocation legs and
  legs outside the judged set; the counts in the report; the rejection of a whole run; a ledger
  never created by an empty append; a short write; the options every command requires; the exit
  status of the scripts.
- **A defect.** A benchmark data path that cannot be resolved, such as a symbolic-link loop, raised
  out of the capture instead of being recorded as unavailable. It is fixed and tested.
- **Code removed.** Conditions and options that changed nothing: a second check of an arm's name, a
  loop guard that could not be reached, a default that was never used, key sorting and character
  escaping options of files that are only ever read back as JSON.

The review rounds added code, and a re-measurement after them found seven more survivors. Five were
answered with tests: the number of rows an append reports, a run committed before it has a
measurement, and a malformed `--param`. One early return that changed nothing was removed. The
seventh is equivalent and is listed below.

## `timed_gate.py`: 8 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `split = count // 2` | `2` → `3` | The split only decides how the residuals are divided into two groups whose signed sums are then combined. Every division yields the same 2^(n−1) absolute sums. |
| `high = [total + residuals[-1] ...]` | `+` → `-` | Fixing the last sign to minus instead of plus negates every sum, and only absolute values are used. |
| `values = [abs(left + right) ...]` | `+` → `-` | The signed sums of the low group are closed under negation, so `left - right` ranges over the same values as `left + right`. |
| `@functools.lru_cache(maxsize=512)` | `512` → `513` | The size of a cache in front of a pure function. |
| `for boundary in boundaries[minimum - 1 : -1]:` | `minimum - 1` → `minimum - 2` | The scan for an earlier stop point would start one pair boundary sooner, at a boundary with fewer pairs than the minimum, where no benchmark can have met its stop condition. |
| `@dataclass(frozen=True)` on `Series`, `StopState`, `Judgement` | `True` → `False`, three sites | Immutability of result objects guards against misuse inside the module. No requirement observes it. |

## `bench_trend.py`: 4 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `if nights[0].night < night:` | `<` → `<=` | The branch is reached only when the series has no entry for `night`, so its first night never equals it. |
| `@dataclass(frozen=True)` on `Night`, `NightState`, `TrendReport` | `True` → `False`, three sites | As above. |

## `bench_ledger.py`: 5 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `subprocess.run(..., timeout=20, ...)` | `20` → `21` | The patience with a probe command; no probe in a test runs that long. |
| `os.open(path, ..., 0o644)` | `0o644` → `0o645` | One permission bit of a newly created ledger file; no requirement states its mode. |
| `if existing.read(1) != b"\n":` | `1` → `2` | The read starts at the last byte of the file, so asking for two bytes returns the same one. |
| `json.dumps(document, indent=2)` in `_write_json` | `2` → `3` | The indentation of `run.json` and `environment.json`, which are read back as JSON. |
| `json.dumps(fingerprint_fields(environment), indent=2)` | `2` → `3` | The indentation of the fields `bench_ledger.py fingerprint` prints. |

## Runtime option capture review supplement (R10)

The runtime-capture correction was checked separately from the historical module totals above.
All 90 tests in `test_bench_ledger` passed. `RuntimeVersionTest` then detected eight in-memory
mutations of the runtime filter: bypassing it (the pre-fix output), omitting each of the three
environment-variable names, omitting the `NOTE:` form, retaining only banners, removing whitespace
trimming, and returning empty text instead of unavailable. There were no survivors. The tests
derive their oracle from CONV-303's privacy and truthful-runtime requirements, using executable
launchers and persisted output rather than implementation text. Source files remained unchanged
during mutation execution. This supplement does not claim a new whole-module mutation count.

## Multiline capture, nightly date and CLI review supplement (R11–R13)

All 140 tests in `test_bench_ledger`, `test_bench_trend` and `test_kit_recording` passed before
the relevant behaviour tests detected 12 focused mutations:

- Runtime capture: restore the previous line filter; restore the previous probe environment;
  keep announcement continuations; keep a diagnostic without a version; inherit each of the three
  option variables separately (seven mutations).
- Nightly reporting: restore the previous selection from usable series; select the oldest run;
  ignore an explicit date (three mutations).
- CLI consumption: omit the shared count guard for `--arm-commit` or `--arm-kind` (two mutations).

Functions were substituted in memory and kit scripts were changed only in temporary test copies
with stub engines and a stub rig lock. Repository sources were not modified by the driver. There
were no survivors. The tests' oracles are CONV-303, CONV-304 and the CLI value contract; they assert
runtime evidence, report dates and error statuses, and rejection before any engine runs. The CLI
mutations reproduced a three-second timeout. Counts and counterfactual observations are recorded
in the [postmortem](../../../../docs/postmortems/benchmark-evidence-boundaries.md); historical
whole-module totals above remain unchanged.

## Empty run recording review supplement (R14)

All 21 tests in `test_kit_recording` passed. Both kits' first-process-failure regressions then
detected two mutations of the shared exit hook: restoring the previous measurements-file guard,
and inverting the opened-run guard. Each mutation caused both tests to fail because the latest
nightly run row was absent. There were no survivors. Tests derive their oracle from CONV-303's
run persistence and CONV-304's latest-night contract, checking ledger rows and the trend CLI's
date and status after a stub engine fails on its first invocation. Existing refusal cases and
the added unregistered ClickBench case confirm that an unopened run appends nothing.

Mutations ran in temporary copies with stub engines and a stub rig lock; repository sources and
the shared rig lock were untouched. This is a focused supplement, not a whole-module recount.
Details are in the [postmortem](../../../../docs/postmortems/benchmark-empty-run-recording.md).

## Nightly calendar review supplement (R15)

All 126 tests in `test_bench_ledger` and `test_bench_trend` passed. The four date regressions then
detected four in-memory mutations of the date predicate: restore the previous shape-only regex,
omit the canonical spelling comparison, accept failed calendar parses, and remove the string
type check. There were no survivors. The counterfactual regex produced 31 failed cases, confirming
that the regression tests observe the previous defect. Tests use Gregorian calendar examples and
CONV-303's canonical date contract as their oracle; they execute row validation, begin, append,
ledger reading and the trend CLI, without using implementation text as evidence.

Repository sources were unchanged during mutation execution. This is a focused supplement rather
than a new whole-module count. Details are in the
[postmortem](../../../../docs/postmortems/benchmark-night-calendar-validation.md).

## Deterministic allocation leg supplement

CONV-302's allocation-leg requirement added `_jvm_flag` and `_allocation_per_operation` to
`bench_ledger.py` and made `_jmh_row` divide an allocation leg's figure by its batch. The operators
above were applied one at a time to every site of those three functions, and `test_bench_ledger`
ran against each mutant in a temporary copy: 42 mutants, 34 of them on changed lines, all killed,
no survivors and no equivalents. The tests take their oracle from CONV-302's leg settings and from
JMH's single-shot result shape (bytes per batch); no test was written to kill a mutant. This is a
focused supplement, not a whole-module recount.

After requiring `-XX:-UseJVMCICompiler`, the same operators were reapplied to all sites of
`_jvm_flag`, `_allocation_per_operation` and `_jmh_row`, including the new Graal JIT clause.
`python3 -B -m unittest -f test_bench_ledger.ImporterTest` ran against each temporary mutant:
46 mutants, all killed, no survivors or equivalents. The refusal and last-effective-flag
regressions derive from CONV-302; this rerun measures only those three functions.

### Compiler follow-up root cause and verification

- Fix / issue: CONV-302's maintainer-approved HotSpot C2 requirement.
- Symptom / trigger: fresh JMH 1.37 processes on Oracle GraalVM 25.0.3 running
  `Arena.ofAuto().allocate(64, 8)` with `-f 1 -bm ss -bs 100000 -wi 3 -i 3 -prof gc`
  and the original documented flags measured 207.97144 versus 207.60112 B/op.
- Root cause: Graal ignores `-XX:-DoEscapeAnalysis` and retains partial escape analysis;
  the importer did not require a compiler that honors the allocation-as-written settings.
- Why tests missed it: reports exercised escape analysis, GC and batching without missing
  or overridden compiler selection.
- Regression: `ImporterTest.test_jmh_results_that_break_the_protocol_are_refused` and
  `ImporterTest.test_the_last_occurrence_of_a_jvm_flag_decides` failed before the fix
  (exit 1, two missing refusals). From this directory,
  `python3 -B -m unittest test_bench_ledger test_timed_gate` passed afterward
  (exit 0, 162 tests). The real CLI rejected both original JMH reports (exit 2 each)
  and accepted both unmodified C2 reports (exit 0 each).
- Residual: with `-XX:-UseJVMCICompiler`, the fresh processes measured
  212.04757333333333 and 212.04768 B/op, a difference of 32 bytes across three
  batches of 100,000 operations (about 0.0001 B/op). Both round to 212 B/op, meeting
  CONV-302's requirement of an identical whole-byte figure. The sub-byte residual is
  the one-time harness/lazy-initialisation allocation that the batch divides.
- Lesson: constrain the compiler that interprets optimization flags and judge fresh-process
  equality on the figure the check rounds.

## Nightly tier: `bench/nightly` and the nightly changes to the ledger and trend tools

Same method as above (AST operators, one change at a time, the module's own tests plus
`test_nightly_scripts.py`, killed when the run fails), run on the 2026-11-26 head of this change. For
`bench_ledger.py` only the lines this change touched were mutated (59 mutants, 59 killed). Every
behavioural survivor of the first rounds was answered with a test derived from CONV-303/304 or the
issue and window rules, not written to kill the mutant: the budget edges and the process order of
`nightly_run.py`, the 20-minute publication margin, the one-week issue dedup, the 2% report bound, the
three-night merge, the two-night confirmation and the ten-night recency of `nightly_analysis.py`.

| Module | Mutants | Killed | Survived (all dispositioned below) |
|---|---:|---:|---:|
| `bench_ledger.py` (changed lines) | 59 | 59 | 0 |
| `bench_trend.py` (whole file) | 141 | 137 | 4 (the three `frozen=True` of the existing dataclasses and `nights[0].night < night`, equivalent as before) |
| `nightly_window.py` | 69 | 69 | 0 |
| `nightly_run.py` | 165 | 165 | 0 |
| `nightly_publish.py` | 79 | 76 | 3 |
| `nightly_issues.py` | 86 | 84 | 2 |
| `nightly_analysis.py` | 138 | 131 | 7 |
| `nightly_dashboard.py` | 183 | 123 | 60 |

Equivalent, with the reason:

- **`sys.path.insert(0, …)` position** (every module): the directory is searched whichever position it
  takes, as nothing else on the path holds a module of these names.
- **`@dataclass(frozen=True)`** (`nightly_analysis.Detection`): no code mutates an instance; the
  property is a guard for later readers, not a behaviour.
- **`nightly_publish._git`, `text=True`'s message and the `[-400:]` tail**: the first is observable only
  as a `b'…'` prefix in an error message, which `test_a_git_failure_message_is_text_not_bytes` now
  forbids; the length of the kept tail of a git error is a display choice.
- **`nightly_issues`: `rsplit("/", 1)`'s limit**: with a larger limit the last element is the same.
- **`nightly_analysis`: `after > before` against `>=`**: equal levels cannot reach it, a change is at
  least 2% or starts from zero. **`… or state.since is None`**: `since` is `None` exactly for the states
  the first operand already excludes. **`series_id`'s `sort_keys` and `ensure_ascii`**: a key is a list
  of ASCII strings and pairs in a fixed order; the identity itself is pinned by a literal in
  `PolicyTest`. **The percent format's `100.0`** is asserted through the detail text of a 5% change.
- **`nightly_dashboard`, 60 survivors**: the layout of the charts: margins, padding of the value axis,
  tick spacing, label offsets, the choice of the middle date label, the markers' radii, the section
  and `figcaption` details. The tests assert what the page means (order and containment of the drawn
  line inside the plot, higher level drawn higher, day-proportional spacing, a release on either edge of
  the range, units at their boundaries, no external reference, escaping) and not pixel positions; the
  page was reviewed as a screenshot of a synthetic 45-night history. A mutant that moves a tick by a
  pixel changes no figure the page states.
- **`write_site` `mkdir(parents/exist_ok)`**: the output directory is created by the command line tests
  in a fresh temporary directory; a pre-existing directory is the workflow's case and is exercised by
  the end-to-end run.

`CpuTimeProfiler.java` has no unit test (`:sirix-benchmarks` has no JUnit source set). It was run for
real: 20 fresh processes of three benchmarks through `nightly_run.py` into a ledger that
`bench_ledger.py`, `nightly_analysis.py` and `bench_trend.py` read back; its CPU figure agrees with the
wall-clock figure of the single-threaded benchmarks to within the noise.
