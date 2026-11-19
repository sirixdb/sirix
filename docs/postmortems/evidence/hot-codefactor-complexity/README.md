# CodeFactor complexity follow-up evidence — Refs #1452

**Supplementary 24-block latency follow-up PASS.** All 96 JVMs completed and all five cases
meet the experiment's fixed analysis rule, with zero timed GC. These latency runs
are supplementary due diligence, not delivery acceptance gates; the required
re-measurement is allocation only. The original four-block trial remains
a separate HOLD; both results and the interrupted attempt are retained. Three
production methods were refactored.
After an initial disk-floor hold, space recovered and fresh quiet admission passed;
the single exclusive comparison began at `2026-11-19T00:52:05Z`.
CodeFactor's remote verdict remains for the outer executor after publication.
The [report](../../hot-codefactor-complexity.md) records behavior, results,
allocation changes, limitations and the fixed timing protocol.

| Artifact | Contents |
|---|---|
| [Timing](timing.tar.gz) | All 16 JVM results, raw measurements and counters, complete-run analysis, frozen hashes, source comparison, and resource/command provenance; overall HOLD |
| [24-block timing](timing-24-blocks.tar.gz) | All 96 JVM results, measurements, counters, frozen hashes, admission, commands, preregistration and complete PASS analysis |
| [Interrupted attempt](timing-interrupted-029.tar.gz) | Scheduling-interrupted attempt: 58 complete JVMs and partial next log, unanalysed; never pooled into either completed trial |
| [Fresh local verification](local-verification-and-shape.tar.gz) | Local focused/allocation and work-budget XML/logs, source/class identity, fresh compiled-shape and helper-inlining captures |
| [Allocation](allocation.tar.gz) | All 160 operations' normalization/fold allocation stacks, full-operation counters, per-operation categories, controls, commands, exact pinned-budget checks, whole-process summaries and hashes of complete raw stacks |
| [Compiled shape](compiled-shape.tar.gz) | Baseline/candidate `javap`, all captured compilation profiles and inlining, exact commands, hashes and setup-failure logs; diagnostic timing is not a latency result |
| [Verification](verification.tar.gz) | Focused/budget JUnit XML, all three overlapping PIT reports, final summary, build/test/PIT logs and counts |
| [Replay/provenance](replay-and-provenance.tar.gz) | Harness source, scripts, local Gradle/PIT scopes, protocol, source/class/dependency hashes and integrity/HOLD record |

[manifest.json](manifest.json) pins the compressed archives. The complete unfiltered
allocation archive remains in `build/codefactor/full-allocation-evidence.tar.gz`,
SHA-256 `db03e0f69a317b998608c753cb6ed12b82ab14a7ceecb7cf3c6488173e59ae13`.
Its 593 MiB expanded runtime traces stay in `build/codefactor/allocation/`.
The repository artifact retains every normalization row plus the separate
whole-process counters/totals; unrelated background stacks are not vendored.

[Mutation dispositions](mutation-dispositions.md) enumerate every remaining
survivor/uncovered mutant. Final count: 179 generated, 164 killed, 10 survived,
5 uncovered. Earlier runs overlap and are not added to this total. Compatibility
and untouched-overload coverage debt is explicitly not claimed equivalent.

## Local commands and custody

The repository sources are the patch; the archive's `refactor.py` is a historical
edit journal and is not a replay command. Generated evidence packages are not
new CI configuration. Analyzer rules and pinned allocation bounds are unchanged.

Commands ran through the required shared wrapper:

- `bash build/codefactor/run-focused.sh`
- `bash build/codefactor/run-budgets.sh` (the full Work budgets block)
- `bash build/codefactor/run-pit.sh` (scoped to all changed methods/helpers)
- The final zero-slot strengthening used the same PIT scope, with
  `:sirix-core:spotlessApply` and report directory `build/codefactor/pit-zero-report`.
- `heavy.sh python3 build/codefactor/run-allocation.py`, then the Python allocation
  analysis and fixed-budget checker.
- Fresh diagnostic JVMs and `javap` ran inside `heavy.sh`; failed fixture/classpath
  setup attempts are retained and were not used to select latency results.

The wrapper is
`/home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh`.
No pipeline control, push, PR edit, merge or external cleanup occurred.

## Timing provenance

The original 4-block trial completed at `2026-11-19T01:39:25Z` and remains
HOLD. Its archive records the historical commands and frozen runtime.
The supplementary 24-block trial used the unchanged candidate and
the same analysis rule, with df 23. See the [protocol](protocol-24-blocks.md)
for the preregistration and recorded scheduling changes. The final launcher
used `FM_TIMING_CPUS=0 timing-window.sh` directly, never nested in `heavy.sh`.
Ordinary build and diagnostic JVM work used `heavy.sh`.

The 24-block archive contains the exact launcher, runner and analysis scripts,
per-JVM command arrays, symmetric ordered classpaths, source/runtime hashes,
quiet-admission readings, all raw results, and complete analysis. Frozen runtime
files remain in the external task proof under `local-codefactor/`; no second
checkout supplied production classes. Both production path names have equal
length. Local compilation matched all 1,854 frozen candidate class files.

These commands and scripts are replay provenance, not authorization to repeat
a trial. The 24-block result passes the experiment's registered analysis rule;
broad commit intervals do not establish equivalence within 1% or a precise speedup.
Exact pinned allocation budgets remain unchanged, and CodeFactor must still pass remotely.
