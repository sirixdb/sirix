# CodeFactor complexity follow-up evidence — Refs #1452

**Latency follow-up in progress.** Three production methods were refactored.
After an initial disk-floor hold, space recovered and fresh quiet admission passed;
the single exclusive comparison began at `2026-11-19T00:52:05Z`.
CodeFactor's remote verdict remains for the outer executor after publication.
The [report](../../hot-codefactor-complexity.md) records behavior, results,
allocation changes, limitations and the fixed timing protocol.

| Artifact | Contents |
|---|---|
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

## Remaining fixed follow-up

The current worktree retains frozen `build/codefactor/before` (published
`9732747e47fc549960256af9d4516644badfda1e`), `after_`, harness `classes`, and
`shared-runtime` directories. Complete source/class/dependency hashes accompany
them. The two benchmark Java sources are byte-identical to the task proof.
Project runtime directories were frozen to prevent concurrent test compilation
from changing the service-provider classpath during a measurement.

Only after restoring at least 60 GiB free, obtaining the original quiet admission
(three consecutive 30-second observations: one-minute load <4, five-minute <6),
and allowing the full run before `2026-11-19T02:30Z`, the outer executor can run
this single planned comparison from the worktree root:

```sh
/home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh \
  python3 build/codefactor/inside-heavy-hft-replication-timing.py \
  python3 build/codefactor/run-measurements.py timing \
  > build/codefactor/timing-driver.log 2>&1
python3 build/codefactor/analyze-timing.py > build/codefactor/timing-analysis.log
```

This upgrades the inherited timing lock; it does not nest/bypass the wrapper.
The runner pins CPU0, uses symmetric production substitution, checks the 60 GiB
floor and frozen bytecode, and stops before the cutoff. The parameters remain
4 ABBA blocks, 16 fresh JVMs, five cases, 30s real + 50 JMH warmup batches,
36 retained measurements, default asynchronous C2; df3 intervals and the original
point/lower-bound/zero-timed-GC decision. No adaptive rerun or tuning is authorized.
The one `timing` invocation is in progress; it must not be launched again. A
missed or inconclusive window retains HOLD; prior timing hashes do not cover
these changed kernels.
