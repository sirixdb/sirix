# Live smoke: one real 100M leg through the current harness

One scored leg, taken to prove the hardware-gated paths execute, pass and release on real
hardware. It is **not** a study: one leg cannot estimate variance, cannot support a candidate
claim, and says nothing about whether 0.5 ln is resolvable. Read
[`../quiet50-20260908`](../quiet50-20260908) and
[the variance study](../../../../../../../docs/CLICKBENCH_RIG_VARIANCE_2026-09-08.md) for those.

## What ran

| | |
|---|---|
| Harness | this checkout, `source_commit 916d85ee2cea32b15a19e22c17c913de66a52321` |
| `runtime_id` | `d70e4d30a017d662894b8c776473160f41c496887d95f304122dbdf47289ea72` |
| Working tree at freeze | two Python test files modified; no engine or harness source change |
| JDK | Oracle GraalVM 25.0.3+9.1, `bin/java`/`lib/modules`/`libjvm.so`/`release` hashed |
| Database | `clickbench-seg100m-20260905-2328/db`, opened read-only, never loaded |
| Protocol | all 43 queries, three tries, one JVM, inherited page cache |

Built and frozen with `measure.py prepare`, then measured with `measure.py run --runtime` so the
frozen manifest was re-verified — every classpath artifact and the JDK re-hashed — before the JVM
started. `leg-command.json` is the exact argv; `leg-plan.json` records the runtime and protocol.

## Gates that fired, passed and released

- **Exclusive host lease.** Both production paths (`/tmp/sirix-clickbench-<uid>.lock` and the
  `CB_RIG_WORK` leg lock), re-verified against `/proc/self/fdinfo` before the launch and inherited
  by the JVM through `pass_fds`. After exit no process held either lock and both files still
  existed, unmodified — release is by descriptor close, never by unlinking.
- **`wait_for_quiet_java`.** Passed with no other JVM alive. Three idle Gradle daemons had to be
  stopped first; that is the normal cost of measuring on a development box, and the gate is what
  makes it visible instead of silently contending.
- **Cooldown and launch recheck.** Released after three consecutive package readings at 45 C, five
  seconds apart, with the final recheck also below the cutoff (15.0 s).
- **MSR power gate.** Both pinned limits read 50 W in every one of the 334 telemetry samples.
- **Platform-domain observation.** The MMIO long-term limit moved **seven times inside this single
  leg**, unprompted, across 45.0 / 45.75 / 49.0 / 49.375 / 58.0 / 66.0 / 76.0 W. It was recorded
  and the leg stood, which is the whole point of the split: an equality gate on this
  firmware-managed value would have aborted a ten-pair collection seven times over in 170 seconds.
  Nothing here tuned any cap, governor, frequency or thermal setting.
- **Live process/policy watch.** No other Java workload and no CPU-policy change during the run;
  observer issue list empty, exit code 0.

## Outcome

Complete and scorable: 43 queries x 3 tries with no missing timing, 38.350 s total suite hot time,
`sum_ln` 65.027, geometric mean 4.537. As descriptive context only, that lands inside the range of
the 20 recorded quiet-50 leg scores, 63.152 .. 66.175 ln. **A score landing inside a historical band
does not establish that the two runs shared physical measurement conditions**, and here they
demonstrably did not. The [power audit](../power-audit-20260908) records the quiet-50 legs under
*mixed* MMIO long-term limits, the readings it enumerates running from 45 to 52.625 W; this leg
observed 45 .. 76 W within its own 170 seconds. Neither is one uniform 50 W envelope. This leg's own
recorded conditions were hot and heavily throttled — mean 95.4 C, 11252 package throttle events,
mean reported 2.57 GHz — and all three fall beyond the corresponding ranges over the twenty legs in
the retained quiet50 findings: hotter, more throttled and
lower-clocked than any of them.

What this leg does establish is narrow: under the conditions recorded here, the gates above fired,
passed and released; a real JVM measured the real 100M database end to end; and telemetry capture
and the lease lifecycle behaved as designed for one run. **This single observation is not an effect
estimate.**

The raw telemetry and query-boundary data were archived off-tree on 2026-09-09 in
`build/rig-evidence-archive-20260909.tar.gz`; the findings remain retained here.

## What this did not test

- **Variance, resolution and candidate effect.** One leg has no pair, no interval and no verdict.
  The 0.5 ln resolution question is untouched.
- **The paired collection path.** `compare`, `prepare_revision`'s two-worktree build, the scratch
  retention and release behaviour, and cross-arm JDK/dependency/harness verification were exercised
  by unit tests and by replay, not by this run.
- **Gate failure paths.** Every gate here *passed*. Their rejection behaviour is covered by the
  Python tests; the box was deliberately not heated, loaded or reconfigured to force a failure.
- **Prior replay work remains synthetic.** The reconstructed and injected-effect exercises,
  including the 60-pair fixture, drive the real answer path with recorded and modified timings.
  They are answer-path tests. They are not additional physical measurements and they do not
  demonstrate that a real 60-pair collection would reach the target precision.
