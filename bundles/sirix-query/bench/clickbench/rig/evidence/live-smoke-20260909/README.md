# Live smoke: the hardware-gated collection path at the child-decision head

One real JVM against the campaign 100M database, taken to prove the hardware-gated collection
paths execute, pass and release at this head — and, specifically, that the measured JVM now
consumes the campaign-identity **decision** its launcher exported rather than re-deriving one.

It is **not** a study and **not** a score. Three queries is not a suite, one leg cannot estimate
variance, and no number below may be quoted as a measurement result. For variance and effect
estimation read [`../quiet50-20260908`](../quiet50-20260908) and
[the variance study](../../../../../../../docs/CLICKBENCH_RIG_VARIANCE_2026-09-08.md).

The earlier [`../live-smoke-20260908`](../live-smoke-20260908) capture is the full-suite precedent,
taken at harness commit `916d85ee2`. It predates the child-environment change, which alters the
environment every measured JVM is launched with, so it could not evidence this head.

## What ran

| | |
|---|---|
| Harness | this checkout, `source_commit fff8e1113fc2bbc607355644ef7a2d8ef7b4aac6` |
| `runtime_id` | `253cf5bfa9de07c5470bfbc3085e55fe38a8b8941d2b1dee9a935b2f032064eb` |
| Working tree at freeze | the child-decision change under test; no engine source change |
| Database | `clickbench-seg100m-20260905-2328/db`, opened read-only, never loaded |
| Classification | `campaign`, `decided_by: campaign-database`, via a resolved `CB100M_DIR` |
| Envelope | the mandatory campaign envelope: `-Xms6g -Xmx14g`, 10 GiB arena, 5 GiB eager |
| Run | `measure.py run --diagnostic --queries 0,19,39 --tries 3 --runtime <frozen>` |

Frozen with `measure.py prepare`, then measured with `measure.py run --runtime`, so the manifest was
re-verified — every classpath artifact and the JDK re-hashed — before the JVM started.
`leg-command.json` is the exact argv and `leg-plan.json` records the runtime and protocol.

## The decision reached the JVM

The line the measured process printed, from `leg-suite.log`:

```
# rig lease: pid=2045703 mode=exclusive db=…/clickbench-seg100m-20260905-2328/db
  campaign=…/clickbench-seg100m-20260905-2328/db via=CB_RIG_CLASSIFICATION host=/tmp/sirix-clickbench-1000.lock
```

`via=CB_RIG_CLASSIFICATION` is the point of this capture: the JVM consulted **no** pointer. It took
the classification its launcher had already reached, having verified that the exported conclusion
names the database this process actually opened. `mode=exclusive` follows from that decision, and
with it `LOCK_EX`, the 100M envelope validation and the legacy-process refusal — the three things a
demoted run silently skips.

## Gates that fired, passed and released

- **Exclusive host lease.** `/tmp/sirix-clickbench-1000.lock`, taken by the Python launcher,
  re-verified against `/proc/self/fdinfo` before launch and inherited by the JVM through `pass_fds`.
  After exit no process held it and the file still existed, unmodified — release is by descriptor
  close, never by unlinking.
- **`require_no_benchmark` / `wait_for_quiet_java`.** Passed with no other JVM alive.
- **Cooldown and launch recheck.** Released after three consecutive package readings below 55 C,
  five seconds apart, with the final recheck at 45.0 C (`leg-cooling.jsonl`, 15.0 s).
- **MSR power gate.** Both pinned limits read 50 W in every one of the 25 samples; `check_power`
  ran at every cooldown sample, every telemetry tick and once more after exit.
- **Platform-domain observation.** No platform-managed limit moved during this run — every RAPL
  domain held one value throughout (`leg-telemetry-summary.json`). This differs from the 20260908
  capture, where the MMIO long-term limit moved seven times; both are recorded rather than gated on,
  which is the point of the split. Nothing here tuned any cap, governor, frequency or thermal
  setting.
- **Live process/policy watch.** No other Java workload and no CPU-policy change during the run;
  observer issue list empty, exit code 0.
- **Leg log parsing.** `measurement.read_timings` was run over this log for queries 0, 19 and 39 and
  returned all nine timings with no missing or duplicate entry.

## Read-only

The full listing of the 48 GB campaign database — every path, size and mtime — was taken before and
after the run and is byte-identical (`sha256 8e9e3990…`, 15 entries). No load ran, nothing under the
database or the shared diagnostics directory was created, moved or deleted.

## What this does and does not establish

Establishes, narrowly: at this head, under the conditions recorded here, the gates above fired,
passed and released; a real JVM opened the real 100M database and answered real queries; the lease
lifecycle behaved as designed; and the measured process took its classification from the launcher's
exported decision rather than re-resolving one.

Does not establish anything about performance. The timings in `leg-suite.log` (q0, q19, q39 at three
tries) are a path exercise. Three queries are not a suite, one leg is not an estimate, and the run
was thermally hot — 98 C peak, 109 package throttle events in ten seconds. **No number in this
directory is a campaign score and none may be quoted as a measurement result.**
