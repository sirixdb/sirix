# Parked by captain, 2026-09-08

The task is incomplete and paused at firstmate inbox 007. No new leg may start until resumed.

- Query-engine and committed harness source remain unchanged from `aa4d81d54`.
- The initial unchanged series completed three legs before firstmate's thermal re-plan.
- Cool-start, transition, and observed-65-W probes are preserved.
- The later variance study at verified flat PL1=PL2=50 W completed `flat50-01` and `flat50-02`.
- `flat50-03` was interrupted during q28 (last completed resource line: try 1) by terminating only this task's query JVM. Its scheduler
  and passive observer were stopped. Its partial log must not enter scoring or variance estimates.
- No further 100M process was launched. The rig lock is released.

The latest instructions make **variance**, rather than a zero throttle count, the acceptance
criterion. Capped runs steer changes using repeated paired confidence intervals; uncapped runs
serve publication. Target resolution is approximately 0.5 ln. Cross-regime sign transfer needs
measurement, not assumption. Earlier reports in this directory retain the prior thermal-gate
framing and are historical evidence, not the final acceptance verdict.

All original working artifacts remain under
`bundles/sirix-query/build/diagnostics/rig-trust/` on the machine that measured them, including raw
telemetry, logs, launch commands, probe/series scripts, analysis scripts and `PARKED.json`. Nothing
in that working directory was deleted. `parked-working-evidence.tar.gz`, the committed copy of it,
**was discarded from this deliverable**; see [RETENTION.md](../RETENTION.md). It captured the
pre-cap power regime, so it cannot be recaptured from today's machine state, and any reader without
access to that original working directory has only the derived files in this directory.

On resumption, first read the inbox and verify the power regime and rig ownership. The next
measurement step is to finish a sufficiently repeated unchanged capped baseline without
reusing or overwriting the partial `flat50-03` directory. Then investigate remaining query
instability and implement the shared process-owned 100M lock, repeated-pair uncertainty,
per-query ln deltas, and regime labels. Validation and the no-mistakes shipping gate remain
outstanding. Do not treat this evidence checkpoint commit as task completion.
