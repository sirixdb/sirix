# Diagnostic JVM trace

The Java child exited 0 with all 129 timings; the observer reported a teardown permission race. Its original failed controller verdict is preserved. This is diagnostic evidence only, never a scored calibration leg.

The exported JFR excludes unrelated InitialEnvironmentVariable and InitialSystemProperty events. All query/JIT/GC observations remain; original local recordings are unchanged. See export-redactions.json for the transformation and hashes.

The subsequent GC-switch plan was **canceled**, not completed. See `gc-switch-cancelled.json`; one on leg completed and no off JVM launched. The plan and launch scripts are retained solely as provenance. Do not run that canceled experiment from this archive.

The raw logs, JFR inputs and replay material were archived off-tree on 2026-09-09 in
`build/rig-evidence-archive-20260909.tar.gz`; this retained finding is not replayable in-tree.

The retained analysis output records exact metric definitions and the query-boundary lag caveat, and
the [report](../../../../../../../docs/CLICKBENCH_RIG_JVM_DIAGNOSTIC_2026-09-08.md) separates observations from causal hypotheses.
