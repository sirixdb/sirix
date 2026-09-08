# Diagnostic JVM trace

The Java child exited 0 with all 129 timings; the observer reported a teardown permission race. Its original failed controller verdict is preserved. This is diagnostic evidence only, never a scored calibration leg.

The exported JFR excludes unrelated InitialEnvironmentVariable and InitialSystemProperty events. All query/JIT/GC observations remain; original local recordings are unchanged. See export-redactions.json for the transformation and hashes.

The subsequent GC-switch plan was **canceled**, not completed. See `gc-switch-cancelled.json`; one on leg completed and no off JVM launched. The plan and launch scripts are retained solely as provenance. Do not run that canceled experiment from this archive.

`raw.tar.gz` held a whitelist of logs, JFR, receipt boundaries and resource observations. **It was discarded from this deliverable and cannot be regenerated**; see [RETENTION.md](../RETENTION.md). `raw-members.json` still records every member and its exact hash, so the archive's contents remain named. `analyze-original.py` and `ReadMainSamples.java` are retained as the provenance of the analysis, but they have no input here and the JFR replay they describe can no longer be run.

`analysis.json` is the retained output of that replay. It records exact metric definitions and the query-boundary lag caveat, and it is the evidence the report rests on. The [report](../../../../../../../docs/CLICKBENCH_RIG_JVM_DIAGNOSTIC_2026-09-08.md) separates observations from causal hypotheses. `gc-switch-plan.json` is the prespecified follow-up, not a completed result. Launch controllers preserve original paths as provenance, not portable launch instructions.
