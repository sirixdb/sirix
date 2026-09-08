# Quiet-machine fixed-50-W control, 2026-09-08

This is the completed 20-leg unchanged-code control. It is not task completion or evidence that a candidate improved.
The [report](../../../../../../../docs/CLICKBENCH_RIG_VARIANCE_2026-09-08.md) explains the variance and resolution limits.

- `summary.json` includes every three-try timing, per-try CPU/GC resources, per-query and suite statistics, modality screens, sequence correlations, and the ten chronological null-pair differences and planning estimates.
- `query-summary.csv` and `hot-resources.csv` make every query inspectable.
- `background-*` preserves the separately labeled background-exposed and transition cohorts. The sequential cohort comparison cannot establish a causal background-load cost.
- `manifest.json` records provenance and hashes.
- `raw-study.tar.gz` held the selected raw logs, telemetry, original launch commands and the
  `study-analysis.py` / `contrast-load.py` / `plot-study.py` / `write-study-report.py` analysis
  scripts. **It was discarded from this deliverable and cannot be regenerated**; see
  [RETENTION.md](../RETENTION.md). `manifest.json` still records its hash under `export_sha256`.

Recomputing the summaries from raw logs is therefore no longer possible. `summary.json` is the
retained output of that analysis and keeps every three-try timing, per-try resource observation,
per-query and suite statistic, modality screen, sequence correlation and null-pair estimate, so
every reported number stays inspectable. The interrupted `flat50-03` was never scored.

The historical pre-cap and paused-study evidence is preserved separately in `../thermal-20260908/`.
