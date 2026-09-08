# Split cooldown study, 2026-09-08

Twenty fixed unchanged-code composites, q0..20 and q21..42 in separate JVMs. This is protocol calibration, not an engine improvement or task completion.

`summary.json` retains all three-try observations, per-query and suite statistics, modality screens, ten chronological A-A pair estimates, and restart-boundary comparisons with the separate quiet full-suite control. The CSV files make every query inspectable. `manifest.json` records raw and exported hashes.

`raw-study.tar.gz` held the raw logs and the `analyze-split.py` analyzer. **It was discarded from this deliverable and cannot be regenerated**; see [RETENTION.md](../RETENTION.md). Recomputing this study from raw logs is no longer possible, and `summary.json` is the retained output of that analysis.

Protocol cohorts are sequential. Differences combine restart, JIT, residency, page cache and thermal effects; they cannot identify a single cause. Lower score variance does not establish sensitivity to real CPU-side changes. The cache-cleared and CPU-perturbation studies remain separate requirements.
