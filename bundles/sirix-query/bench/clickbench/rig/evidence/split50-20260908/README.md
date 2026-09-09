# Split cooldown study, 2026-09-08

Twenty fixed unchanged-code composites, q0..20 and q21..42 in separate JVMs. This is protocol calibration, not an engine improvement or task completion.

The retained findings include all three-try observations, per-query and suite statistics, modality
screens, ten chronological A-A pair estimates, and restart-boundary comparisons with the separate
quiet full-suite control.

The raw logs, CSV exports and replay material were archived off-tree on 2026-09-09 in
`build/rig-evidence-archive-20260909.tar.gz`; the findings are retained here and are not replayable
in-tree.

Protocol cohorts are sequential. Differences combine restart, JIT, residency, page cache and thermal effects; they cannot identify a single cause. Lower score variance does not establish sensitivity to real CPU-side changes. The cache-cleared and CPU-perturbation studies remain separate requirements.
