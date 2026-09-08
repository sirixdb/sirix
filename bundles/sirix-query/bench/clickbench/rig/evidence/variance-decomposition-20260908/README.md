# Variance attribution from existing observations

Run `python3 analyze.py` here. It uses the standard library and only reads the committed full/split summaries in the adjacent evidence directories. It launches no JVM and accesses no database. Source SHA256 hashes are recorded in `summary.json`; the variance and range identities are asserted. Both cohorts remain separate. CSV line endings are canonical LF.

The [report](../../../../../../../docs/CLICKBENCH_RIG_VARIANCE_ATTRIBUTION_2026-09-08.md) explains the identities, findings, and limits. Covariance allocation is descriptive rather than causal; post-hoc range endpoints and leave-one-out sensitivity ranges are not confidence intervals.
