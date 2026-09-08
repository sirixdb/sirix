# Launcher validation

The Java build and focused serving/result-encoding tests passed. All 25 Python
harness tests passed; malformed analysis input returns exit 2 without a score.
Shell syntax and Git whitespace checks passed. Raw generated logs retain their
original bytes, including logger-emitted whitespace.

The native Java/Python probe checked exclusive/shared locks, rejection,
inheritance after parent close, close-only semantics and process-exit release.
A raw full-envelope benchmark JVM rejected an occupied host lock before opening
the database.

The 1M pair report is wiring validation only: two pairs deliberately produce
UNRESOLVED and exit 3. It is not a 100M noise estimate or a candidate claim. Both
historical/current engine builds used the same benchmark-main/process-guard
source overlay and identical query catalog. The complete 100M leg then exited 0
with no observer issue; its single observation is not an effect or variance
estimate. Plan/runtime hashes, raw timing log and verdict are retained.
Database/corpus/classpath copies are excluded.

`100m-suite.log` was captured before the runner began opening every `suite.log` with a
`# steering-only` provenance line, so it carries no such line. It is nevertheless a steering leg,
as its `100m-leg.json` records. Converting it with `mkleg.py` yields an `unknown` leg, which
`rank.py` refuses; the log's own silence about its regime is what stops it, not a per-file warning.

This validation covered the opt-in wider steering mode, which has since been removed from the
harness pending a full-context calibration; the record below is historical. It passed 30 Python tests and 16 focused
Java tests (including three schedule/output tests). This validation executes
argument scheduling, raw-log completeness, paired analysis/replay and publication
rejection without a database. It does not recalibrate full-suite variance.
