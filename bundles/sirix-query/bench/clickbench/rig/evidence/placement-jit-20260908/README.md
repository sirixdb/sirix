# Ordinary GC, JIT and CPU-placement diagnostics

These are diagnostic processes, not a variance calibration or an A/B performance claim. The original failed observer verdict for placement-01-full is retained; it lacks CPU sample ids and is reconciled explicitly. The remaining three placement recordings completed, as did the separate one-process synchronous-compilation test. The original runtime was frozen before later harness compilation.

The raw JFR/perf inputs and replay material were archived off-tree on 2026-09-09 in
`build/rig-evidence-archive-20260909.tar.gz`; the retained findings are not replayable in-tree.

The inferred query windows use output receipt minus rounded wall time, so event overlaps are approximate. Allocation weights are sampling estimates. Raw controller paths preserve provenance and are not portable launch instructions. Read the [report](../../../../../../../docs/CLICKBENCH_RIG_JIT_GC_2026-09-08.md) before interpreting these observations.
