# Fixed paired comparison: bounded aggregation memory allowance

Baseline: b815d459d1218ab9081f360257ae6f16eff9f476. Candidate: this branch's committed pass-count change, with no partial-probing implementation. The rig freezes both runtimes before any timed launch.

Use `compare.py compare --baseline <baseline> --candidate <candidate> --out <fresh output> --pairs 12 --effect-ln 1.5 --seed 0 --baseline-jvm-arg=-Dsirix.projection.groupPasses.planDiag=true --candidate-jvm-arg=-Dsirix.projection.groupPasses.planDiag=true`, with CB100M_DIR naming the existing campaign database parent. The new diagnostic prints only one compact integer summary per completed group plan; the identical flag is supplied to both arms. The baseline predates this optional summary; its pass counts come from the archived baseline profile. No broad projection diagnostics or profiler are enabled in the paired study.

The rig's prior-study noise model requires 10 pairs, conservatively 12, to resolve a 1.5-ln target (`planning.json`). This is a prespecified resolution target, not a claim about the change. Actual improvement remains subject to the paired confidence interval. Complete all 12 balanced AB/BA pairs; do not extend or stop for significance. If the rig reports UNRESOLVED, report it.

The rig holds its exclusive lease throughout preparation and collection, uses one query JVM, verifies the unchanged 6g/14g heap, 10 GiB arena and canonical eager budget, cools before each leg and observes power limits. The wrapper checks MemAvailable >= 26 GiB immediately before every query launch, adds the same untimed lossless answer dumps to both arms, and stops at the first difference among all 43 answers. The first baseline is also checked against the pre-change full-suite profile's committed answer files. No performance claim precedes that equality gate.

Allowance: two profiling JVMs already used. At most 24 further query JVMs (12 pairs), including every verifier. No DuckDB run, database load, extra profile, ablation, replacement failed leg or unbudgeted verifier is authorized. Raw evidence is retained.

Rig signs: benefit = baseline minus candidate, positive means improvement. Per-query delta = candidate minus baseline, positive means regression. Report every query sorted by delta, with hot seconds and score contributions; also report suite seconds, sum-ln, uncertainty and q16/q18/q32 achieved passes beside predictions 1/1/2. Fewer passes remove rescans but do not divide lookup work by the old count, and larger tables can increase cache misses. Only the paired comparison establishes payoff.
