# Post-cache profile assessment and bounded next experiment

The accepted baseline is commit `d685cae276`, optimized image
`d30dc2cf56dd0caebc7c11a11e8929be2944042f5e9675365d654431b23f9afb`.
It remains 9.29385 / 9.22944 times behind paired ClickHouse at 100M.
The diagnostic image was rebuilt from the same frozen group-repeat-v1 source and fresh 100M
profile with `-g`; its separate source/runtime manifests and exact commands remain in evidence.
Diagnostic binary SHA-256 is `0a0295c798d9df113d785aeb26031618f9a5be0c4065a7ff564c9b2e3fc75b87`.
`group-repeat-profile-class-continuity.json` verifies all class bytes match the ranked build:
the only differing jar member is `META-INF/sirix-hft-build.properties`.
No diagnostic timing is ranked. Q1/Q4 diagnostic and sampled runs each matched exactly with
the expected aggregate routes. Build/freeze/profile guards passed (311 / 2 / 49 samples).

`profile-group-repeat-100m-attempt1/q{1,4}.perf.data` contains self samples at 997 Hz,
`cycles:u`, no call stacks and no lost samples. Hybrid CPU event denominators are separate:

| Query/event | Approximate sampled cycle denominator | Composite scan | Dense acquire | Exact identity compare |
| --- | ---: | ---: | ---: | ---: |
| Q1 cpu_atom/cycles/u | 9,870,355,957 | 32.90% | 18.70% | 4.17% |
| Q1 cpu_core/cycles/u | 20,009,053,821 | 26.08% | 15.12% | 4.76% |

Q4 denominators are 14,840,789,397 atom cycles and 26,268,775,805 core cycles.
Its prominent self costs include method-handle argument conversion (7.85% / 6.78%),
XXH3 hashing (7.14% / 6.31%), and array opaque reads (6.66% / 5.78%). Already negative
atomic-array, directory-batching and manual-decoding experiments remain excluded.

These are whole-process sampled CPU fractions, not wall-time shares or added percentages
across heterogeneous CPU events. Only one capture per query was taken; scheduler placement,
sampling variation, debug-image code layout, and inlining limit attribution. No stack samples
were taken, so method-handle self samples do not establish their caller. Phase diagnostics
independently put Q1/Q4 directory walks at 1939.8 / 1941.3 ms. Q1 grouping took 359 ms
within a 3.140-second diagnostic query, so eliminating its entire fold would still not close
the overall gap. Full paired attempts decide whether a smaller improvement is worth retaining.

Next mechanism: bounded local-dictionary count aggregation for a single untransformed string
group key and count-only query. Scan the already evaluated selected-row mask once, maintaining
counts and first row positions for at most 256 dictionary entries plus missing. Fold used entries
in first-encounter order through the existing exact identity registry and partitioned group table.
Reset at every row group; never treat a local dictionary id as cross-group identity. Rejecting a
fingerprint collision, missing-to-empty substitution, first-seen/aux ordinals, pass ownership,
high-cardinality fallback, and partial final masks receive focused tests. This is aggregation
inside the measured query, with no global state, answer cache, persisted index change or prepass.

The 4 GiB allowance is cumulative. Existing-database artifacts occupy 2,693,046,272 allocated
bytes after diagnostics. Before another candidate build, losslessly compress only the 70 completed
per-attempt 1M profiles, recording both hashes and verifying decompression before unlinking their
expanded copies. Keep training profiles, measured binaries, source archives, raw answers/timings,
all databases and column-major v5. Use a JVM 1M gate to avoid another 70 large instrumentation
profiles; then fresh instrument/train/optimize at 100M and complete before/after and leader pairs.
The original 66 GiB experiment floor and permanent 20 GiB floor remain unchanged. Count both
group-repeat and dictionary-count candidate artifacts against the same original allowance.
The independent column-major space decision remains open; no import, rebuild or migration is authorized.
