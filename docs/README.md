# SirixDB Documentation

Start here. This index groups the docs by audience so you don't have to read a flat
list of files.

> Looking for the quickstart, install instructions, and query examples? Those live in the
> [project README](../README.md). The hosted docs site is at <https://sirix.io/docs/index.html>.

## For users & operators

| Doc | What it covers |
|-----|----------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | How SirixDB works: the node-tree encoding, copy-on-write page layout, sliding-snapshot versioning, indexes, and the bitemporal model. The best single doc for understanding the system. |
| [operations.md](operations.md) | Running SirixDB in production: configuration, memory/JVM tuning, supported environments, and deployment guidance. |
| [BULK_IMPORT.md](BULK_IMPORT.md) | The sequential and parallel bulk JSON loaders: API, the configurations they refuse, which index families they maintain in the load's single pass, verification guarantees, tuning knobs, and measured numbers. |
| [NATIVE_IMAGE.md](NATIVE_IMAGE.md) | Building and running the GraalVM native binaries (instant startup for the CLI, shell, and REST server). |
| [MCP_SERVER_DESIGN.md](MCP_SERVER_DESIGN.md) | The Model Context Protocol server for AI agents: tools, resources, snapshot/diff workflow, and security model. |

## Correctness

| Doc | What it covers |
|-----|----------------|
| [formal-verification.md](formal-verification.md) | The invariant catalog: load-bearing invariants of the engine stated as pre/post-conditions, each with a proof sketch and a pointer to the CI test that discharges it. |
| [cost-based-optimizer-design.md](cost-based-optimizer-design.md) | Design of the cost-based query optimizer (PathSummary statistics, selectivity/cardinality estimation, predicate pushdown, join ordering). |
| [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md) | Every disabled test in `sirix-core`, why it is off, and the tracking artifact — so a real correctness gap is never mistaken for a benchmark that is simply not run in CI. |

## Projection indexes

| Doc | What it covers |
|-----|----------------|
| [PROJECTION_INDEXES.md](PROJECTION_INDEXES.md) | The user-facing feature: what a columnar projection index is, how to create one in JSONiq, and which analytical queries it accelerates. Start here. |
| [PROJECTION_INDEX_DEEP_DIVE.md](PROJECTION_INDEX_DEEP_DIVE.md) | One dataset walked through every layer — JSON rows to columnar leaves to semantic segments to the bytes on disk to the SIMD kernels — and back up through maintenance and time travel. |
| [PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md](PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md) | Normative contract for the V0 storage format: exact record lookup, document-order routing, and local update/delete/insert/move maintenance. |

The remaining `PROJECTION_INDEX_*.md` files are storage-format design notes; the deep dive cites
them where they matter. Plans for shrinking what these indexes store are working notes of the
ClickBench 100M campaign, indexed under [Design notes & development archive](#design-notes--development-archive).

## Benchmarks

| Doc | What it covers |
|-----|----------------|
| [BENCHMARKS.md](BENCHMARKS.md) | REST-API behavior under concurrency and a 10,000-commit large-history run, with the environment and raw-log provenance for every number. |
| [CLICKBENCH.md](CLICKBENCH.md) | The ClickBench port: the 43 queries translated SQL → JSONiq, the JSON encoding, how to run the load/query/differential gates, and the engine defects the port uncovered. |
| [BENCHMARK_CAMPAIGNS.md](BENCHMARK_CAMPAIGNS.md) | Every change made in the ClickBench and JSONBench performance campaigns, written to be readable without prior knowledge of SirixDB internals. |

## Design notes & development archive

These are engineering working-notes kept for transparency and future reference. They document
*how* some of the harder subsystems were designed, proven, and hardened — especially the **HOT
(Height-Optimized Trie)** index. They are not required reading to use SirixDB, and some describe
superseded iterations; treat them as an archive of the design process rather than current
user-facing documentation.

- **HOT index — foundations & invariants:**
  [HOT_FORMAL_FOUNDATION.md](HOT_FORMAL_FOUNDATION.md),
  [HOT_INVARIANTS_CATALOG.md](HOT_INVARIANTS_CATALOG.md),
  [HOT_OPERATIONS_INVARIANTS_MATRIX.md](HOT_OPERATIONS_INVARIANTS_MATRIX.md),
  [DEWEYID_HOT_INDEX_FORMAL_PROOF.md](DEWEYID_HOT_INDEX_FORMAL_PROOF.md),
  [HOT_PAPER_IMPOSSIBILITY.md](HOT_PAPER_IMPOSSIBILITY.md)
- **HOT index — design & implementation:**
  [HOT_FIX_DESIGN.md](HOT_FIX_DESIGN.md),
  [HOT_FIX_DESIGN_V2.md](HOT_FIX_DESIGN_V2.md),
  [HOT_STRICT_BINNA_DESIGN.md](HOT_STRICT_BINNA_DESIGN.md),
  [HOT_ROUTING_ENCODING_REWRITE.md](HOT_ROUTING_ENCODING_REWRITE.md),
  [HOT_OPTION_B_PHASE_5_DESIGN.md](HOT_OPTION_B_PHASE_5_DESIGN.md),
  [HOT_PHASE_7_DESIGN.md](HOT_PHASE_7_DESIGN.md),
  [HOT_PHASE_7Q_DESIGN.md](HOT_PHASE_7Q_DESIGN.md),
  [HOT_INCREMENTAL_PORT_PLAN.md](HOT_INCREMENTAL_PORT_PLAN.md),
  [HOT_BULK_BUILD.md](HOT_BULK_BUILD.md)
- **HOT index — verification, audits & results:**
  [HOT_INCREMENTAL_SPLIT_VERIFICATION.md](HOT_INCREMENTAL_SPLIT_VERIFICATION.md),
  [HOT_EXISTING_CODE_AUDIT.md](HOT_EXISTING_CODE_AUDIT.md),
  [HOT_WRITER_GETRECORD_AUDIT.md](HOT_WRITER_GETRECORD_AUDIT.md),
  [HOT_EMPIRICAL_FAILURE_TABLE.md](HOT_EMPIRICAL_FAILURE_TABLE.md),
  [HOT_PHASE_4B_DIAGNOSIS.md](HOT_PHASE_4B_DIAGNOSIS.md),
  [HOT_CAMPAIGN_RESULTS.md](HOT_CAMPAIGN_RESULTS.md),
  [HOT_ADDENTRY_STRADDLE_FIX.md](HOT_ADDENTRY_STRADDLE_FIX.md),
  [HOT_STRADDLE_GUARD_REMOVAL_PLAN.md](HOT_STRADDLE_GUARD_REMOVAL_PLAN.md),
  [HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md](HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md),
  [HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md](HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md)
- **Other subsystem plans:**
  [COST_BASED_QUERY_OPTIMIZER_PLAN.md](COST_BASED_QUERY_OPTIMIZER_PLAN.md),
  [ZERO_COPY_PLAN.md](ZERO_COPY_PLAN.md),
  [NAME_DICTIONARY_RECONSTRUCTION_PLAN.md](NAME_DICTIONARY_RECONSTRUCTION_PLAN.md),
  [ROWSTORE_RANGED_DECODE_PLAN.md](ROWSTORE_RANGED_DECODE_PLAN.md)

### ClickBench 100M storage & speed campaign

Working notes from the campaign to shrink the 100M-row ClickBench database without giving up query
speed. [CLICKBENCH.md](CLICKBENCH.md) above owns the benchmark itself and its published numbers;
these documents record the plans, the arithmetic, and the verdicts — including the levers that were
measured and rejected.

| Doc | What it covers |
|-----|----------------|
| [CAMPAIGN_PROGRESS.md](CAMPAIGN_PROGRESS.md) | The campaign's running ledger: every session's measurements, gates, dead ends and verdicts in chronological order. The briefs below cite it for raw numbers. |
| [CLICKBENCH_100M_RESUMPTION_PLAN.md](CLICKBENCH_100M_RESUMPTION_PLAN.md) | The correctness + HFT plan the campaign resumed from after the 2026-08-29 crash: root cause, the failures it had to clear, and the measurement protocol. |
| [STORAGE_AND_SPEED_PLAN.md](STORAGE_AND_SPEED_PLAN.md) | Plan of record for cutting storage and latency together, including the generality contract every lever must satisfy — triggered by data, statistics or configuration, never by a column name or query id. |
| [STORAGE_FOOTPRINT_REDUCTION_PLAN.md](STORAGE_FOOTPRINT_REDUCTION_PLAN.md) | Measurement-gated plan for shrinking the primary tree, projection segments and global dictionaries without giving up direct SIMD execution or incremental copy-on-write updates. |
| [STORAGE_TO_MID_TABLE.md](STORAGE_TO_MID_TABLE.md) | Where the bytes of the 69.6 GB database actually sit, and what would have to change to reach the leaderboard's ~15 GB median. |
| [ROADMAP_TO_30GB.md](ROADMAP_TO_30GB.md) | The stack of levers between 69.5 GB and the ~30 GB target, each with its delta, its status, and the gate that would accept it. |
| [SEGMENT_SCOPED_DICTIONARIES.md](SEGMENT_SCOPED_DICTIONARIES.md) | Design for retiring the load-time dictionary pre-pass by scoping record-page dictionaries to a segment. |
| [P2_GLOBAL_DICTIONARY_DESIGN.md](P2_GLOBAL_DICTIONARY_DESIGN.md) | Resource-wide dictionaries for the fat string columns: the design, and the post-mortem of why its first gate failed and the track was stopped. |
| [P2_SEGMENT1_BRIEF.md](P2_SEGMENT1_BRIEF.md) | Segment 1 of that design with the acceptance corrected to distinct-weighted value lengths; read it together with the document above. |
| [INGEST_RADIX_ALLOCATION_BRIEF.md](INGEST_RADIX_ALLOCATION_BRIEF.md) | Allocation profile of the global value-dictionary radix during ingestion, recorded so the measurement survives. Deliberately not implemented. |
