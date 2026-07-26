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
| [NATIVE_IMAGE.md](NATIVE_IMAGE.md) | Building and running the GraalVM native binaries (instant startup for the CLI, shell, and REST server). |
| [MCP_SERVER_DESIGN.md](MCP_SERVER_DESIGN.md) | The Model Context Protocol server for AI agents: tools, resources, snapshot/diff workflow, and security model. |

## Correctness

| Doc | What it covers |
|-----|----------------|
| [formal-verification.md](formal-verification.md) | The invariant catalog: load-bearing invariants of the engine stated as pre/post-conditions, each with a proof sketch and a pointer to the CI test that discharges it. |
| [cost-based-optimizer-design.md](cost-based-optimizer-design.md) | Design of the cost-based query optimizer (PathSummary statistics, selectivity/cardinality estimation, predicate pushdown, join ordering). |

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
  [HOT_INCREMENTAL_PORT_PLAN.md](HOT_INCREMENTAL_PORT_PLAN.md)
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
  [NAME_DICTIONARY_RECONSTRUCTION_PLAN.md](NAME_DICTIONARY_RECONSTRUCTION_PLAN.md)
