# SirixDB specification

Specification target: **SirixDB 1.0.0**. The P0 chapters below are drafts for adoption
before the approximately **9 December 2026** format/API freeze. They define intended
behavior, with implementation/test gaps and open decisions recorded explicitly.
The NATIVE design retains its existing adoption status. No new runtime conformance
or frozen release artifact is claimed by this documentation change.

| Chapter | Contract | Status |
|---|---|---|
| [CONV](CONV-conventions.md) | normative language, IDs, evidence, traceability rollout, official work budgets, timed acceptance verdict, results ledger and nightly trend tier | P0 draft |
| [FMT](FMT-disk-format.md) | persisted bytes, feature negotiation, strict readers, compatibility | P0 draft |
| [TXN](TXN-transactions-durability.md) | isolation, durability, recovery, async commits, hooks and backup consistency | P0 draft |
| [VER](VER-versioning-bitemporal.md) | revisions, timestamps, retention representation, versioning equivalence | P0 core draft |
| [CONF](CONF-configuration.md) | persisted database/resource settings and format-affecting defaults | P0 persisted-settings draft |
| [JAPI](JAPI-java-api.md) / [manifest](JAPI-stable-api-manifest.md) | Java ownership/errors/hooks/identity and explicit compatibility inventory | P0 draft; three-tier boundary decided |
| [REST](REST-api.md) | OpenAPI baseline, revisions, preconditions, authorization, errors | P0 draft |
| [MEM](MEM-memory-pool.md) | one evictable memory pool: frames return budget across size classes, one cache frame budget with priority eviction, compact-image pool reservation | P0 draft (decisions of 2026-11-26 folded in) |
| [QRY](QRY-query-execution.md) | column-served predicated count/sum with nested array membership: claimed shapes, interpreter equality, scope and seam proofs, threads, revisions, work/allocation budgets | P0 draft (captain decisions of 2026-11-28 folded in) |
| [NATIVE](NATIVE-embedded-c-api.md) | JVM-free C/C++/Rust/Python embedding, stable ABI 1, JSON/XML parity | Design adopted for 1.0; runtime implementation pending |

Requirement IDs are stable and never reused. Every numbered requirement has a test
mapping or an explicitly planned conformance case. Source-backed mappings are not test
pass claims; the trace tables are maintained manually until tooling lands. Follow
[CONTRIBUTING](../CONTRIBUTING.md) and [VERIFICATION](../docs/VERIFICATION.md) for validation.
See [changes](CHANGELOG.md) and [retired IDs](RETIRED.md).

The P0 documentation slice intentionally leaves spec_trace.py, @Verifies, annotation
migration, response validation, compiled API checking and the crash CI lane as planned
work. This slice implements no product or CI behavior.
Full MODEL/IDX/QRY/DIFF/LIM/SEC chapters and broader valid-time semantics remain P1;
MCP/OPS and formal models remain later work. Required cross-cutting decisions already
have stable owners here: HOT rules FMT-301–304, defaults CONF-106, budgets CONV-301,
JSON replacement JAPI-301–302, derived backup TXN-613, conditional writes TXN-311 with
REST-204/205 and JAPI-111.
