# Why SirixDB

*A document store where history is the data model, not a feature.*

SirixDB is an embeddable, open-source (BSD-3) store for JSON and XML that
**never overwrites data**. Every commit creates a new, immutable revision that
structurally shares everything it didn't change with the revision before it.
That single design decision is where everything below falls out from — the
good parts and the trade-offs. This document is the honest version of the
pitch: what the architecture buys you, what we've measured, and where it
loses.

---

## The one-paragraph mental model

Think of a persistent (copy-on-write) tree, like the data structures inside
Clojure or Git, but as a database engine with fine-grained nodes instead of
whole files: every JSON object, array, field, and value is a node with a
stable identity across revisions. Commit `N+1` copies only the page fragments
it touches; everything else is a reference into the past. A *sliding
snapshot* algorithm bounds how many fragments any read must consult to
reconstruct a page, so a database with 10,000 revisions opens and reads as
fast as one with ten — we benchmarked exactly that claim, found it false in
two places, and fixed both (see "Receipts" below).

## What this buys you

### 1. Time travel is a query, not a restore job

Any revision, any wall-clock instant, first-class in the query language
(XQuery / JSONiq with temporal extensions):

```xquery
jn:open('orders','orders.json', xs:dateTime('2026-05-01T00:00:00Z'))
  .customers[].name
```

Every node also knows its own history — `jn:all-times($node)` walks every
version of one field without touching the rest of the document. There is no
"as-of replica", no WAL archaeology, no application-level `valid_from`
columns. Auditing "what did this record say when we made that decision?" is a
one-liner.

### 2. Diffs are semantic and instant

Because revisions share structure physically, SirixDB computes diffs by tree
comparison with rolling hashes, not by serializing two snapshots and running a
text diff. Diffing two revisions of a document returns exact node-level
operations (insert/update/delete with stable node keys) in **~0.3 ms** in our
benchmark — and it stays flat regardless of document size, because unchanged
subtrees hash-skip. The same machinery powers the web UI's revision scrubber
and structured diff view.

### 3. Storage grows with *change*, not with *data size*

A commit costs O(changed nodes), not O(document). Update one field in a 100 MB
document and you pay for one page-fragment chain, not a 100 MB copy. We
decomposed the actual per-commit byte cost on disk (currently ~1.7 KB fixed
overhead per small commit, fully attributed byte-by-byte in
[`STORAGE_COST.md`](STORAGE_COST.md), with a roadmap to ~700 B). For
small-document workloads PostgreSQL's storage is still tighter (see the
honest comparison below); the crossover argument is about *large documents
with small edits*, and we won't quote a crossover point until we've
benchmarked it.

### 4. Crash safety you can audit, not just trust

The commit protocol is two ordered write barriers: data write-ahead, then a
dual-slot "uber beacon" flipped with data-integrity write-through (O_DSYNC;
FUA on NVMe), with the revisions file opened O_SYNC. One explicit fsync per
commit. We built a **power-loss simulation harness** that records every write
and force at the FileChannel level, then materializes thousands of crash
states — torn writes, dropped unforced writes, metadata/size splits — and
cold-opens each one: acked revisions must survive, unacked ones must be
rejected *cleanly*. 0 failures across the state space, and the harness is in
the tree (`bundles/sirix-core/src/test/java/io/sirix/crash/`), not in a
slide deck.

### 5. Analytics without an ETL hop

Pages carry columnar (PAX) regions — dictionary-encoded strings, bit-packed
numbers with zone maps, bit-packed booleans — and a vectorized executor with
SIMD kernels uses them for group-by, filtered counts, aggregates, and
count-distinct. At 1M records (cold executor, results verified byte-identical
against the interpreted pipeline):

| query | interpreted | vectorized |
|---|---|---|
| group-by (string key) | 18.2 s | 3.2 s |
| group-by (two keys) | 20.4 s | 1.6 s |
| group-by (numeric key) | 18.3 s | 1.4 s |
| sum / avg / min+max | 15–37 s | 1.4–1.8 s |
| count-distinct | 18.4 s | 1.7 s |

With the in-memory columnar projection installed, the suite lands within
1.2-4.2x of DuckDB 1.5.2 on the same machine at 100M records — winning one
shape outright (filtered group-by 41 vs 59 ms) — full methodology and honest
caveats in [\`COMPARISON_DUCKDB.md\`](COMPARISON_DUCKDB.md). The unpredicated group-by drops
from a ~27 s page walk to ~1 ms at 100M records. Every fast path is
**fail-closed**: the optimizer only claims a pipeline when it can prove the
query's shape matches what the kernel emits, and kernels verify their own
coverage per page (a value the column can't represent falls back to the
general path). Wrong-but-fast is treated as a bug class, not a configuration
option — a differential suite runs every shape through both pipelines and
requires byte-identical output.

### 6. It's a library first

The core is an embeddable Java library (also usable from Kotlin); the
Vert.x-based REST server, the CLI, and the SolidJS web UI are layers on top.
Single process, no sidecar, no cluster to operate. `jn:store` a document and
you have a versioned database in a directory.

## Receipts (we benchmark against ourselves and publish the losses)

- **History-independent performance**: we found session opens were O(history)
  (a quadratic `access()` syscall storm — 50M syscalls over a 10k-commit
  build) and per-commit work degraded 296→154 commits/s. Both root-caused and
  fixed: opens now 0.18 ms flat at 10k revisions, commit rate flat ~570/s.
  The full causal chain is in [`BENCHMARKS.md`](BENCHMARKS.md).
- **Honest PostgreSQL comparison** ([`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md)):
  PG 17 with a history table wins raw small-document numbers — ingest 4,015
  vs ~430 commits/s (PG sits at 84% of the device's fsync floor; that's its
  home turf) and total storage 4.7 vs ~12 MiB. SirixDB wins per-statement
  embedded reads, 0.3 ms semantic diffs, and sub-document time travel, which
  PG simply doesn't have. Durability settings were verified equivalent before
  measuring.
- **Correctness sweeps as release gates**: adversarial round-trip fidelity
  (83 JSON shapes × every serializer mode), a JSONiq result-correctness sweep,
  and the vectorized differential suite all run as tests. Several of the bugs
  they caught — invalid JSON from fused-node serialization, an optimizer
  rewrite that returned empty results for valid predicates, group-by over
  numeric keys returning nothing — were found *by these gates*, then fixed at
  the root.

## Where SirixDB is the wrong choice (today)

- **High-rate small-document OLTP.** PostgreSQL ingests ~10× faster in that
  regime and stores it tighter. If you don't need history, you don't need us.
- **Distributed workloads.** Single-node, single-writer-per-resource by
  design. Horizontal scale is not on the short-term roadmap.
- **Relational queries across many entities.** It's a document store; joins
  exist in JSONiq but a relational engine will beat it at relational shapes.
- **Maturity.** This is a beta of a research-grade engine. The disk format
  (V0) is now contract-documented and crash-tested, but you should expect
  rough edges, and the known limitations are listed in
  [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) rather than hidden.

## Use cases where it shines

- **Audit-grade record keeping** — financial/medical/compliance documents
  where "show me this record exactly as it was on date X, and prove what
  changed since" is the product requirement, not an afterthought.
- **Collaborative or ML-pipeline document evolution** — checkpoint every
  transformation of a config/feature/document tree and diff any two states
  semantically in sub-millisecond time.
- **Debugging production state** — keep the full history of a fast-changing
  JSON state tree and bisect *data* the way you bisect code.
- **Versioned content/configuration APIs** — serve any historical version
  with the same latency as the head revision.

## Try it

- Live read-only demo (tree explorer, query editor with optimized plan view,
  revision scrubber, structured diffs): **https://demo.sirix.io**
- Quickstart (one process, no auth, for evaluation):
  [`QUICKSTART.md`](QUICKSTART.md)
- Sources: https://github.com/sirixdb/sirix — the engine,
  https://github.com/sirixdb/brackit — the query compiler,
  https://github.com/sirixdb/sirixdb-web-gui — the UI.
