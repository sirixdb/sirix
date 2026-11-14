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
reconstruct a page. See [`BENCHMARKS.md`](BENCHMARKS.md) for measured
history-depth costs and workload limitations.

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

### 2. Diffs are semantic

Because revisions share structure physically, SirixDB computes diffs by tree
comparison with rolling hashes, not by serializing two snapshots and running a
text diff. Diffing two revisions of a document returns exact node-level
operations (insert/update/delete with stable node keys), skipping unchanged
subtrees when their hashes match. The same machinery powers the web UI's
revision scrubber and structured diff view.

### 3. Storage grows with *change*, not with *data size*

A commit costs O(changed nodes), not O(document). Update one field in a 100 MB
document and you pay for one page-fragment chain, not a 100 MB copy. We
decomposed the actual per-commit byte cost on disk (currently ~1.7 KB fixed
overhead per small commit, fully attributed byte-by-byte in
[`STORAGE_COST.md`](STORAGE_COST.md), with a roadmap to ~700 B). For
the withdrawn PostgreSQL comparison and its methodological limits, see
[`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md). It does not support a
storage crossover claim.

### 4. Crash safety you can audit, not just trust

The [disk-format contract](DISK_FORMAT.md#1-files) owns the commit protocol,
durability profiles and crash-window guarantees. The
[operations guide](operations.md#10-known-limitations-and-operational-caveats)
links the process-crash and power-loss simulation coverage and explains its limits.

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

With the in-memory columnar projection installed, the whole suite lands
within 1.1–4.5× of DuckDB 1.5.2 on the same machine at 100M records — sum
16 ms, two-key group-by 240 ms — and the profile-guided-optimized native
binary comes out ahead of DuckDB on three of nine shapes (filtered count,
filtered group-by, compound-range count). Full methodology and honest
caveats in
[`COMPARISON_DUCKDB.md`](COMPARISON_DUCKDB.md). Every fast path is
**fail-closed**: the optimizer only claims a pipeline when it can prove the
query's shape matches what the kernel emits, and kernels verify their own
coverage per page (a value the column can't represent falls back to the
general path). Wrong-but-fast is treated as a bug class, not a configuration
option — a differential suite runs every shape through both pipelines and
requires byte-identical output.

### 6. It's a library first — and it compiles to a native binary

The core is an embeddable Java library (also usable from Kotlin); the
Vert.x-based REST server, the CLI, and the SolidJS web UI are layers on top.
Single process, no sidecar, no cluster to operate. `jn:store` a document and
you have a versioned database in a directory.

It also builds as a GraalVM native image — *including the write path*, which
took resolving a real toolchain blocker (GraalVM restricts shared `Arena`
*close*, not creation, so the off-heap allocator uses an auto-managed arena in
AOT and lets the GC reclaim mappings; the on-disk file is fsync'd at commit
independently). A native binary creates, shreds, commits, reopens, and
time-travels with no JVM warmup, and on warm analytical queries the
ahead-of-time binary runs **7–17× faster than the JVM** (better instruction
throughput, no JIT ramp). The honest caveat: a cold query whose predicate
needs runtime code generation falls back to the interpreter in AOT (no
class-loading at image runtime), and single-threaded ingest is slower than the
JVM — so the natural split is *ingest on the JVM, embed the native binary for
read/query latency*. Both verdicts and the full perf tables are in
[`NATIVE_IMAGE.md`](NATIVE_IMAGE.md).

## Receipts (we benchmark against ourselves and publish the losses)

- **History-independent performance**: we found session opens were O(history)
  (a quadratic `access()` syscall storm — 50M syscalls over a 10k-commit
  build) and per-commit work degraded 296→154 commits/s. Both root-caused and
  fixed: opens now 0.18 ms flat at 10k revisions, commit rate flat ~570/s.
  The full causal chain is in [`BENCHMARKS.md`](BENCHMARKS.md).
- **Concurrent reads while a writer commits**: a mixed-workload benchmark (16
  reader threads + 1 committing writer over REST) exposed two real defects —
  a reader-side page-lifecycle bug that freed a shared page out from under a
  concurrent reader (sporadic 500s *and* silently-wrong reads), and a second
  O(history) cost where every storage open racing a commit re-read the whole
  revision index one syscall at a time. Both root-caused (via a use-after-free
  stress gate that reproduced the crash in seconds, and a wall-clock profile)
  and fixed at the root. On a 12,800-revision database the same workload went
  from 361 to **11,198 reads/s**, reader p99 from 334 ms to **4.8 ms**, with
  **zero errors** — the aged database now outruns the pre-fix fresh one.
- **PostgreSQL comparison status**: see
  [`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md) for why its cross-system
  ratios are withdrawn.
- **Correctness sweeps as release gates**: adversarial round-trip fidelity
  (83 JSON shapes × every serializer mode), a JSONiq result-correctness sweep,
  and the vectorized differential suite all run as tests. Several of the bugs
  they caught — invalid JSON from fused-node serialization, an optimizer
  rewrite that returned empty results for valid predicates, group-by over
  numeric keys returning nothing — were found *by these gates*, then fixed at
  the root.

## Where SirixDB is the wrong choice (today)

- **Workloads without a history requirement.** Evaluate whether document
  versioning is needed; benchmark candidate engines on your own workload.
- **Distributed workloads.** Single-node, single-writer-per-resource by
  design. Horizontal scale is not on the short-term roadmap.
- **Relational queries across many entities.** It's a document store; joins
  exist in JSONiq but a relational engine will beat it at relational shapes.
- **Maturity.** This is a beta of a research-grade engine. The
  [disk format](DISK_FORMAT.md) is contract-documented and crash-tested, but you should expect
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
  directly; use the [benchmark methodology](BENCHMARKS.md) to assess latency.

## Try it

- Live read-only demo (tree explorer, query editor with optimized plan view,
  revision scrubber, structured diffs): **https://demo.sirix.io**
- Quickstart (one process, no auth, for evaluation):
  [`QUICKSTART.md`](QUICKSTART.md)
- Sources: https://github.com/sirixdb/sirix — the engine,
  https://github.com/sirixdb/brackit — the query compiler. For browser access,
  use the live demo linked above.
