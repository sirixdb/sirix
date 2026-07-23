# Storage & Execution Lessons from DuckDB, ClickHouse, and Umbra

*Last updated: 2026-07-23*

A researched comparison of what SirixDB can borrow from three analytical/storage
engines — DuckDB, ClickHouse, and Umbra (the TUM/Neumann system, with its
LeanStore/vmcache buffer-manager lineage) — framed against Sirix's actual
architecture.

Every finding below was cross-checked against primary sources (peer-reviewed
papers where possible, official engineering blogs and source otherwise) and
adversarially verified. Each carries a **confidence** tag and its sources.
Where a lesson conflicts with Sirix's append-only / bitemporal invariants, that
is called out explicitly.

## Sirix baseline (what we are comparing against)

A temporal, bitemporal, append-only, copy-on-write versioned document store
(JSON/XML). Record/node-oriented: documents are shredded node-by-node into
slotted `KeyValueLeafPage`s (≤1024 records/page) addressed through a persistent
fixed-fanout (1024) indirect-page radix trie; each revision has its own
`RevisionRootPage` over an `UberPage`. Versioning is per-page fragment-based
(full / differential / incremental / sliding-snapshot) reconstructed at read
time. Analytical techniques already retrofitted onto this layout: a PAX
columnar overlay per leaf page (`NumberRegion`/`StringRegion`) with per-tag
min/max zone maps, FSST string compression, LZ4/deflate page compression, a
smallest-of-codecs bake-off, offset-table template dedup, a frame-of-reference +
bit-packing numeric codec, and a delta-of-delta/DoubleDelta codec for monotonic
temporal columns. Execution is mostly tuple-at-a-time (Brackit) with a partial
vectorized/SIMD columnar path; the buffer pool uses size-class frame allocation
with clock-sweep eviction (LeanStore/Umbra-style).

## Summary

The strongest, most Sirix-specific lessons cluster in **string/encoding
layout**, **buffer-manager architecture**, and **adaptive execution**. The
single sharpest *tension* is buffer-manager design: Sirix already swizzles, but
in a coarse **object-reference** form (`PageReference.setPage()` installs a live
`Page`; a cold reference resolves by `key` through the buffer cache) rather than
LeanStore/Umbra's packed tagged word. That form already tolerates the multiple
incoming references created by copy-on-write cross-revision sharing; what vmcache
would buy is O(1) resolution of *non-owning* references without the keyed cache
probe, plus fragmentation-free variable-size pages — a narrower, but real, edge.
One honest gap: this pass surfaced **no
verifiable ClickHouse findings**; treat ClickHouse as unaddressed here, not as a
negative result.

Recommended order of adoption:

1. **German/Umbra-string comparison prefixes in `StringRegion`** — cleanest
   high-confidence win, pure read-side, no invariant risk.
2. **"Flying Start" two-tier execution principle** for the Brackit engine.
3. **vmcache-vs-swizzling** buffer-manager question — most architecturally
   interesting, highest effort, needs a TLB-cost analysis first.

---

## Storage & encoding

### 1. German/Umbra strings in `StringRegion` — the cleanest new win
**Confidence: high (unanimous 3-0).**

A fixed-width 16-byte (128-bit) value: 32-bit length field, with strings of
≤12 chars stored **fully inline** (no pointer, no heap) and, for longer strings,
a **4-char inline prefix** plus an out-of-line body reference carrying a 2-bit
storage class. The inline prefix short-circuits equality, lexicographic sort,
prefix, and range/zone-map skip checks *without dereferencing the body*.

Sirix's `StringRegion` is already documented "BtrBlocks/Umbra-style" with a
dictionary zone-map, but does **not** yet use inline comparison-prefixes to
short-circuit checks. The concrete win is on the read/execution path and in zone
maps.

**Invariant / transfer flag:** the raw-pointer long-string variant is an
in-memory execution representation; on disk it must use a buffer-index+offset
form (as Arrow `Utf8View` does). Since `StringRegion` is already FSST-compressed
on disk, "directly transferable" is optimistic for persistence — this applies
most cleanly to the read/execution path. Fully compatible with append-only/COW.

Sources: [Umbra CIDR 2020](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf),
[CedarDB — German strings](https://cedardb.com/blog/german_strings/).

### 2. Retire the outer whole-page LZ4/deflate wrap; keep compression per-segment
**Confidence: high (unanimous 3-0).**

DuckDB scans each column segment to pick the best-fit lightweight codec
(constant / RLE / bitpacking / FOR / dictionary / FSST / float) then compresses,
so it can decompress a few rows at a time and support efficient random seeks —
whereas a general-purpose 256 kB block may require decompressing multiple MB to
fetch a single row.

This validates Sirix's smallest-of-codecs bake-off but argues for (a) finer
granularity (per column region/segment, not per leaf page) and (b) retiring the
outer LZ4/deflate whole-page wrap, which penalizes trie-indexed point lookups.
Note: DuckDB selects by *estimated* compressed size then compresses once — a
cheaper variant of Sirix's brute-force compress-with-every-codec-and-pick-
smallest approach. Fully compatible with append-only/COW.

Source: [DuckDB — Lightweight Compression](https://duckdb.org/2022/10/28/lightweight-compression).

### 3. Late-materialize compressed strings — decompress after filtering
**Confidence: high (unanimous 3-0).**

Keep FSST-compressed strings compressed as they flow through the pipeline and
decompress only *after* predicate filtering, so filtered-out rows are never
decompressed. DuckDB implements this with a dedicated `FSST_VECTOR` type. Sirix's
`StringRegion` already uses FSST, so this is a direct read-path lesson.

**Caveat:** the feature is gated behind `enable_fsst_vectors` and off by default
in DuckDB (it notes higher overhead / under-utilization), so treat as a design
direction rather than a proven default. Pure read-side; compatible with Sirix's
invariants.

Source: [DuckDB PR #4366](https://github.com/duckdb/duckdb/pull/4366).

### 4. Add a float codec — target ALP/ALPRD, not Chimp/Patas
**Confidence: medium (2-1; three neighboring Chimp/Patas claims refuted 0-3).**

Add a specialized lossless float codec to the `NumberRegion` bake-off for general
doubles/decimals (complementary to the delta-of-delta already implemented for
monotonic temporal columns). Target **ALP/ALPRD**, which as of DuckDB 0.10 (2024)
*replaced* Chimp/Patas as the default (ALP ~4.3× ratio and 2-4× faster
decompression vs Chimp ~2.4× / Patas ~2.1×).

**Note:** the claim that Chimp/Patas are the SOTA "next float codec" was refuted
(0-3). The specific codec choice warrants direct benchmarking against Sirix's
existing FOR+bitpacking+delta-of-delta on real numeric/temporal data before
adoption.

Source: [DuckDB — Lossless Floating-Point Compression](https://duckdb.org/library/fp-compression/).

### 5. Store dictionaries / large objects natively and consecutively
**Confidence: high (unanimous 3-0).**

Umbra: "if a dictionary is stored consecutively in memory, decompression is just
as simple and fast as in an in-memory system." Store dictionary tables, long
strings, and columnar chunks in variable-size pages so decompression runs at
in-memory speed. This *validates* Sirix's existing size-class `FrameSlotAllocator`
and offers a choice between:

- **Umbra** power-of-two size classes (≥64 KiB) backed by per-class mmap'd
  anonymous regions (physical memory mapped lazily on `pread`, released via
  `MADV_DONTNEED` on eviction); vs
- **vmcache** any-multiple-of-4KB mapping of non-contiguous physical frames into
  one contiguous virtual range (zero main-memory fragmentation).

Fully compatible with append-only/COW.

Sources: [Umbra CIDR 2020](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf),
[vmcache SIGMOD 2023](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf).

---

## Buffer management

### 6. vmcache is a better architectural fit than swizzling for a versioned store
**Confidence: high (3-0 on the core; 2-1 on the forest sub-claim).**

Sirix already *swizzles*, but in a coarser **object-reference** form rather than
LeanStore/Umbra's packed tagged word. A `PageReference` holds both a
`volatile Page page` — the resident in-memory object, installed via `setPage()`
and nulled on eviction — and a `long key`, the on-disk page id. The hot path is a
single field read (`getPage()`); a cold reference (`page == null`) resolves
through the buffer cache by `key`. Functionally this is a swizzle (hot = follow
the object reference, cold = load by id), but it is a per-reference heap field,
**not** a single machine word packed inline in the parent page as in
LeanStore/Umbra.

That representation matters for the invariant analysis. Swizzling's textbook
**single-owner-tree** constraint (every structure a tree, the pool a forest)
comes from the *packed* form, where the swip lives inline in the parent and is
the sole handle. Sirix's object-reference form sidesteps that strict packing
constraint, so it is in fact *more* tolerant of multiple incoming references than
tagged-pointer swizzling. But the underlying cost does not vanish: for a
**copy-on-write cross-revision shared page** reachable from several
`PageReference`s (different revisions' indirect pages, or a secondary index), at
most one reference can hold the live `page` object; the others fall back to a
keyed buffer-cache lookup.

vmcache — by Leis, LeanStore's own creator — removes both the translation hash
table and swizzling via virtual-memory page-table translation, and natively
supports arbitrary multiple-incoming-reference graphs (it explicitly names
secondary indexes and graph next-pointers as the swizzling pain points). It
retains DBMS control over faulting/eviction via anonymous memory +
`MADV_DONTNEED`, unlike file-backed mmap.

**Refined conclusion (narrower than "swizzling can't do multiple references"):**
Sirix's object-reference swizzle already represents multiple incoming references
fine — the keyed buffer-cache lookup for non-owning references *is* its handling
of them. That keyed lookup is exactly the translation-table cost vmcache removes:
under vmcache *every* reference resolves in O(1) via the page table, with no
per-reference resident field and no hash probe. So vmcache's edge over Sirix's
swizzle is (a) uniform O(1) resolution for non-owning references (no cache probe
for COW-shared pages), and (b) fragmentation-free variable-size pages — not that
swizzling is fundamentally incompatible with structural sharing. The immutability
of Sirix pages further bounds the tension (a shared page is never mutated in
place, so a stale resident copy is never wrong), which is why emulated object-ref
swizzling is likely *sufficient* today; vmcache becomes attractive mainly as
cross-reference-heavy features (richer secondary/graph indexes) grow.

**Cost not yet weighed:** vmcache's page-table manipulation incurs TLB-shootdown
costs (which motivated the follow-up exmap work); this has not been weighed
against Sirix's single-writer/MVCC-snapshot workload.

Sources: [LeanStore ICDE 2018](https://db.in.tum.de/~leis/papers/leanstore.pdf),
[Umbra CIDR 2020](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf),
[vmcache SIGMOD 2023](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf).

### 7. Append-only makes clean-page eviction free — already exploited
**Confidence: high (unanimous 3-0).**

Because every persisted page is immutable, the pool can drop any cached read page
with **zero writeback and no dirty-page tracking**. Confirmed directly in Sirix
code: `ClockSweeper` evicts via `page.close()` + `setPage(null)` + `onEvicted`
with no serialization, and `CacheablePage` has no dirty flag. This is a genuine
structural advantage over mutable-page engines — keep leaning on it. (Reinforced
by DuckDB's own clean-page-drop-on-eviction behaviour.)

### 8. LeanEvict cooling-FIFO as a lower-overhead eviction refinement
**Confidence: medium (2-1, single source).**

Replace per-access eviction bookkeeping with LeanStore's identify-the-cold
strategy: speculatively unswizzle random pages into a ~10%-of-pool "cooling"
FIFO that grants a grace period before eviction, doing zero work on hot-page
access. Simulation shows near-parity hit rate (92.8% at 10% cooling vs 93.1% LRU
/ 93.8% 2Q at 5 GB data / 1 GB pool / Zipf 1.0) at lower runtime overhead — a
concrete lower-overhead refinement of Sirix's clock-sweep eviction. Compatible
with append-only (immutable pages drop with no writeback).

**Caveat:** single claim, split vote; hit-rate parity in a trace-driven
simulation does not guarantee an end-to-end win on Sirix's specific workload, and
the LeanStore authors themselves note hit rates do not translate directly into
performance.

Source: [LeanStore ICDE 2018](https://db.in.tum.de/~leis/papers/leanstore.pdf).

---

## Query execution

### 9. "Flying Start" — begin cheap, escalate if hot
**Confidence: high (unanimous 3-0).**

Move from a hard vectorized-vs-interpreted choice to a two-tier "begin-cheap /
escalate-if-hot" strategy. Umbra lowers plans to a lightweight IR, starts every
query on a fast single-pass backend (**geomean 108× faster compilation than
LLVM -O3 at only 1.2× slower execution**), and dynamically switches long-running
pipelines to LLVM-optimized machine code at runtime. This avoids compiling cheap
queries (always-compiling HyPer spent up to 29× more time compiling than
executing) while long queries still reach peak throughput (geomean 3.0× on JOB,
1.8× on TPC-H over HyPer).

Directly targets Sirix's mostly tuple-at-a-time Brackit engine with only a
partial vectorized/SIMD path.

**Transfer caveat:** Umbra's mid-query switch relies on morsel-driven
parallelism, a substrate Sirix lacks, so the transferable form is the
query-level *principle* (cheap default tier, escalate hot fragments), not the
literal dual-compiler + morsel-switch machinery.

Sources: [Umbra CIDR 2020](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf),
[Tidy Tuples & Flying Start (VLDBJ 2021)](https://db.in.tum.de/~kersten/Tidy%20Tuples%20and%20Flying%20Start%20Fast%20Compilation%20and%20Fast%20Execution%20of%20Relational%20Queries%20in%20Umbra.pdf).

### 10. Out-of-core discipline via a unified buffer manager
**Confidence: high (unanimous 3-0).**

Adopt DuckDB's out-of-core execution discipline:

- Route persistent **and** temporary/spill data through **one** unified buffer
  manager on fixed-size pages, recomputing pointers after reading spilled
  variable-length pages back in rather than serialize/deserialize.
- Spill blocking operators (aggregations, distinct counts, joins, sort, window)
  to disk **adaptively** only once intermediates exceed the memory limit.
- Over-partition aggregation hash tables (e.g. 32 partitions > threads) to bound
  parallel-combine memory.
- Drop clean cached pages on eviction with no writeback (see §7 — reinforcing for
  Sirix).

The under-emphasized, genuinely-new lesson is the **unified memory budget shared
between persistent pages and spillable query intermediates**, which Sirix's
tuple-at-a-time engine does not yet do — the main gap to close as Sirix adds
blocking columnar/vectorized operators. Execution-side; orthogonal to the
temporal invariants.

Sources: [DuckDB — External Aggregation](https://duckdb.org/2024/03/29/external-aggregation),
[DuckDB — Memory Management](https://duckdb.org/2024/07/09/memory-management).

---

## Not yet addressed / open questions

- **ClickHouse (largest gap):** an explicit research target (MergeTree
  parts/marks/granules, sparse primary index, data-skipping indexes,
  projections, async inserts, wide-vs-compact parts, GIN/text indexes) that
  produced **zero surviving claims** in this pass. This reflects the research not
  surfacing verifiable findings, not that ClickHouse has nothing transferable.
  Warrants a dedicated follow-up.
- **JSON columnar shredding:** how to shred semi-structured data columnarly
  (DuckDB `SHREDDED_VECTOR`, [JSON tiles](https://db.in.tum.de/~durner/papers/json-tiles-sigmod21.pdf),
  ClickHouse typed JSON columns) within Sirix's node-by-node shredding — and
  whether per-column JSON shredding conflicts with per-node bitemporal versioning
  and the PAX-per-leaf overlay.
- **vmcache vs emulated swizzling:** does vmcache's page-table buffer manager
  actually outperform Sirix's swizzling emulation on a single-writer,
  MVCC-snapshot, COW workload once TLB-shootdown costs are accounted for?
- **On-disk (not just in-memory) codec wins:** does ALP/ALPRD beat Sirix's
  existing FOR + bit-packing + delta-of-delta on real temporal numeric columns,
  and does German-string short-string inlining pay off once `StringRegion` is
  already FSST-compressed and dictionary-encoded?

## Refuted claims (recorded for the record)

- "Pointer swizzling is a drop-in replacement for the page-translation layer,
  directly transferable to Sirix." Refuted — the single-owner-tree requirement
  conflicts with COW structural sharing (see §6).
- "Chimp/Patas (Gorilla-lineage XOR codecs) are the concrete next float codec /
  achieve substantially better ratios than prior SOTA." Refuted (0-3) —
  superseded as DuckDB's default by ALP/ALPRD (see §4).

## Provenance & confidence notes

- Umbra / LeanStore / vmcache findings rest on **peer-reviewed primary papers by
  the systems' own authors** (strongest).
- DuckDB findings rest on **official engineering blogs** (primary but not
  peer-reviewed), several corroborated by PRs, current docs, source, or an
  ICDE 2024 paper — weighted below peer-reviewed.
- Specific defaults are time-sensitive: Umbra's 2020 bytecode-VM tier was
  superseded by the 2021 direct-machine-code Flying Start; DuckDB's Chimp/Patas
  were superseded as default by ALP/ALPRD (0.10, 2024); `FSST_VECTOR` remains
  experimental / off by default. The underlying mechanisms and design principles
  are current; pin any figure to its stated config.

## Sources

Primary (peer-reviewed):

- Umbra: A Disk-Based System with In-Memory Performance — CIDR 2020: <https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf>
- LeanStore: In-Memory Data Management Beyond Main Memory — ICDE 2018: <https://db.in.tum.de/~leis/papers/leanstore.pdf>
- Virtual-Memory Assisted Buffer Management (vmcache) — SIGMOD 2023: <https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf>
- Tidy Tuples and Flying Start — VLDB Journal 2021: <https://db.in.tum.de/~kersten/Tidy%20Tuples%20and%20Flying%20Start%20Fast%20Compilation%20and%20Fast%20Execution%20of%20Relational%20Queries%20in%20Umbra.pdf>
- JSON Tiles — SIGMOD 2021: <https://db.in.tum.de/~durner/papers/json-tiles-sigmod21.pdf>

Primary (official blogs / PRs / docs):

- DuckDB — Lightweight Compression: <https://duckdb.org/2022/10/28/lightweight-compression>
- DuckDB — Lossless Floating-Point Compression: <https://duckdb.org/library/fp-compression/>
- DuckDB — External Aggregation: <https://duckdb.org/2024/03/29/external-aggregation>
- DuckDB — Memory Management: <https://duckdb.org/2024/07/09/memory-management>
- DuckDB PR #4366 (FSST vectors): <https://github.com/duckdb/duckdb/pull/4366>
- CedarDB — German Strings: <https://cedardb.com/blog/german_strings/>

_Research method: five decomposed search angles, ~26 sources fetched and
deduplicated, 123 candidate claims extracted, then per-claim 3-vote adversarial
verification (a claim needed a majority of independent verifiers to survive).
22 claims confirmed, 3 killed, merged into the 9 findings above._
