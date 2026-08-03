# SirixDB: A Bitemporal, Versioned Document Store
## Architecture, Algorithms, and an Honest Evaluation

*An in-depth treatise, synthesised from the source tree and design documents of `sirix-core`, `sirix-enterprise`, `brackit`, and the SirixDB Web GUI. This is an uncommitted working document. It privileges honesty over polish: where a subsystem is partially implemented, aspirational, or carries known technical debt, this is stated explicitly. Benchmark numbers are reproduced from the project's own `docs/COMPARISON_POSTGRES.md` and `docs/BENCHMARKS.md` with their caveats intact.*

---

## Abstract

SirixDB is a temporal, versioned document store for JSON and XML built on a single, uncompromising idea: **immutability with structural sharing**. Every write transaction publishes a new, immutable *revision* of a resource by copying only the pages on the path from a change to the tree's root (copy-on-write), while every unchanged page is shared, by reference, with all prior revisions. From this one decision flow the system's distinctive properties — time-travel reads that are direct traversals rather than reconstructions, append-only durability with no separate write-ahead log, lock-free snapshot isolation on the read path, sub-document field history, and hash-guided semantic diffs.

This document develops the architecture bottom-up across thirteen chapters: the copy-on-write page trie and node model (Ch. 2); the sliding-snapshot page-versioning algorithm and its reconstruction correctness (Ch. 3); the self-describing, checksum-protected on-disk format, its dual-beacon durable-commit protocol, the evolution of that protocol from seven sync calls per commit to one explicit fsync, and the crash-recovery contract validated under SIGKILL fault injection (Ch. 4); the Height-Optimised Trie (HOT) used for index and intra-page navigation, with its invariant catalogue and the engineering campaign that reduced a reproducer's violations from 127 to 1 (Ch. 5); the path summary with incremental statistics and the secondary index family (Ch. 6); the brackit JSONiq/XQuery engine and its cost-based, index-aware optimiser (Ch. 7); the columnar/SIMD vectorised execution path, with its differential-testing discipline and honest in-progress status (Ch. 8); the enterprise io_uring, S3, and Kafka back-ends, including the falsification of the io_uring linked-commit chain by the W1 benchmark (Ch. 9); and the SolidJS web front-end (Ch. 10). Chapter 11 presents a rigorous, same-machine evaluation against PostgreSQL that PostgreSQL *wins* in its home regime, and explains precisely why and where each system's advantages lie. Chapter 12 situates the work; Chapter 13 concludes and lists the open frontier.

---

## Table of Contents

1. Introduction: Thesis, Contributions, and Honest Scope
2. The Persistent Data Structure
3. Versioning: The Sliding Snapshot
4. The On-Disk Format and the Durable-Commit Protocol
5. HOT: The Height-Optimised Trie
6. The Path Summary and Secondary Indexes
7. Query Processing and Optimisation
8. Vectorised and Columnar Execution
9. The Enterprise I/O Stack: io_uring, S3, Kafka
10. The Web GUI
11. Evaluation
12. Related Work
13. Conclusion and Future Work
— References
— Appendix A: Map of Key Classes
— Appendix B: Glossary

---

## 1. Introduction: Thesis, Contributions, and Honest Scope

### 1.1 The problem

Most databases overwrite data in place. History, where offered at all, is a bolt-on: a write-ahead log consumed by checkpointing and then recycled, audit tables maintained by triggers, or temporal-table machinery layered over a mutable heap. Reading the past is a *reconstruction* — replay a log, filter a validity interval, join against a history table.

SirixDB takes the opposite stance: **history is the database.** A resource — a JSON or XML document of arbitrary size — is an append-only sequence of immutable page fragments. Each commit publishes a new revision that shares all unchanged structure with its predecessors. "The document as of revision 17", or "as of last Tuesday at 14:00", is therefore not reconstructed; it is a snapshot already present on disk, reached by a direct traversal with no temporal predicate and no join.

### 1.2 Thesis statement

> *A disciplined commitment to immutability and copy-on-write, executed through a high-fan-out page trie, a sliding-snapshot page-versioning algorithm, and a self-describing dual-beacon on-disk format, yields a document store whose temporal capabilities — time travel, sub-document history, lock-free snapshot reads, and hash-guided semantic diffs — are intrinsic rather than emulated, at a per-version storage and write cost of O(change) plus fixed metadata rather than O(document).*

The thesis is not that this is universally faster. Chapter 11 shows it is *not*, in the small-document regime that is a relational engine's home turf. The thesis is about *cost shape* and *capability*: SirixDB pays a fixed per-revision metadata floor and a deliberately strong durability protocol, and in return gets a class of temporal operations that a `jsonb` column with a trigger cannot express at any document size.

### 1.3 Lineage

The model descends from the doctoral work of Sebastian Graf on versioned tree storage, and specifically the *sliding snapshot* page-versioning strategy (cited throughout `docs/ARCHITECTURE.md`), which this implementation refines into a production system. It draws conceptually on the persistent-data-structure tradition (immutability with sharing; cf. Okasaki), on copy-on-write filesystems (WAFL, ZFS, Btrfs), and on temporal database theory (transaction-time/valid-time bitemporality). Its trie and index machinery draw on modern in-memory index research (ART, and the HOT — Height-Optimised Trie — of Binna et al.). Chapter 12 develops these connections.

### 1.4 Contributions documented here

1. A complete account of the **copy-on-write page trie** and the four **page-versioning strategies**, with the sliding-snapshot reconstruction algorithm and its correctness argument (Ch. 2–3).
2. A self-describing, checksum-protected **on-disk format** with a **dual-beacon commit protocol**, and a documented evolution of that protocol — from 7 sync calls per commit to 1 explicit fsync — including the discovery and elimination of a quadratic `access(2)` syscall pathology, all re-validated under power-loss and SIGKILL fault injection (Ch. 4).
3. A production **Height-Optimised Trie** adapted to multi-key leaves, with a 16-invariant catalogue and an engineering campaign reducing a reproducer's violations from 127 to a single, reads-correct, marginal case (Ch. 5).
4. An **incremental path summary** with deferred per-path statistics (HyperLogLog cardinality, Roaring page bitmaps) that turns *N* value writes into O(distinct-paths) copy-on-write operations (Ch. 6).
5. An **index-aware, cost-based query optimiser** over an embedded JSONiq/XQuery engine, including a path-rewrite that must respect node-kind to avoid a destructive-replace correctness bug (Ch. 7).
6. A **columnar/SIMD execution** path with frame-of-reference and dictionary encodings, validated by differential testing — presented with its genuine in-progress caveats (Ch. 8).
7. A pluggable **enterprise I/O** family (io_uring via the Java FFM API with no JNI; S3; Kafka), and the honest finding that an io_uring linked-commit chain is *slower* than the serial path for single-resource commits (Ch. 9).
8. An **honest evaluation** against PostgreSQL that the relational engine wins in the small-document regime, with a precise account of why (Ch. 11).

### 1.5 The shape of the system

```
                         ┌───────────────────────────────────────────────┐
   Web GUI (SolidJS) ───▶│  REST API  (Vert.x)                            │
   brackit clients   ───▶│  Query layer: JSONiq/XQuery + cost-based opt.  │  Ch.7
                         │  Vectorised columnar executor (SIMD)           │  Ch.8
                         ├───────────────────────────────────────────────┤
                         │  Transactional node layer                     │
                         │  cursors · path summary · indexes · hashing   │  Ch.6
                         ├───────────────────────────────────────────────┤
                         │  Page layer:  CoW trie · sliding snapshot     │  Ch.2-3
                         │               HOT index/navigation            │  Ch.5
                         ├───────────────────────────────────────────────┤
                         │  Storage SPI: dual-beacon commit, recovery    │  Ch.4
                         │  FileChannel · mmap · io_uring · S3 · Kafka    │  Ch.9
                         └───────────────────────────────────────────────┘
```

The remaining chapters treat each layer in turn.

---

## 2. The Persistent Data Structure

### 2.1 The copy-on-write page trie

The physical hierarchy is a multi-level trie mapping a 64-bit logical *node key* to a physical page. Its apex is the **`UberPage`** (`io/sirix/page/UberPage.java`), a singleton resource descriptor holding the reference to the current **`RevisionRootPage`**. Each `RevisionRootPage` (`io/sirix/page/RevisionRootPage.java`) anchors one immutable revision and carries the revision number, commit timestamp, the maximum allocated node key, and references to the document's primary node index plus the auxiliary structures (path summary, name page, and the CAS/path/name/DeweyID/projection-index roots).

```
                  UberPage  (one per resource; pointer is swapped atomically at commit)
                     │  getRevisionRootReference()
                     ▼
              RevisionRootPage  (revision N: revNo, timestamp, maxNodeKey,
                     │           refs → {document index, pathSummary, name,
                     │                   CAS, path, name, deweyID, projection})
                     ▼
              IndirectPage  ── 1024 references ──┐    (height grows with maxNodeKey)
                     ▼                           ▼
              IndirectPage                   IndirectPage
                     ▼
              KeyValueLeafPage  (≤ 1024 records, off-heap MemorySegment, slot bitmap)
```

From the document-index reference, a chain of **`IndirectPage`** nodes (`io/sirix/page/IndirectPage.java`) decomposes the node key into 10-bit slices, each indexing a 1024-entry reference array. Navigation extracts successive 10-bit offsets via the exponent ladder `[70,60,50,40,30,20,10,0]`:

```
Algorithm 2.1  Trie navigation: nodeKey → leaf
  input: rootRef, pageKey         // pageKey = nodeKey >> log2(recordsPerPage)
  page ← deref(rootRef)
  level ← pageKey
  for exp in [70,60,50,40,30,20,10,0][8 - height ..]:   // only the live levels
      offset ← level >> exp
      level  ← level − (offset << exp)
      page   ← deref(page.getReference(offset))
  return page                      // a KeyValueLeafPage
```

The trie **height grows dynamically**: a document whose keys fit in 10 bits needs a single indirect level (direct leaf access); larger key spaces add levels lazily. With fan-out 1024, depth is `⌈log₁₀₂₄(maxNodeKey)⌉` — at most seven indirections for any realistic document — so node lookup is effectively constant-time while small documents pay no structural overhead.

The terminal page is the **`KeyValueLeafPage`** (`io/sirix/page/KeyValueLeafPage.java`):

```
KeyValueLeafPage  (off-heap MemorySegment)
 ├─ populated bitmap: 16 × long  = 1024 bits   (bit i ⇒ slot i occupied)
 ├─ slot table:       1024 × int (byte offsets into the payload arena)
 ├─ payload arena:    serialized DataRecords, variable length
 ├─ (optional) DeweyID region: inline encoded DeweyIDs, parallel to slots
 └─ overflow map:     PageReference entries for records > ~150 KB
```

A record is addressed by `slotIndex = nodeKey & 1023`; the 1024-bit bitmap makes iteration `O(populated)` rather than `O(1024)`. Records exceeding the maximum inline size (~150 KB) spill to **overflow `PageReference`** entries, which must be carried through every fragment recombination (Ch. 3) to avoid loss.

### 2.2 Structural sharing — the heart of the system

When revision *N+1* mutates a node, only the pages on its root-to-leaf path are copied; sibling subtrees keep their old references. The `UberPage` is itself immutable — a commit publishes a *new* `RevisionRootPage` and atomically swaps the pointer.

```
Revision N                         Revision N+1  (modify a leaf under B)
   Root_N                              Root_{N+1}          ← new root
   ├─▶ A ─▶ C                          ├─▶ A ─▶ C          ← A, C SHARED (unchanged)
   └─▶ B ─▶ D                          └─▶ B' ─▶ D'        ← B', D' copied (on the path)
```

For a write touching *k* pages of an *n*-page document, pages written = `O(k + height)` and storage growth = `O(actual delta)`, not `O(n)`. This is the cost-shape claim of §1.2 made concrete. Every record additionally carries `previousRevision` and `lastModifiedRevision`, enabling temporal predicates ("in which revisions did node 12345 exist or change?") to be answered structurally.

**Property 2.1 (Snapshot immutability).** *For any committed revision N, the set of pages reachable from `Root_N` is fixed for all time; no later commit mutates a page in that set. Hence a read transaction bound to revision N observes a consistent snapshot with no locks.* — This follows directly from copy-on-write: a mutation produces new pages and never overwrites a page referenced by an already-published root.

### 2.3 DeweyIDs

Optionally (`ResourceConfiguration.Builder#useDeweyIDs(true)`), each node receives a **`SirixDeweyID`** (`io/sirix/node/SirixDeweyID.java`): a hierarchical, outline-style identifier (e.g. `1.3.5.7`) using a variable-length, prefix-free binary encoding. Divisions are encoded tier-by-tier with prefix-free tier codes:

```
tier 0   0…127           prefix 0    + 7-bit value
tier 1   128…16 511      prefix 10   + 14-bit value
tier 2   16 512…2.1 M    prefix 110  + 21-bit value
tier 3   …270 M          prefix 1110 + 28-bit value
tier 4   …2.1 G          prefix 1111 + 31-bit value
encode(d) = encode(d₀) ∥ encode(d₁) ∥ … ∥ encode(dₙ)
```

**Theorem 2.2 (Order preservation; `docs/DEWEYID_HOT_INDEX_FORMAL_PROOF.md`).** *For all `a,b`: `a.compareTo(b) < 0  ⟺  compareUnsigned(encode(a), encode(b)) < 0`.* The proof rests on the prefix-free tier codes (no encoding is a prefix of another) and order-preserving within-tier suffix comparison; tier-boundary values were shown to round-trip correctly after a 2026-01 fix. This is what lets a HOT index over encoded DeweyIDs (Ch. 5) yield results in document order.

DeweyIDs give O(1) ancestor/descendant tests (prefix containment), O(1) document-order comparison, and sibling insertion without renumbering (using an inter-sibling distance, default 16), at the cost of inline storage and occasional re-assignment under dense insertion.

### 2.4 Node hashing

Controlled by `HashType` (`io/sirix/access/trx/node/HashType.java`), SirixDB maintains a **rolling Merkle hash** over the node tree. Each node stores a 64-bit hash computed (in `node/json/*.java`) over its identity, kind, structural pointers, name key, and aggregated child/descendant counts:

```
hash(node) = XXH3( nodeKey ∥ parentKey ∥ kind ∥ childCount ∥ descendantCount
                   ∥ leftSibling ∥ rightSibling ∥ firstChild [∥ lastChild] ∥ nameKey )
```

On mutation the node's hash is recomputed and the change propagates up the ancestor chain to the root, forming a Merkle tree whose root digests the whole document. The decisive payoff is **hash-guided diffing**: two revisions are compared by descending only into subtrees whose hashes differ, never deserialising unchanged nodes — the basis of the sub-millisecond semantic diff measured in Ch. 11. `POSTORDER` is a heavier variant; `NONE` trades diff/integrity capability for ~13 % more commit throughput (measured, §11.1).

---

## 3. Versioning: The Sliding Snapshot

### 3.1 The strategy space

The page layer reconstructs a logical page from a chain of **page fragments** linked by `PageFragmentKey`s, under one of four strategies (`io/sirix/settings/VersioningType.java`):

| Strategy | On-disk per commit | Read (fragments combined) | Write spikes | Storage |
|---|---|---|---|---|
| **FULL** | whole page | 1 (O(1)) | none | high — O(page) every change |
| **INCREMENTAL** | delta; full every `R` revisions | ≤ `R` | periodic full rewrite | low between spikes |
| **DIFFERENTIAL** | delta-from-last-full | ≤ 2 (O(1)) | none | delta grows unbounded |
| **SLIDING_SNAPSHOT** (default) | sparse delta + preserved tail | ≤ `R` | **none** | bounded, even |

FULL is simplest but write-heavy; INCREMENTAL keeps deltas small but re-emits whole pages periodically (a 6× write spike for a one-record change in a 1024-record page); DIFFERENTIAL bounds reads at two fragments but lets the delta grow without limit. SLIDING_SNAPSHOT combines INCREMENTAL's small deltas with DIFFERENTIAL's bounded reads while eliminating periodic spikes — the contribution that defines the system.

### 3.2 The sliding-snapshot algorithm

Let `R = revisionsToRestore` (the window; default a small constant). On modification, the page is reconstructed and the *next* fragment is prepared in two phases:

```
Algorithm 3.1  SLIDING_SNAPSHOT.combineRecordPagesForModification
  input: fragment chain F = [f₀ (newest) … f_{m}], window R
  complete ← empty page;  inWindow ← 1024-bit zero bitmap
  // Phase 1 — in-window merge (newest wins)
  for f in f₀ … f_{min(R-1, m)}:               // newest R-1 fragments
      for slot s populated in f:
          mark s in inWindow
          if s not in complete:  complete[s] ← f[s]     // newer shadows older
  // Phase 2 — out-of-window preservation
  if |chain| == R:                              // an oldest fragment is about to fall out
      f_old ← the R-th fragment
      for slot s populated in f_old:
          if s not in complete:  complete[s] ← f_old[s]          // fill a gap
          if s not in inWindow:  modifying.markSlotForPreservation(s)  // carry forward
  return (complete /*read view*/, modifying /*sparse page written at commit*/)
```

Phase 1 merges the newest `R-1` fragments, newer slots shadowing older. Phase 2 inspects the single fragment about to leave the window: any slot it holds that *no in-window fragment covers* is **marked for preservation** — copied forward lazily so the next on-disk fragment remains self-sufficient within the window.

**Property 3.1 (Bounded reconstruction).** *A logical page is fully reconstructable from at most `R` fragments at any revision.* Each record leaving the window is preserved into the new fragment exactly when no newer fragment already covers it, so the union of any `R` consecutive fragments covers every live slot.

**Property 3.2 (Even amortisation).** *Each record is preserved at most once per window pass.* Preservation is triggered only for slots uncovered in-window and is not repeated while the record stays covered, spreading the carry-forward cost across revisions (≈ 1/`R` extra writes per revision) rather than concentrating it in periodic full rewrites.

Special cases the implementation handles: **overflow records** (large values stored as references) are carried through combination; **FSST symbol tables** (string compression) are propagated on single-fragment combines and decompressed-on-merge for multi-fragment combines to keep the dictionary coherent.

### 3.3 The Transaction Intent Log

Uncommitted state lives in the **Transaction Intent Log (TIL)** as a list of `PageContainer`s, each holding a `complete` reconstruction (the read view during modification) and a sparse `modified` page (what is serialised at commit). Commit serialises the modified pages, writes them to the append-only data file, and links them into the new `RevisionRootPage`; rollback discards the TIL. This separation is why a read-during-write sees a consistent in-progress view without exposing partial state.

---

## 4. The On-Disk Format and the Durable-Commit Protocol

*(`docs/DISK_FORMAT.md`; `io/sirix/io/*`; `io/sirix/page/PagePersister.java`.)*

### 4.1 File layout

A resource's `FILE_CHANNEL`/`MEMORY_MAPPED` storage is two files. Both open with an identical 64-byte **superblock** (`io/sirix/io/Superblock.java`) validated fail-fast at open: magic `"SIRIXDB!"`, layout version 0, a role byte, an endianness-check word, geometry, and an XXH3-64 CRC over the prefix.

```
sirix.data
  0       Superblock (64 B): magic, version, role=DATA, endianness, geometry, XXH3 CRC
  64..4096  sparse hole (written only at first commit)
  4096    PRIMARY beacon slot   [u32 len][UberPage payload][u64 XXH3][zero pad]   (4 KiB block)
  8192    SECONDARY beacon slot [identical copy — a SEPARATE 4 KiB block]         (4 KiB block)
  12288   DATA_REGION_START — append-only page records: [u32 len][payload], 8-aligned
          …───────────────────────────────────────────────────────────────────▶ (grows)

sirix.revisions
  0       Superblock (role=REVISIONS)
  4096    REVISIONS_RECORDS_START — fixed 32-byte slots, slot N at 4096 + N*32:
          [u64 rootPageOffset][u64 epochMillis][u64 checksum][u64 rootPageHash]
```

Endianness regimes: superblocks, beacon checksums, and revision records are pinned **little-endian**; page-record length prefixes and payload primitives are currently host-order, gated by the endianness check (full LE pinning is a pre-format-freeze item). The **deterministic** revision-slot layout (slot N is always at `4096 + N*32`, never appended at file-size) means a failed commit cannot shift later slots, and recovery can derive the *logical* write frontier from the durable revision graph rather than from `fileChannel.size()` — the property the preallocated-commit mode (§4.5) depends on. The fourth revision-record field is the XXH3-64 of the revision-root page's compressed body — the only integrity anchor on the otherwise parent-less `readRevisionRootPage` path. The checksum covers 24 bytes when a hash is present and 16 for legacy records, so older resources open without false corruption errors.

### 4.2 The commit protocol and its crash contract

The dual-beacon protocol writes the new uber page to two separate 4 KiB blocks, after the page tail is durable:

```
Algorithm 4.1  Commit (default profile, FileChannelWriter.writeUberPageReference)
  flush buffered page tail (ends with the new RevisionRootPage)
  write superblocks if first commit
  force(false)  data                     # WRITE-AHEAD BARRIER (tail durable)
  # revision record already written via O_SYNC channel, durable at write-return
  write SECONDARY beacon  (buffered to data channel)
  write PRIMARY beacon    (buffered to data channel)
  force(false)  data                     # COMMIT-END BARRIER (both beacons durable)
```

**The crash contract.** Because the tail is fsynced *before* any beacon is written, and the two beacon copies are in separate blocks:

| Crash point | PRIMARY | SECONDARY | Outcome on reopen |
|---|---|---|---|
| before write-ahead barrier | old | old | previous revision (commit never acknowledged) — correct |
| after barrier, before beacons | old | old | previous revision — correct |
| mid-beacon writes (one torn) | new/torn | old/new | reader validates `[len][payload][XXH3]`, falls back to the intact copy, naming a *durable* tail |
| after commit-end barrier | new | new | new revision — acknowledged |

**Property 4.2 (Torn-block safety).** *Whichever beacon copy survives a torn-block crash names a durable tail, because the tail is fsynced before any beacon is written and the two copies occupy distinct filesystem blocks.* A subtle residual case — a crash between the secondary and primary writes that leaves the secondary advertising a *truncated-away* revision — is healed by `repairBeaconSlotsAfterTruncate`, which copies the good slot over the stale one so both agree.

### 4.3 Recovery and integrity

Recovery (`io/sirix/io/AbstractReader.java`, `FileChannelWriter.java`, `InterruptedFirstCommitRecovery.java`) rests on three pillars: **dual-beacon redundancy** (validate bounds + XXH3, fall back PRIMARY→SECONDARY); **checksummed records and page bodies** (a torn record or page is caught before it is trusted); and **beacon repair** after truncation. The **interrupted-first-commit** gap — a crash before a resource's header/beacons exist, leaving a non-empty file with a zero header — is healed *conservatively* by `InterruptedFirstCommitRecovery`: it re-initialises the resource empty only if the on-disk bytes *prove* that at most the empty bootstrap revision was ever acknowledged (all-zero or checksum-valid superblock; beacons either zero or advertising only revision 0; and no checksum-valid revision record); otherwise it rethrows the original failure untouched.

The whole protocol is exercised by **`CrashRecoveryInjectionTest`** (`io/sirix/crash/`), an opt-in (`-Dsirix.crash.run=true`) harness that forks a writer, `SIGKILL`s it at random instants in its commit loop, and verifies on reopen that: the database opens; every acknowledged commit is intact; all pages deserialise; the truncate-recovery path runs when the `.commit` marker is present; and the resource accepts a new writer. A complementary power-loss simulator models a stricter metadata-split durability than POSIX fdatasync, which is *why* (see §4.4) the two data barriers that cover the tail append remain full fsyncs deliberately even when the rest of the protocol relaxes.

### 4.4 The commit protocol's evolution (an engineering case study)

The protocol reproduced in §4.2 is the endpoint of a measured journey (`docs/BENCHMARKS.md`). The instructive part is how *much* of the per-commit cost was incidental rather than fundamental.

1. **Seven sync calls per commit.** The original protocol issued 5 `fsync` + 2 `fdatasync`. A redundant `forceAll` (`t3`) ran while the tail was still buffered and covered strictly less than `writeUberPageReference`'s internal barrier. Removing it and reducing the acknowledge barrier to a data-only `fdatasync` cut the count to **four** (fsync data write-ahead; fsync revisions; fdatasync data beacon-order; fsync data acknowledge).
2. **A quadratic `access(2)` pathology.** Wall-clock profiling of a 10 000-commit build showed the late phase dominated by `access(2)`. A syscall census revealed **50 196 928** `access()` calls over the build (~50 M of them `ENOENT`), versus 520 k for 1 000 commits — a perfect `Σi ≈ N²/2`. Root cause: `initializeIndexController` probed `revision.xml, (revision-1).xml, …, 0.xml` with one `Files.exists` per step to find the latest index definitions, and a new controller is created *per commit*. With no secondary indexes (the default), no file ever exists, so every commit walked the entire history. Fix: a single directory listing picking the max-numbered file ≤ revision (an empty directory short-circuits). This, plus an **amortised** `RevisionIndex.withNewRevision` (capacity-doubling shared arrays with a deferred Eytzinger rebuild, replacing a full O(size) array copy per commit), turned the build from **48.4 s (4.84 ms/commit, declining to ~150 commits/s at depth 10 k)** into **20.5 s (2.05 ms/commit, flat ~570 commits/s)** — the decline *eliminated*, not merely reduced.
3. **Toward one explicit sync.** The "one explicit sync" design was then implemented: the revisions record goes through an `O_SYNC` channel (durable incl. size at write-return) and both beacons through an `O_DSYNC` channel (in-place; write-return gives secondary-before-primary ordering and makes the primary's return the commit acknowledge), leaving a *single* explicit `fdatasync` for the data tail. On consumer NVMe with FUA this measured at **parity** with the 4-sync protocol (three serialised write-through round-trips cost about what the saved flushes did), with the structural win of an explicit durable-on-return contract; gains are expected on server stacks where FUA is materially cheaper than a cache flush. All power-loss and SIGKILL gates re-validated green.

The lesson — that most of a commit's apparent cost was redundant syscalls and a hidden quadratic, not the durability barriers themselves — directly motivated the preallocated profile below.

### 4.5 The preallocated, journal-free commit profile

Two flags, **on by default**, configure the current commit path: `sirix.commit.preallocated` and `sirix.commit.bufferedBeacons`.

Under preallocation, the data and revisions files are grown ahead of the write frontier in **geometrically increasing chunks** (`sirix.commit.preallocInitialChunkBytes` 64 KiB, doubling to a `preallocChunkBytes` 64 MiB cap). A low-commit resource therefore ends only a chunk or two past its real data, with a fast first commit; a high-commit-rate resource ramps to the cap and amortises the one-time per-chunk fsync over many commits. Because `i_size` is then stable across commits, the write-ahead barrier becomes a **journal-free `fdatasync`** instead of a growing-file `fsync` (which on ext4/xfs forces a metadata-journal commit). The logical write frontier is **derived from the durable revision graph** — read the primary/secondary beacon for the last committed revision, then its offset from the revisions file — never from the preallocation-inflated `fileChannel.size()`.

`bufferedBeacons` writes the two beacon copies buffered and makes them durable with a single commit-end `fdatasync`, rather than two O_DSYNC (FUA) writes — one fewer device round-trip per commit, preserving both the write-ahead ordering and the two-copy redundancy.

A *true* single-`fdatasync` design (folding tail and beacons into one barrier) was evaluated and **rejected**. Its worst case — a single unordered flush in which both beacon blocks reach the device before the tail — leaves *no surviving prior beacon* and would demand prior-uber reconstruction or a ping-pong scheme; and ping-pong sacrifices the dual-beacon redundancy, so a single bit-rotted beacon block could lose an *already-acknowledged* revision. The chosen flags therefore change only the *I/O shape*, never the durability model. The new default has been validated under the SIGKILL injection harness (25/25 random-timing kills, no acknowledged-data loss) and across the full unit suite (9 255 tests; the only failures were file-size assertions in tests that assumed a growing file, since preallocated commits write in place). `MEMORY_MAPPED` uses its own mmap/msync path, ignores the flags, and remains format-compatible (it reads preallocated files via the beacon; the trailing zeros are unreachable).

A composable **byte-handler pipeline** (`ByteHandlerPipeline`) applies per-page structural encodings (zero-run, byte-run, an LZ4-format codec selected per page) and optionally an outer compressor; it supports zero-copy `MemorySegment` operation. Hashing is XXH3-64 throughout, computed zero-allocation over native segments.

---

## 5. HOT: The Height-Optimised Trie

*(`docs/HOT_*.md`; `io/sirix/page/HOTLeafPage.java`, `HOTIndirectPage.java`; `io/sirix/access/trx/page/HOTTrieReader.java`, `HOTTrieWriter.java`, `HOTRangeCursor.java`.)*

### 5.1 Motivation

HOT is a cache-conscious, height-optimised trie used for index and intra-page navigation, replacing heavyweight B⁺-tree-style indirection over flat arrays. Rather than one tree level per bit, a HOT indirect node captures *k* **discriminative bits** (0–32) at chosen absolute bit positions and routes among 2–32 children by their values at exactly those positions — packing multiple bits per level so a typical node fits in one or two cache lines. It is the structure behind the DeweyID forward/reverse indexes (Ch. 6) and the projection index (Ch. 8), and is designed generically for any prefix-free, bit-discriminable key serialisation.

### 5.2 The structure: discriminative bits and the sparse path

For each child slot, the stored partial key is the parallel-bit-extract of that child's first key under the node's mask:

```
stored[c] = PEXT(firstKey[c], mask)        // the "sparse path": only on-path bits kept
```

Worked example. Let `firstKey = 0b1101_1010` and `mask = 0b1100_0001` (bits 0, 6, 7).
`PEXT(0b1101_1010, 0b1100_0001)` gathers bits {7,6,0} = `1,1,0` → stored partial `0b110`.

Splitting a node to add a new discriminative bit β is a `compress ∘ expand` round-trip:

```
Algorithm 5.1  Add discriminative bit β to a node
  for each slot c:
      full  ← PDEP/Long.expand(stored[c], oldMask)   // restore old-width bits
  newMask ← oldMask | (1 << β)
  for each slot c:
      stored[c] ← PEXT/Long.compress(full_c possibly with β set, newMask)
```

**Invariant I-PDEP-PEXT (`docs/HOT_INVARIANTS_CATALOG.md`).** `compress ∘ expand` is the identity on the old partial-key space — the round-trip is bijective, so routing is preserved across the split.

Three node layouts encode the mask and partials in the page's `MemorySegment`:

```
BiNode (2 children):   [1B initialBytePos][8B bitMask(single bit)]                + child refs
SingleMask (3–32):     [1B initialBytePos][8B bitMask][N×4B partial][N×8B childRef]   (all bits in one 8B window)
MultiMask (bits spread):[extractionPositions[]][extractionMasks[]][partials in 2×32B slots] + child refs
```

The read path descends to the highest-popcount slot whose stored partial is a subset of the query's dense PEXT, then binary-searches the terminal leaf:

```
Algorithm 5.2  HOTTrieReader.getRecord(node, key)
  if node is leaf: binary-search entries by key; return value-slice or ⊥
  densePK ← PEXT(key, node.mask)
  c ← argmax_{slots s : stored[s] ⊆ densePK} popcount(stored[s])   // most specific match
  return getRecord(child[c], key)
```

`HOTRangeCursor` yields ordered ranges from a leaf, optionally prefetching sibling leaves.

### 5.3 Invariants and the multi-key-leaf deviation

The implementation tracks sixteen invariants. A representative subset:

| Inv. | Statement |
|---|---|
| I2 | leaf entries are lexicographically sorted, keys unique |
| I3/I4 | partial keys unique; the leftmost slot's partial is 0 |
| I5 (β-constancy) | every key under a slot agrees with the stored partial at the node's bits |
| I6 | `descend(K)` lands at K's actual leaf |
| I7/I8 | partials sorted; children sorted by first key |
| I9/I10 | height ≤ 64; fan-out ∈ [2,32] |
| I-CoW / I-MR | page-key identity preserved across CoW; multi-revision read isolation |

SirixDB deviates from the Binna reference by allowing **up to ~512 keys per leaf**, which makes β-constancy (I5) non-trivial: a leaf can legitimately contain keys that differ at an ancestor's discriminative bit (in a single-key leaf, every bit is trivially constant). This routing subtlety is the source of essentially all the difficulty below.

### 5.4 The hardening campaign

A documented campaign (`docs/HOT_CAMPAIGN_RESULTS.md`, `HOT_OPERATIONS_INVARIANTS_MATRIX.md`) built a validator across 35 mutating operations × 16 invariants and drove a 50 000-insert reproducer's violations **from 127 to 1** across 21 attempts. The recurring pattern was that closing one cell of the operations×invariants matrix opened another: e.g. an attempt that tightened the I8 (children-sorted) gate cascaded to ~6 000 violations elsewhere; a reroute attempt to ~28 000. The empirical failure table showed the residual violations emerging from *happy-path* writer operations with zero recovery-counter firings — the default paths, not exotic fallbacks. The single remaining marginal I8 violation is **not a production-correctness defect**: production reads remain correct because the reader's structural walk-up compensates. Eliminating it cleanly is bounded by routing-encoding semantics (stored = PEXT(firstKey, mask) cannot capture a bit that is constant across an older subtree but variable across a newer one) and is scoped as a routing-encoding rework (`docs/HOT_ROUTING_ENCODING_REWRITE.md`, options: AND-encoding, proactive disc-bit extension, or recompute-on-CoW).

The DeweyID-HOT secondary index is backed by formal order-preservation and lookup-correctness proofs (Theorem 2.2 and `docs/DEWEYID_HOT_INDEX_FORMAL_PROOF.md`), including tier-boundary and deep-hierarchy edge cases.

*(Honesty note: HOT is an area of active, documented engineering; the "1 remaining marginal violation, production reads correct" status is the project's own characterisation, reproduced here as such.)*

---

## 6. The Path Summary and Secondary Indexes

### 6.1 The path summary

The **path summary** (`io/sirix/index/path/summary/`) is a compact tree of every *distinct* path class in a resource: all `title` elements at a given depth collapse to one `PathNode`, regardless of how many document nodes instantiate them.

```
Document (many nodes)                Path summary (one node per path class)
  /store                               /store                       references=1
    /book[0]/title  "A"                  /book      (ARRAY)          references=1
    /book[1]/title  "B"                    /title   (OBJ_NAMED_STR)  references=N  + PathStats
    /book[2]/title  "C"
```

`PathNode` (`PathNode.java`) is a single inlined heap object — identity, full struct-node state, naming keys, kind, a `references` count of document nodes using the path, level, and optional `PathStats` — replacing the legacy three-delegate pointer chase with one load. The summary is maintained **incrementally** during writes: `PathSummaryWriter.getPathNodeKey` resolves or creates the child path node in O(1) via a primitive `childLookupCache`, incrementing or initialising its `references`; deletion decrements, removing the path subtree only when the last user departs.

Per-path **statistics** (`PathStats`: count, sum, min/max, null count, a HyperLogLog cardinality sketch, and a Roaring bitmap of the leaf pages holding values) are accumulated cheaply. Value observations are **buffered** in a `pendingStats` map during the transaction and applied in a single copy-on-write pass per path at commit (`flushPendingStats`):

**Property 6.1 (Deferred-stats cost).** *Maintaining per-path statistics over a transaction touching `V` values across `P` distinct paths costs `O(P)` copy-on-write operations, not `O(V)`.* This is decisive for analytical loads: 100 M rows over a few hundred paths incur a few hundred CoW updates at commit, not 100 M.

### 6.2 The index family

Three secondary index types are declared by `IndexDef` and persisted as red-black (or HOT) trees rooted in per-index references on the `RevisionRootPage`:

| Type | Page | Key → Value | Serves |
|---|---|---|---|
| **PATH** | `PathPage` | path class → node-key set | `//title`, path navigation |
| **CAS** | `CASPage` | (value, path class) → node-key set | `[price > 50]` value predicates (typed; optional uniqueness) |
| **NAME** | `NamePage` | name → node-key set | `*[name()='price']` (include/exclude filters) |

A **DeweyID index** (`DeweyIDPage`) provides forward (DeweyID→nodeKey, document order) and reverse (nodeKey→DeweyID) lookups. The `NamePage` also holds the **name dictionary** (interned element/attribute/key names with usage counts); its reconstruction is an open design item (`docs/NAME_DICTIONARY_RECONSTRUCTION_PLAN.md`) because the current high-water-mark scan grows under name churn — a documented invariant set (I-N1…I-N7) constrains any fix.

Index maintenance is driven by per-transaction **listeners** (`PathIndexListener`, `CASIndexListener`, `NameIndexListener`) that the `IndexController` notifies on every insert/modify/delete; each consults the path summary for path/type eligibility and updates its tree's `NodeReferences` set. New indexes are populated by a full `DescendantAxis` scan (`IndexBuilder`) and persisted on the next commit.

---

## 7. Query Processing and Optimisation

*(`brackit`; `io/sirix/query/*`; `docs/cost-based-optimizer-design.md`; `cascades-optimizer-architecture.md`.)*

### 7.1 Brackit: the JSONiq/XQuery engine

SirixDB embeds **brackit**. A query is parsed into an `AST` whose nodes double as the optimiser's annotation medium and the translator's blueprint; FLWOR expressions normalise into tuple pipelines (for/let/where/group/order/return). Execution is a **bind-evaluate (Volcano) iterator model**: every `Expr.evaluate(QueryContext, Tuple)` returns a lazy `Sequence` of `Item`s pulled on demand, so operators avoid materialisation. A **store SPI** lets back-ends plug in without touching the compiler/runtime.

### 7.2 The SirixDB binding and temporal functions

SirixDB implements that SPI as `JsonDBStore`/`JsonDBCollection` (`io/sirix/query/json/`), wrapping a Sirix `Database<JsonResourceSession>`; collections are time-aware (`getDocument(name, Instant)` opens a read-only transaction at a point in time). Document nodes surface as a lazy `JsonDBItem` hierarchy wrapping a cursor and materialising fields on access. The `jn:` family adds the temporal surface — `jn:doc(db,res[,rev])`, `jn:changes(...)`, `jn:diff(rev1,rev2[,path])` — layering brackit's language directly over the bitemporal store.

### 7.3 Index-aware path rewriting (and a correctness bug worth studying)

The optimiser's centrepiece is the path-index rewrite (`AbstractJsonPathWalker` and subclasses `JsonPathStep`, `JsonCASStep`, `JsonObjectKeyNameStep`). It extracts the JSON access path of a `jn:doc(...)[...]` expression, matches it against the path summary, and rewrites the subtree into an `IndexExpr` backed by a PATH or CAS index when applicable.

A subtle, historically real bug: for `.store.book[][?$$.price gt 10]`, the rewrite fuses the scalar predicate leaf (`price`) into the path. The path-summary lookup may search for `price` as an `OBJECT_NAMED_OBJECT` and find nothing, even though `price` exists as an `OBJECT_NAMED_NUMBER`. Treating "no match of the assumed kind" as "the path is empty" once **destructively replaced a live subtree with the empty sequence**, silently dropping results. The fix checks segment existence *regardless of kind* before any such replacement. The lesson generalises: index rewrites must distinguish "this path genuinely yields nothing" from "my kind assumption was wrong", or they become silent-wrong-result generators.

Rewrites are gated by a cost flag so the optimiser can prefer a sequential scan when an index would not pay.

### 7.4 The cost-based optimiser

The optimiser (`docs/cost-based-optimizer-design.md`) estimates cardinalities and selectivities — default heuristics (≈1 % equality, ≈33 % range) refined by most-common-value lists and equi-width/equi-depth histograms drawn from the path-summary statistics — and compares a sequential-scan cost against an index-scan cost that reflects Sirix's trie storage and prefetch behaviour. Illustratively, for a 10 M-row document with 10 matching rows the index plan is on the order of 10⁴× cheaper, while at 90 % selectivity the sequential scan wins — exactly the crossover a cost model exists to find. Joins are reordered by dynamic programming (`DPhyp`, with a greedy `GOO` fallback beyond ~20 relations), and plan alternatives are recorded in equivalence classes from which the cheapest is selected.

A bitemporal optimisation is exploited: **historical revisions are immutable, so their statistics/histograms are cacheable without expiry** — only the latest revision's statistics go stale after writes. The whole compile is plan-cached on `queryText + indexSchemaVersion` and bounded by a timeout circuit-breaker.

*(Honesty note: several cost-model specifics, the multi-stage "mesh" search, and the full Cascades memo/rule/property-enforcement optimiser (`cascades-optimizer-architecture.md`) are drawn from design documents and are, in places, ahead of or divergent from the shipping code; they are presented as the intended design.)*

---

## 8. Vectorised and Columnar Execution

*(`io/sirix/page/pax/*`; `io/sirix/index/projection/*`; `io/sirix/query/scan/SirixVectorizedExecutor.java`.)*

### 8.1 Model and a naming hazard

For analytical patterns — aggregates, filtered counts, group-by-count over JSON arrays — SirixDB can leave the row-at-a-time iterator for a **batch-oriented columnar path** processing fixed 1024-row batches with SIMD. A naming hazard to dispel: `VectorPage` is the root of an *embedding / k-nearest-neighbour vector-search* index, a different subsystem; the columnar machinery lives in `pax` and `projection`.

### 8.2 Columnar storage

Two facilities exist. **PAX regions** (`io/sirix/page/pax/`) compress numeric/boolean/string field groups *within* a `KeyValueLeafPage`, grouping a parent key's children into contiguous, SIMD-friendly columns. **Projection-index leaf pages** (`io/sirix/index/projection/ProjectionIndexLeafPage.java`) are a secondary, declared projection of up to 1024 rows of a fixed field schema:

```
ProjectionIndexLeafPage (per column, by kind):
  NUMERIC_LONG : [min][max] zone-map · valueBitWidth · valueBase (frame-of-reference) · packedValues
  BOOLEAN      : packed bitset
  STRING_DICT  : localDict(lengths, concatenated UTF-8) · dictIdBitWidth · packedDictIds
  v2 tail      : per-column flags · presence bitmaps · "PIX2" magic   (v1 readers ignore trailing bytes)
```

These are orthogonal to the row-oriented primary storage and rebuilt independently. (Acknowledged debt: a projection leaf is currently stored as a single HOT value, which breaks the sliding-snapshot share ratio on update-heavy workloads — reads stay correct; a sub-slot refactor is pending.)

### 8.3 SIMD execution and the executor

Comparison kernels (`io/sirix/page/pax/NumberRegionSimd.java`) use the **Java Vector API** (`jdk.incubator.vector`):

```
Algorithm 8.1  Vectorised count(values OP threshold)
  thr ← broadcast(threshold);  count ← 0
  for i in start … end-LANES step LANES:
      v    ← LongVector.fromMemorySegment(payload, off(i), LITTLE_ENDIAN)
      mask ← v.compare(OP, thr)            // VectorMask, one bit per lane
      count += mask.trueCount()            // popcount
  count += scalar_tail(...)
  return count
```

Conjunctions AND masks; bit-packed widths up to ~56 are gathered and unpacked, wider widths fall to scalar. The `SirixVectorizedExecutor` fans scans across a worker pool (each with its own read transaction), takes a **projection-index fast path** when a covering index carries the requested column (turning a reported ~27 s slot-walk over 100 M records into ~1 ms of in-memory SIMD), extracts conjunctive per-column predicates (fusing `>`+`<` into one `BETWEEN`), and re-enters brackit's sequence model through a thin envelope (`{groupKey,count}` arrays, scalar counts, or `[count,sum,min,max]`). Results are cached per `(session, revision)`.

### 8.4 Correctness and honest status

This path is **partly in progress**: the optimiser's vectorised detection/routing stages are presently disabled in favour of brackit's vectorized-executor SPI to avoid double-optimisation; compiled-predicate bytecode generation is HotSpot-only (disabled under GraalVM native image, falling back to the interpreter at a measured 10–30 % cost); string matching is scalar per distinct value. Correctness is guarded by **differential testing**: `PredicateTreeDifferentialTest` shreds 200 K records and compares 20 randomised predicate shapes against an in-memory ground truth (≈15 min/run, opt-in), and `NestedSameNamePathScopingTest` is a regression for a real cardinality bug where local-name-only indexing conflated a top-level `age` with a nested `pet.age` (fixed by path-filtered slot scans). A `-Dsirix.scan.diag` mode emits per-page fast-path/slow-path counters to bisect silent divergences (e.g. "fast-path recorded 100 records; slow-path re-scan found 110").

---

## 9. The Enterprise I/O Stack: io_uring, S3, Kafka

*(`/tmp/sirix-hft/sirix-enterprise-*`.)*

### 9.1 io_uring via the Java FFM (Panama) API — no JNI

The enterprise io_uring backend drives Linux io_uring **directly from Java with no JNI**, via `java.lang.foreign` (`FFIIOUring.java`): cache-line-aligned SQE/CQE layouts, `VarHandle`/`MethodHandle` accessors, and the `io_uring_setup`/`io_uring_enter`/`io_uring_register` syscalls, with a striped ring pool (≈2×cores rings, queue depth 512), best-effort registered files/buffers, and block-aligned buffers for O_DIRECT. The storage class implements the same `IOStorage` SPI and writes the **identical V0 on-disk format**, so it is wire-compatible with the FileChannel backend; it differs only in mechanism (MemorySegment I/O, async submission, O_DIRECT alignment, striped locking).

### 9.2 The linked-commit chain — and its falsification

The writer's durability barrier is an io_uring **linked-SQE chain**:

```
[ fdatasync(data)  IORING_OP_FSYNC | IORING_FSYNC_DATASYNC | IOSQE_IO_LINK ]
   └▶ [ pwrite(SECONDARY beacon)  RWF_DSYNC | IOSQE_IO_LINK ]
        └▶ [ pwrite(PRIMARY beacon)  RWF_DSYNC ]        # commit acks iff this CQE.res == slot size
```

submitted in one syscall, with a feature-probe and a `-Dsirix.iouring.linkedCommitChain` kill-switch falling back to a serial fdatasync + two O_DSYNC writes of identical durability.

The honest finding, from the **W1 commit-throughput benchmark**: the linked chain is **0.91–0.97× the serial path on a single resource** — a commit's barriers are an inherently serial dependency chain with nothing to overlap, and io_uring may punt the fsync to a kernel worker (io-wq) — so the chain is **disabled by default**, its only advantage being higher *aggregate* throughput across many concurrent resources. The same work surfaced and fixed two real bugs (a reader stripe-lock deadlock; an ENOSPC from an unreclaimed preallocated tail), which informed the (separate, FileChannel-side) preallocated-commit profile of §4.5. This is a case where the disciplined benchmark *killed* an appealing idea — exactly its job.

### 9.3 The reader and the other backends

The reader (`FFMIOUringReader.java`) does zero-copy, optionally busy-polled, batched page reads under per-thread stripe locks, with an O_DIRECT-aware aligned/unaligned split; a batched descendant-prefetch path reads *N* pages in two syscalls (header batch, data batch) rather than 2*N*. The **S3 backend** maps the format onto object storage with a hand-rolled AWS SigV4 client (no SDK), ordered manifest+revisions writes for crash consistency, and a (not-yet-wired) L2 SSD cache; a documented audit fixed manifest-load data-loss, page-layout collisions, atomicity, and reader-concurrency issues. The **Kafka backend** streams committed revisions for replication/CDC, with followers reloading the leader's authoritative S3 manifest rather than rebuilding it locally, idempotent in-order apply, and at-least-once offset semantics. *(Maturity of the enterprise modules varies and is tracked in their own production-readiness docs.)*

---

## 10. The Web GUI

*(SolidJS + Vite + TypeScript.)*

The front-end uses SolidJS fine-grained signals/stores for UI state and **TanStack Query** for server state, with structured query keys enabling surgical cache invalidation after a commit and `staleTime: Infinity` on immutable revision diffs. Vite proxies `/api` to the SirixDB REST server; **Monaco** is dynamically imported on the query page with a JSONiq Monarch grammar and a "Midnight Ember" theme; Kobalte + Tailwind provide accessible, themed primitives.

The pages map onto the temporal model: an **Explorer** that lazily expands the node tree via `maxLevel`/`maxChildren`/`startNodeKey` cursors (handling 100 GB+ resources, with windowed rendering and a path-summary view), a read-only historical mode keyed on `?revision=N`, and a pending-changes model that batches inserts/updates/deletes into a **single atomic JSONiq commit** (`replace json value of sdb:select-item(...)`, `insert`, `delete`, `sdb:commit`); a **Query** editor with templates, envelope stripping, and parsed/optimised/candidate **query-plan** visualisation; a **History** page with time-travel (floor-semantics revision-at-timestamp) and revision comparison; a **Diff** page that merges consecutive diffs client-side for non-consecutive revision pairs (cancelling insert+delete, folding update+delete) with a revision scrubber; and a **Sunburst/Treemap** view built from the path summary or paginated data.

The `SirixApiClient` (`src/lib/api.ts`) wraps the REST endpoints and tolerantly parses responses (multi-object, raw-text, and single-named-scalar-unwrap fallbacks). A well-documented subtlety is the **wire format**: SirixDB serialises object entries either as legacy nested `{metadata,value}` wrappers or as *fused* `OBJECT_NAMED_*` records whose children sit as a bare array; a single normaliser (`src/lib/sirixTree.ts`) re-wraps the fused shape so one parser handles both, avoiding the class of bug where duplicated parsers drift. Auth is Keycloak JWT with epoch-guarded refresh (a refresh resolving after logout cannot resurrect the session) and a `waitForAuth` gate that blocks data requests until a token is ready; `VITE_DEMO_MODE` yields a read-only cloud-sandbox build.

---

## 11. Evaluation

This chapter reproduces the project's own measurements (`docs/COMPARISON_POSTGRES.md`, `docs/BENCHMARKS.md`) with their caveats. The honest summary is stated first: **in the small-document, single-writer regime, PostgreSQL wins most raw numbers.** The value of the evaluation is the *analysis* of where each system's advantage actually lies.

### 11.1 SirixDB vs PostgreSQL (same machine, durability verified)

Setup: one ~2.4 KB JSON document, 5 000 single-field durable updates → 5 001 retained versions, same i7-12700H / NVMe / ext4. SirixDB embedded (GraalVM JDK 25, `FILE_CHANNEL`, `SLIDING_SNAPSHOT`); PostgreSQL 17 (`synchronous_commit=on`, `fsync=on`), server-side plpgsql loops (no client round-trips — PostgreSQL's most favourable honest setup). Durability is *verified*, not assumed: `pg_test_fsync` measured **4 778 fdatasync/s (209 µs)** on the same volume, and PostgreSQL's 4 015 commits/s is 84 % of that floor — i.e. it is fsync-bound and honestly tuned. SirixDB's per-commit protocol (write-ahead fsync + three FUA write-throughs) is, if anything, *stronger* than a single fdatasync. Both runs reproduce the identical 5 001-version history (cross-checked: counter sum = 12 502 500 on both).

| Workload | SirixDB (full) | SirixDB (lean) | PostgreSQL 17 | Winner |
|---|---|---|---|---|
| **W1** 5 000 durable single-field commits | 375 commits/s (2.66 ms) | 429 commits/s (peak 555) | **4 015** commits/s (0.25 ms) | PostgreSQL **5.5–10.7×** |
| **W2** 1 000 random point-in-time full reads | 75.7 µs/read | 74.5 µs | 17.5 µs batched · **~104 µs per-statement** | PG batched 4.3× — **SirixDB wins per-statement** |
| **W3** list 5 001 timestamps | 4.57 ms | 3.36 ms | 1.99 ms | PG ~2× (but see §11.2) |
| **W4** one field across all 5 001 versions | 55/49 ms | 49/48 ms | 6.9 ms | PG ~7× (dense field; see §11.2) |
| **W5** storage for full history | 16.4 MiB (full) | 11.8 MiB (lean) | **4.66 MiB** | PG 2.5–3.5× |
| **W6** diff of adjacent versions | **0.30 ms** node-level semantic patch | 0.40 ms | 0.15 ms top-level fields only | sub-ms tie on speed; **SirixDB on capability** |

**Why PostgreSQL wins this regime, precisely.** Its per-commit work is one compact WAL record + one fdatasync; SirixDB writes a CoW page-tree (several pages) plus an fsync barrier and (in the protocol measured) three FUA writes, hashes, and — in the full config — a per-commit diff file. For history *scans* (W3/W4), a heap scan over 5 001 pglz-compressed rows beats opening 5 001 revision contexts (~10–18 µs each). For *storage* at 2.4 KB, a compressed full copy (836 B/row) beats SirixDB's per-revision metadata floor (~2.4–3.4 KB/version: revision root + indirect pages + record fragment) — the floor exceeds a compressed full copy when the document is barely larger than the floor.

**Where SirixDB wins, and the cost-shape argument.** Per-statement read latency from an application is **75.7 µs embedded vs ~104 µs** for client-server PostgreSQL even over a local unix socket — there is no batching trick for an app that needs one document *now*. Versioning is a first-class capability, not a pattern to assemble: numbered *and* timestamped revisions, stable node identity across versions, time-travel axes, per-node history indexes, audit-grade rolling hashes, and **node-level semantic diffs in 0.3 ms** — for which the PostgreSQL side must ship both full documents to the application and diff there. Most importantly, the **cost shape**: SirixDB's per-version cost is `O(changed nodes) + fixed metadata`, *independent of document size*; PostgreSQL's is `O(document)` per version (a full jsonb copy in heap *and* WAL). At 2.4 KB this is PostgreSQL's win; the storage/write-amplification crossover plausibly sits in the tens-of-KB document range — *a claim the project explicitly refuses to quote until measured on 100 KB / 1 MB / 10 MB documents.*

The project's own positioning conclusion is reproduced verbatim in spirit: **do not pitch SirixDB as "faster than PostgreSQL for keeping history of small documents" — it is not.** The honest pitch is the cost shape, the capability set, and embedding.

### 11.2 History-path improvements (different machine; shape, not absolutes)

A reworked history read path (`getHistoryTimestamps()` from the resident in-memory `RevisionIndex`, a `long[]` + one `arraycopy`) serves all 5 001 timestamps in **~0.05 ms — ~40× faster than the PostgreSQL heap scan** and the prior SirixDB path. For a field that changes *rarely* (the common real shape), `scanValueRuns` reads **one** record and reconstructs the value across all 5 001 versions in **~0.05 ms vs PostgreSQL's ~9.8 ms (~200×)** — the O(changes) cost shape made visible. For a field that changes on *every* commit, the change set is everything, so all paths still read 5 001 records and PostgreSQL's compressed heap scan still leads (~8×); closing that dense case needs a deferred fragment-chain single-pass scan, left as future work.

### 11.3 Concurrency and the revision-depth pathology (and its fix)

A REST concurrency benchmark validated that moving blocking work off Vert.x's *ordered* worker queue (`ordered = false`) cut the p95(c=16)/p95(c=1) ratio from a queue-serialised ~16× to **~3×**, with read throughput scaling 1→16 by 4.85× before CPU saturation. It also surfaced a pathology: reading the *latest* revision of a same-sized document became ~4× slower (c=1) and ~10–12× lower throughput (c=16) once the resource carried ~1 400 revisions — and *fully recovered* after re-seeding to 6 revisions on the same JVM, ruling out aging/GC. Root cause (Ch. 4.4): every storage open eagerly reloaded the per-revision index, O(revisions) per request, and a hidden O(R²) `access(2)` walk. After the fix, read performance at 1 900 revisions **exceeds** the fresh-resource baseline (c=16: 1 042 → **18 361 req/s**, p99 245 ms → **1.84 ms**), and the large-history build is flat at ~570 commits/s through 10 000 commits. Random-revision access is **position-independent** — trx open+read is ~18 µs warm whether the target is revision 1, 5 000, or 10 000 — a direct dividend of structural sharing.

### 11.4 Threats to validity

Single machine, co-located client+server, fully cached, one document shape, single writer; "cold" drops only in-process caches (OS page cache stays warm); SirixDB is a dev build with JIT warm-up tax, PostgreSQL a GA release. The regime deliberately favours PostgreSQL. None of the large-document, many-document, many-version, or concurrent-writer regimes are measured — and the project is explicit that the cost-shape crossover must be *measured, not extrapolated in public*.

---

## 12. Related Work

**Persistent data structures.** SirixDB's structural sharing is the database-scale analogue of purely functional persistent structures (Okasaki, *Purely Functional Data Structures*): a "modification" produces a new version sharing the unchanged majority. The page trie is a wide-fan-out persistent trie; the sliding snapshot is a page-granular versioning policy layered on it.

**Copy-on-write filesystems.** WAFL, ZFS, and Btrfs popularised CoW B-trees with atomic root-pointer swaps and snapshotting; SirixDB applies the same root-swap-on-commit discipline at the document/page level, with dual beacons playing the role ZFS's überblock array plays — redundant, checksummed roots tolerant of torn writes.

**Versioned and temporal stores.** Git and Fossil version content with content-addressed structural sharing but are not queryable databases; bitemporal relational features (SQL:2011 system-versioned tables, Datomic's accumulate-only model) offer time travel at the row/datom level. SirixDB differs in offering *sub-document*, node-level history and diffs over hierarchical JSON/XML, with stable node identity across revisions — capabilities the W6/W4 results show have no native relational equivalent.

**Storage engines and durability.** Against the log-structured-merge mainstream (LevelDB/RocksDB) and the WAL-and-heap mainstream (PostgreSQL), SirixDB has *no separate WAL* — its append-only data files are the entire on-disk story. The commit-latency analysis (Ch. 4, 11) is in dialogue with the TU-Munich/LeanStore/Umbra line (Haubenschild et al. on rethinking logging/recovery; Haas & Leis on what modern NVMe can do): SirixDB adopts their core NVMe insight — group commit is a spinning-disk tax; the lever on flash is removing per-commit barrier overhead, not amortising fsync — which is precisely what the preallocated profile does. It does *not* adopt their parallelism mechanism (distributed per-worker logging with remote-flush-avoidance), because a SirixDB resource has a single writer; its analogue of their parallelism is *across* resources.

**Indexing.** HOT (Binna et al.) and ART (Leis et al.) are the lineage of the in-memory trie; SirixDB's HOT adapts the single-key-leaf reference design to multi-key leaves, which is the source of the invariant subtleties of Ch. 5. The path summary is in the tradition of XML path indexes (DataGuides, A(k)-indexes) repurposed for JSON and extended with per-path statistics for cost-based optimisation.

**Columnar execution.** The PAX layout (Ailamaki et al.) and the C-Store/MonetDB columnar tradition inform the projection-index pages and SIMD scan; frame-of-reference + bit-packing and dictionary encoding are standard lightweight column codecs. SirixDB's twist is layering an *optional, declared* columnar projection over a row-oriented, versioned primary store.

---

## 13. Conclusion and Future Work

SirixDB derives a broad surface of useful properties — time travel, structural sharing, lock-free snapshot reads, sub-document history, hash-guided diffs, append-only durability with no WAL — from one disciplined commitment to immutability and copy-on-write, executed through a fan-out-1024 page trie, a sliding-snapshot page-versioning algorithm with bounded reconstruction and even amortisation, and a self-describing, dual-beacon, checksum-protected on-disk format whose recovery is validated under SIGKILL and power-loss fault injection. Above the store sit a HOT-based navigation/index layer, a path summary with incremental statistics, a brackit JSONiq/XQuery engine with an index-aware cost-based optimiser, and an emerging columnar/SIMD execution path; alongside it, pluggable FileChannel/mmap/io_uring/S3/Kafka back-ends and a SolidJS GUI built explicitly for the temporal model. The evaluation is honest about the regime where a relational engine wins, and precise about the cost-shape and capability axes where SirixDB does.

The open frontier, stated plainly:

- **HOT** — the single remaining marginal routing invariant and its encoding rework (AND-encoding / proactive disc-bit extension / recompute-on-CoW).
- **Vectorisation** — re-enabling the optimiser's vectorised stages, the projection-leaf sub-slot refactor to restore the sliding-snapshot share ratio, and SIMD string matching.
- **Query** — the full Cascades optimiser realisation.
- **History scans** — the deferred fragment-chain single-pass scan that would close the dense-field W4 case.
- **Storage** — the name-dictionary reconstruction strategy; full little-endian pinning of page-record fields before a format freeze.
- **Durability** — for a release candidate of the new default commit profile, validation on power-loss-protected enterprise NVMe and a torn-block (not merely process-death) fault injector.
- **Evaluation** — the large-document benchmark (100 KB / 1 MB / 10 MB) that the cost-shape crossover claim is waiting on; concurrency and many-document regimes.

None of these undercut the core thesis. They are the ordinary frontier of a system that has chosen a harder, more principled foundation than most — and has, by its own measurements, paid for that choice honestly.

---

## References

*(Indicative; works the design and this document draw upon.)*

1. S. Graf. *Versioned Index Structures for Tree-Structured Data.* PhD dissertation (the sliding-snapshot lineage; cited in `docs/ARCHITECTURE.md`).
2. C. Okasaki. *Purely Functional Data Structures.* Cambridge University Press, 1998.
3. R. Binna, E. Zangerle, M. Pichl, G. Specht, V. Leis. *HOT: A Height Optimized Trie Index for Main-Memory Database Systems.* SIGMOD 2018.
4. V. Leis, A. Kemper, T. Neumann. *The Adaptive Radix Tree (ART).* ICDE 2013.
5. M. Haubenschild, C. Sauer, T. Neumann, V. Leis. *Rethinking Logging, Checkpoints, and Recovery for High-Performance Storage Engines.* SIGMOD 2020.
6. G. Haas, V. Leis. *What Modern NVMe Storage Can Do, and How to Exploit It.* VLDB 2023.
7. A. Ailamaki, D. DeWitt, M. Hill, M. Skounakis. *Weaving Relations for Cache Performance (PAX).* VLDB 2001.
8. M. Stonebraker et al. *C-Store: A Column-oriented DBMS.* VLDB 2005.
9. D. Hitz, J. Lau, M. Malcolm. *File System Design for an NFS File Server Appliance (WAFL).* USENIX 1994.
10. ISO/IEC 9075:2011 (SQL:2011), system-versioned temporal tables.
11. Project documents: `docs/DISK_FORMAT.md`, `docs/COMPARISON_POSTGRES.md`, `docs/BENCHMARKS.md`, `docs/HOT_*.md`, `docs/DEWEYID_HOT_INDEX_FORMAL_PROOF.md`, `docs/cost-based-optimizer-design.md`, `cascades-optimizer-architecture.md`, `docs/NAME_DICTIONARY_RECONSTRUCTION_PLAN.md`.

---

## Appendix A: Map of Key Classes

| Concern | Entry points |
|---|---|
| CoW trie | `page/UberPage`, `RevisionRootPage`, `IndirectPage`, `KeyValueLeafPage` |
| Versioning | `settings/VersioningType` (`SLIDING_SNAPSHOT.combineRecordPagesForModification`); TIL `PageContainer` |
| Identity / hashing | `node/SirixDeweyID`, `access/trx/node/HashType`, `node/json/*` |
| Storage / commit | `io/Superblock`, `io/IOStorage`, `io/filechannel/FileChannelWriter` & `FileChannelReader`, `io/AbstractReader`, `io/InterruptedFirstCommitRecovery`, `crash/CrashRecoveryInjectionTest` |
| HOT | `page/HOTLeafPage`, `HOTIndirectPage`; `access/trx/page/HOTTrieReader`/`HOTTrieWriter`/`HOTRangeCursor`; `docs/HOT_*.md` |
| Path summary / indexes | `index/path/summary/{PathNode,PathSummaryReader,PathSummaryWriter,PathStats}`; `index/IndexDef`; `page/{CASPage,PathPage,NamePage,DeweyIDPage}`; `index/{path,cas,name}/*Listener` |
| Query / optimiser | `brackit/*`; `query/json/{JsonDBStore,JsonDBCollection}`; `query/compiler/optimizer/walker/json/AbstractJsonPathWalker`; `docs/cost-based-optimizer-design.md` |
| Vectorisation | `page/pax/NumberRegionSimd`; `index/projection/{ProjectionIndexLeafPage,ProjectionIndexByteScan}`; `query/scan/SirixVectorizedExecutor` |
| Enterprise I/O | `sirix-enterprise-core/.../iouring/{FFIIOUring,FFMIOUringWriter,FFMIOUringReader,IOUringRingPool}`; `sirix-enterprise-{s3,kafka}` |
| Web GUI | `src/lib/api.ts`, `src/lib/sirixTree.ts`, `src/hooks/useSirix.ts`, `src/pages/{Explorer,Query,History,Diff,Sunburst}.tsx`, `src/stores/authStore.ts` |

## Appendix B: Glossary

**Beacon** — one of two redundant on-disk copies of the uber page (offsets 4096/8192), each `[len][payload][XXH3]`, in separate filesystem blocks. **CoW** — copy-on-write. **DeweyID** — order-preserving hierarchical node identifier. **Fragment** — a partial on-disk version of a logical page; combined at read time. **HOT** — Height-Optimised Trie. **PAX** — Partition Attributes Across; an in-page columnar layout. **Path class** — an equivalence class of document nodes sharing a path; a node in the path summary. **Revision** — an immutable committed version of a resource. **Sliding snapshot** — the default page-versioning strategy: bounded-window reconstruction with out-of-window preservation. **TIL** — Transaction Intent Log, the in-memory uncommitted page set. **Uber page** — the per-resource root descriptor pointing at the current revision root.
</content>
