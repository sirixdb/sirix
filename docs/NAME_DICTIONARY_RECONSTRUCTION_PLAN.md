# Name-Dictionary Reconstruction — Design & Verification Plan

Status: DRAFT for review. The **sizing** half of the leak is already fixed and validated
(`Names` constructor no longer pre-sizes maps by `maxNodeKey`); this plan covers the **residual
O(maxNodeKey) reconstruction scan** and the **root cause (monotonic high-water growth under name
churn)**. Versioning correctness is the hard constraint and the focus of the formal-verification
section.

---

## 1. Problem statement

`Names.fromStorage(reader, indexNumber, maxNodeKey)` rebuilds an in-memory name dictionary by:

```
for (long i = 1; i < maxNodeKey; i += 2) {
  node = reader.getRecord(i, IndexType.NAME, indexNumber);   // 2 reads per slot
  if (live HashEntryNode) { populate maps; read its count node at i+1; }
}
```

`maxNodeKey` is `NamePage.maxNodeKeys[indexNumber]` — a **monotonic high-water mark** of node keys
ever allocated in that dictionary. It is **only incremented, never decremented**. Under name churn
(`setName` of a previously-removed name does `maxNodeKey += 2`), the high-water grows without bound
even though the number of **live** names is constant.

Consequences:
1. **(FIXED) Memory:** the constructor pre-sized three fastutil maps to `maxNodeKey/0.75`, so each
   `Names` ballooned to O(high-water) (~17 MB at ~400k high-water) while holding ~1024 live names.
   With the bounded names cache retaining hundreds of instances, this OOM'd the heap. Fixed by
   sizing maps to the live entry count (default-grown).
2. **(OPEN) Reconstruction cost:** every cache-miss reconstruction scans `1..maxNodeKey`, issuing
   ~`maxNodeKey` `getRecord` calls, the vast majority hitting **dead/tombstoned slots**. This is
   O(historical-high-water), not O(live-names) — not HFT-grade and degrades monotonically with
   churn. (Currently latent: the names cache absorbs most reconstructions; at 520k/260-rev soak it
   still completed in ~22s. It will bite at larger scale / colder caches.)

Empirical provenance: async-profiler `alloc,live` ranked `io.sirix.index.name.Names.<init>` the #1
retained-array allocation site; the CAS workload (1 distinct name) never reproduces it while the
name workload (~1024 names churned every revision) OOMs at rev 196.

---

## 2. Current model (confirmed facts)

- A `Names` instance holds three primitive maps keyed by an **int name-key**:
  - `nameMap`        : key → UTF-8 bytes of the name
  - `countNameMapping`: key → live occurrence count
  - `countNodeMap`   : key → node-key of the paired `HashCountEntryNode`
- Persisted form, per dictionary, in the versioned node store `IndexType.NAME` / `indexNumber`:
  - `HashEntryNode(nodeKey, key, name)` at an even-ish slot,
  - `HashCountEntryNode(nodeKey+1, count)` immediately after.
- Five dictionaries by `indexNumber`/offset: XML `{attributes=0, elements=1, namespaces=2, PI=3}`,
  JSON `{jsonObjectKeys=0}`. XML and JSON resources are disjoint, so offset 0 never means two things
  within one resource — **but any code keyed by offset alone must also key by resource type.**
- `NamePage.maxNodeKeys` (`Int2LongMap`, offset→high-water) lives in the `NamePage`, is **CoW-copied
  per revision**, and is **only incremented** (`createRecord`/`incrementMaxNodeKey`); `removeName`
  deletes the two records but does **not** lower the high-water.
- `setName(name)`:
  - name already present (by key, with byte-equality guard) → increment its count node; no key alloc.
  - else → `maxNodeKey += 2`; create entry+count nodes; on **hash collision** (`key=name.hashCode()`
    already used by a different name) resolve via `getNewKey` (linear probe to a free int key).
- `removeName(key)`: decrement count; at 0 → `removeRecord` both nodes (tombstoned in the version
  chain). High-water unchanged.
- `fromStorage`: the reconstruction scan above, executed at the **reader's revision** — `getRecord`
  returns the slot's state as of that revision via sliding-snapshot fragment reconstruction.
- `NamesCache` (max 500) memoizes `Names` by `NamesCacheKey`; misses run `fromStorage`.

---

## 3. Invariants (must hold for every revision R, before and after any change)

- **I-N1 Bijection.** Within R, `key ↦ name` and `name ↦ key` are inverse on the live set; collided
  hashes get distinct keys via `getNewKey`. The key stored in `HashEntryNode` is authoritative —
  reconstruction must use the **stored** key, never re-derive it (re-derivation is order-dependent).
- **I-N2 Count consistency.** `countNameMapping[key]` == `HashCountEntryNode.value` for the paired
  node == number of document nodes in R that reference `key`.
- **I-N3 Pairing.** Every live `HashEntryNode(n)` has exactly one live `HashCountEntryNode(n+1)`, and
  `countNodeMap[key] == n+1`.
- **I-N4 Monotonic, non-reused keys.** Node keys allocated to name nodes are strictly increasing over
  the resource lifetime and never reused. (Hard versioning requirement: a historical revision may
  still reference a node key; reusing it would alias two logical entries across revisions.)
- **I-N5 Historical fidelity.** Reconstructing at R yields exactly the names live at R — neither names
  removed at ≤R nor names added at >R. Independent of which later revisions exist.
- **I-N6 Reader isolation.** Concurrent readers at different revisions reconstruct independently with
  no shared mutable dictionary state; a writer commit must not mutate a structure a reader is reading.
- **I-N7 Round-trip.** `deserialize(serialize(D)) == D` as a set of `(key,name,count)` triples, for
  every dictionary kind, XML and JSON.

Any new fast path must be **observably equivalent** to the current scan on I-N1..I-N7 — this is the
differential oracle in §6.

---

## 4. Corner cases the design must address

1. **Hash collisions** — two names, same `hashCode()`, distinct probed keys. Reconstruction must read
   the stored key; a compact snapshot must store the resolved key, not the hash.
2. **Re-add after delete (the churn case)** — same name string, NEW node key, possibly the SAME int
   key (if no live collision) — produces a higher high-water. Must not double-count or alias.
3. **Tombstoned slots** — `DELETE`-kind nodes in the scan range must be skipped; a compact form must
   not list them.
4. **Empty dictionary** — `maxNodeKey == 0`: no entries (common for unused kinds, e.g. namespaces in
   JSON).
5. **Count-node pairing across versions** — entry live but count node at a different fragment; sliding
   snapshot must reconstruct both at R.
6. **High-water persistence** — `maxNodeKeys` CoW per revision; a compaction approach changes it and
   must keep historical NamePages’ high-waters intact.
7. **NamesCache coherence** — cache key must include (databaseId, resourceId, revision, offset); a
   stale entry must never leak across a structural change/compaction.
8. **Five kinds × {XML,JSON}** — every kind exercised, including the offset-0 overlap between XML
   attributes and JSON object keys (disjoint per resource, but test both).
9. **Concurrent readers + writer commit** (I-N6) — the soak’s reader/writer race must stay clean.
10. **Multi-resource / global buffer manager** — `NamesCache` is global; keys must not collide across
    resources.

---

## 5. Candidate approaches (with versioning analysis)

### A — Iterate only live entries (no dead-slot probing)
Replace the `1..maxNodeKey` probe with iteration over the dictionary’s **live** name nodes.
- *Needs:* a live-record iterator over `IndexType.NAME`/`indexNumber` at revision R. The NAME store
  is itself a versioned page-trie keyed by node-key; a revision-correct forward cursor over live
  leaves gives O(live + tombstones-in-touched-pages) instead of O(high-water).
- *Versioning:* safe — purely a read-path change; no key reuse, no format change. Historical fidelity
  follows from the cursor honoring R (same guarantee `getRecord` already gives per slot).
- *Risk:* tombstones still occupy leaf pages until version GC; iteration cost is bounded by live +
  resident tombstones, not by the all-time high-water. Need to confirm the NAME store exposes (or can
  cheaply expose) a revision-scoped live-leaf cursor.

### B — Compact per-revision snapshot
Serialize the live dictionary as one compact blob referenced by the `NamePage`; load in O(live).
- *Versioning:* the blob is CoW per revision like any page; historical reads load the blob of their
  revision. Must keep the node-form (for transactional `removeName`/count updates) **or** move fully
  to the blob (bigger change; affects mutation path + recovery).
- *Risk:* dual source of truth (nodes + blob) unless we fully migrate; format/version bump + migration
  for existing databases.

### C — Compaction of the dictionary
Periodically rewrite a dictionary’s live entries into fresh **contiguous** node keys and reset the
high-water for **future** revisions.
- *Versioning:* safe **only** as a new version — old revisions keep their own NamePage + high-water +
  node keys; the compacted revision starts a fresh contiguous range. Never rewrite in place. (This is
  the COW-correct analogue of slot reuse and the only safe way to bound the high-water.)
- *Risk:* most complex; triggering policy (when to compact), interaction with sliding-snapshot
  fragment chains, and a correctness proof that no live key changes identity within a revision.

### D — Incremental in-memory dictionary (avoid reconstruction)
Keep the writer’s live `Names` resident and update it incrementally on set/remove; readers still need
a revision-correct view, so this complements but does not replace A/B for historical reads.

### Rejected: naive slot reuse / high-water decrement
Directly violates **I-N4/I-N5** — a removed name’s node key could be reused by a different name in a
later revision, aliasing the two for any reader of an intermediate revision. This is the core
versioning trap and is **out of scope**.

**Tentative recommendation:** **A** as the primary fix (smallest, read-path-only, no format change),
with **C** considered later only if high-water growth itself (not just the scan) proves to be a
problem (e.g. serialized `maxNodeKeys` size, or live-leaf cursor cost dominated by resident
tombstones). Decision pending the open questions in §8.

---

## 6. Formal verification & test plan

1. **`NameDictionaryInvariantValidator`** (mirrors `HOTInvariantValidator`): given a reconstructed
   `Names` + the NAME store at revision R, assert I-N1..I-N7. Reusable across XML/JSON and all kinds.
2. **Differential oracle:** for every revision in a randomized churn run, assert
   `reconstructNew(R) ≡ scanReconstruct(R)` as `(key,name,count)` sets, across all dictionary kinds.
   This pins the new path to the proven-correct (if slow) scan.
3. **Property/fuzz:** randomized insert/remove/re-add/collision sequences (seeded, multi-seed) with
   per-revision validation past the chunk boundary; include forced hashCode collisions and the
   delete→re-add churn pattern.
4. **Historical-fidelity test:** after N revisions, open read txns at a sample of past revisions and
   assert each sees exactly its live name set (I-N5), independent of later revisions.
5. **Concurrency:** extend the existing soak (reader/writer race) to assert reader name-resolution
   correctness, name index + name dictionary, multi-seed.
6. **Scale gate:** the heavy soak — confirm reconstruction cost is now O(live) (instrument
   reconstruction work / `getRecord` count per `fromStorage`), and that the name soak holds flat
   memory across revisions (no growth with high-water).
7. **Argument (proof sketch):** A preserves I-N5 because the live-leaf cursor at R returns exactly the
   `getRecord(i,R)`-live entries the scan would, minus the dead slots the scan discards — i.e. the
   same set by construction; I-N4 is untouched (no allocation/reuse on the read path).

---

## 7. Rollout

1. Land the **sizing fix** (done, validated: full sirix-core green; 520k name soak completes, was
   OOM@196). Standalone, low-risk.
2. Build the validator + differential oracle (§6.1–6.2) **first** — they gate everything.
3. Implement Approach A behind the validator; differential-test, fuzz, soak.
4. Re-run the heavy multi-seed/520k matrix (CAS + name) to confirm flat memory + O(live) reconstruct.
5. Reconsider C only if §8 answers indicate high-water growth itself is a problem.

---

## 8. Open questions (need decisions before implementing)

- **Q1 (versioning core):** Is a revision-scoped **live-leaf cursor** over `IndexType.NAME` available
  (or cheaply addable) so Approach A can iterate live entries without probing dead slots? What does it
  cost when many tombstones are resident in the version window?
- **Q2:** Is the unbounded growth of `maxNodeKeys` itself a concern (serialized NamePage size; very
  long-lived resources), or is bounding only the **reconstruction cost** (Approach A) sufficient?
- **Q3:** Acceptable to keep the node-form as the source of truth (A) vs. migrating to a compact blob
  (B, format bump + migration)?
- **Q4:** Compaction (C) — desired now, or deferred? If desired, what triggers it and how do we prove
  no live key changes identity within a revision?
- **Q5:** Should the names cache be re-keyed/parameterized (size, key tuple) as part of this, given it
  was the multiplier that turned the per-instance bloat into an OOM?
