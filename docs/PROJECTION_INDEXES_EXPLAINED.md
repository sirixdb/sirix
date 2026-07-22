# Projection Indexes, Explained From Scratch

*A gentle, build-up-from-nothing guide to what SirixDB's projection indexes
are, how reading and writing them works, and how they interact with SirixDB's
page-versioning algorithms.*

This document assumes **no prior knowledge** — not of SirixDB, not of
database internals, not of versioning. Every term is defined the first time it
appears. If you already know columnar storage and copy-on-write, you can skip
ahead using the map below; if you don't, read straight through.

There are two sibling documents that go far deeper and assume you already know
the fundamentals this one teaches:

- `docs/PROJECTION_INDEX_DEEP_DIVE.md` — the byte-level, contributor-facing
  tour of the projection index.
- `docs/PROJECTION_INDEX_STORAGE_REDESIGN.md` — the design spec of record.

Think of *this* document as the on-ramp to those two.

## Reading map

| Part | What it covers | Skip if… |
|---|---|---|
| 1 | The absolute basics: databases, rows vs. columns, indexes | you know what a columnar index is |
| 2 | What SirixDB itself is: versioned JSON, revisions, node keys | you've used SirixDB |
| 3 | What a projection index *is*, in plain English | — |
| 4 | How the data is physically laid out (columns, leaves, segments) | — |
| 5 | Versioning fundamentals: copy-on-write and the page tree | you know CoW page stores |
| 6 | **The four page-versioning algorithms**, worked out step by step | — |
| 7 | **Writing** a projection index — under each versioning strategy | — |
| 8 | **Reading** a projection index — including reading the past | — |
| 9 | One full worked example tying everything together | — |
| 10 | Glossary | — |

---

## Part 1 — The absolute basics

### 1.1 What is a database, and what is a query?

A **database** is a program that stores data durably (it survives restarts)
and lets you ask questions about that data. A question you ask is a **query**.

Two very different *kinds* of query matter here:

1. **"Give me one record."** *"Show me employee #4017."* You want every field
   of one thing.
2. **"Summarize many records."** *"What is the average age of all active
   employees?"* You want one field (age) from a million things, and you want a
   single number back.

The second kind is called **analytics** or **OLAP** (Online Analytical
Processing). Projection indexes exist to make the second kind *fast*. To
understand why they help, you first need to understand how data is laid out on
disk.

### 1.2 Rows vs. columns — the single most important idea

Imagine a spreadsheet of employees:

| id | age | active | dept  |
|---:|----:|:------:|-------|
| 0  | 30  | true   | Eng   |
| 1  | 45  | false  | Sales |
| 2  | 52  | true   | Eng   |
| 3  | 23  | true   | HR    |
| 4  | 61  | false  | Eng   |

There are two ways to store this on disk.

**Row-major** ("row storage") keeps each record's fields together:

```
[0, 30, true, Eng] [1, 45, false, Sales] [2, 52, true, Eng] ...
```

This is great for "give me record 2" — everything about record 2 sits in one
place, one read.

**Column-major** ("columnar storage") keeps all values of one field together:

```
ids:    [0, 1, 2, 3, 4]
ages:   [30, 45, 52, 23, 61]
active: [true, false, true, true, false]
depts:  [Eng, Sales, Eng, HR, Eng]
```

Now "average age" reads *only* the `ages` block — a tight, contiguous run of
numbers — and touches none of the other fields. Over a million records, that
is the difference between reading 8 MB and reading 200 MB.

Columnar storage has a second gift: values of the same field have the same
shape, so they **compress** extremely well. Three tricks recur throughout this
document:

- **Dictionary encoding.** Instead of storing the strings
  `Eng, Sales, Eng, HR, Eng`, store each *distinct* string once in a
  **dictionary** — `[Eng, Sales, HR]` — and replace each value with a small
  integer id pointing into it: `[0, 1, 0, 2, 0]`. Now a string comparison
  becomes an integer comparison, and "group by department" becomes "count how
  many of each id."
- **Frame of reference (FOR) + bit-packing.** If a block of ages ranges from
  23 to 61, store the minimum (23) once, then store each value as its *offset*
  from the minimum: `[7, 22, 29, 0, 38]`. The biggest offset is 38, which fits
  in 6 bits instead of a 64-bit number. Same information, a tenth of the
  space, and trivially reversible.
- **Zone maps.** For each block, remember the smallest and largest value it
  contains. If a query asks for `age > 70` and a block's stored maximum is 61,
  you can skip the *entire block* without reading a single value. This "decide
  from tiny metadata that big data can't match" trick is called **pruning**.

### 1.3 What is an index?

An **index** is an auxiliary data structure that answers a specific kind of
question faster than scanning everything. A phone book is an index: it trades
some extra pages (the alphabetical listing) for the ability to find "Smith"
without reading every entry.

A **projection index** is a *columnar* index. The word "projection" comes from
databases: to **project** means "pick out these particular columns." So a
projection index is a saved, always-up-to-date, columnar copy of *just the
fields you care about* from *just the records you care about* — laid out for
analytics speed, with all the compression tricks above baked in.

That's the whole idea. Everything below is *how SirixDB does this while every
past version of the data stays queryable.*

---

## Part 2 — What SirixDB is (just enough)

SirixDB is a **versioned JSON document store**. Three properties matter here.

### 2.1 It stores JSON

Your data is JSON — objects, arrays, numbers, strings, booleans. Our running
example is an array of five objects:

```json
[
  {"age": 30, "active": true,  "dept": "Eng"},
  {"age": 45, "active": false, "dept": "Sales"},
  {"age": 52, "active": true,  "dept": "Eng"},
  {"age": 23, "active": true,  "dept": "HR"},
  {"age": 61, "active": false, "dept": "Eng"}
]
```

Internally SirixDB shreds this into a **tree of nodes** — one node for the
array, one per object, one per field, one per value. Every node gets a
permanent 64-bit id called its **node key**, handed out in increasing order and
*never reused or changed*. When we talk about a **record** in a projection
index, we mean "one of the objects under the array," and its **record key** is
that object node's node key. The fact that node keys only ever increase is a
load-bearing invariant we'll rely on later.

### 2.2 It is versioned — every change makes a new revision

This is the defining feature. Every time you commit a change, SirixDB produces
a new **revision** — a complete, immutable, numbered snapshot of the whole
resource. Revision 1, revision 2, revision 3, … Nothing is ever overwritten;
the old revisions stay fully queryable forever. You can ask *"what was the
average age as of revision 42?"* years later and get the exact answer.

This is called **time travel**, and it is *free* in the sense that SirixDB
doesn't keep N copies of everything — it shares the unchanged parts between
revisions. Part 5 explains exactly how.

### 2.3 It never modifies anything in place — copy-on-write

The mechanism behind versioning is **copy-on-write (CoW)**: to change
something, SirixDB writes a *new* copy of the changed part and leaves the old
one untouched. We'll unpack this properly in Part 5, because it's the key to
understanding both time travel *and* how projection-index writes work.

---

## Part 3 — What a projection index is, concretely

Now we can state it precisely.

> A **projection index** is a persistent, always-maintained, columnar copy of
> selected fields from a set of JSON records, that lives *inside* SirixDB's
> versioned storage — so every revision of it is queryable, and it updates
> itself on every commit.

You create one by naming:

- a **root path** — which records (e.g. `/[]`, "every object directly inside
  the top-level array"), and
- a list of **field paths** with declared **types** — which columns (e.g.
  `/[]/age` as `long`, `/[]/active` as `boolean`, `/[]/dept` as `string`).

In SirixDB's query language that looks like:

```xquery
let $doc := jn:doc('mydb','sales.jn')
return jn:create-projection-index($doc, '/[]',
    ('/[]/age', '/[]/active', '/[]/dept'),
    ('long', 'boolean', 'string'))
```

From that moment, analytical queries over those fields are answered from the
compact columnar copy instead of by walking the JSON tree:

```xquery
sum(for $r in $doc[] return $r.age)                          (: → 211 :)
count(for $r in $doc[] where $r.age > 40 and $r.active return $r)
for $d in distinct-values(for $r in $doc[] return $r.dept) ...
```

Three properties make it different from, say, a Parquet file or a DuckDB table
(two well-known columnar formats):

1. **It is versioned.** It lives in the copy-on-write page tree, so every
   commit yields a new queryable snapshot and unchanged parts are shared. Time-
   travel analytics ("group-by as of revision 42") run on the *same* code.
2. **It is always maintained.** A **change listener** patches it at every
   commit — no manual "refresh the index" step. Updates rebuild the affected
   piece, appends extend the tail, deletes drop rows.
3. **It is fail-closed.** Before answering any query from the index, SirixDB
   *proves* it can answer exactly. If it can't (wrong types, missing fields, a
   staleness concern), it silently falls back to the slower but always-correct
   tree walk. Speed never outruns correctness.

Supported column types today: `long`, `boolean`, `string`, and
`double`/`float`/`decimal` (the last three all handled by one "double" column
kind).

---

## Part 4 — How the data is physically laid out

This part builds the mental picture you need before we can talk about reads,
writes, and versioning. We go from the top (the whole index) down to the bytes.

### 4.1 The leaf: 1024 rows at a time

The projection doesn't store all records as one giant column. It chops them
into **logical leaves** of up to **1024 consecutive records** each, in
node-key order. Leaf 0 holds records 0–1023, leaf 1 holds 1024–2047, and so
on. Our tiny 5-record example fits in a single leaf.

Why 1024? Three reasons that will recur:

- It's the unit the query engine's vectorized ("SIMD") routines process — they
  work on 1024-bit masks at a time.
- It's the unit of **maintenance**: when records in a leaf change, SirixDB
  re-extracts *that whole leaf* from the document and re-encodes it. A small
  leaf keeps that cheap.
- It's small enough that copy-on-write sharing (Part 5) stays fine-grained.

Each leaf holds, per column:

| Column kind | How it's stored inside the leaf |
|---|---|
| `NUMERIC_LONG` | 64-bit values + a zone map (min/max) |
| `BOOLEAN` | a bitmap — one bit per row |
| `STRING_DICT` | a small per-leaf dictionary + one integer id per row |
| `NUMERIC_DOUBLE` | an order-preserving transform of the double (see the deep dive) |

Plus two more per-column things that JSON makes necessary:

- A **presence bitmap** — one bit per row saying "is this field even present on
  this record?" JSON is sparse; a record can simply lack `dept`.
- **Provenance flags** — sticky warning bits recording anything that would make
  a fast answer *inexact*. Example: if a value that was supposed to go in a
  `long` column wasn't actually a whole number, the `NON_INTEGRAL` flag is set,
  and integer-exact queries on that column refuse the fast path. These flags
  are how "fail-closed" is enforced at the data level.

### 4.2 Segments: the leaf, split by column

When a leaf is written to disk, it is **not** written as one blob. It is split
along **column boundaries** into pieces called **segments**:

- `KEYS` — the record keys of the rows in this leaf.
- `BODY(c)` — one segment per column *c*: its flags, zone map, presence bitmap,
  and encoded values.
- `DICT(c)` — for string columns only: that column's dictionary.

So our 3-column example leaf becomes **five** segments: `KEYS`, `BODY(age)`,
`BODY(active)`, `BODY(dept)`, and `DICT(dept)`.

Why split this way? Because it makes *both* reads and writes surgical:

- **Reads become columnar.** A query filtering on `age` fetches only
  `BODY(age)`. It never touches the department dictionary.
- **Writes become contained.** Change one age? You re-encode only `BODY(age)`.
  Every other segment is untouched — and, as we'll see, literally shared with
  the previous revision.

Each segment is stored in its own little page on disk, called a
**`ProjectionSegmentPage`**.

### 4.3 The descriptor: a tiny table of contents

For each leaf, SirixDB stores a small (~100–200 byte) summary called a
**descriptor** (`PIXD`). It contains:

- the row count, column count, and column kinds,
- the first and last record key in the leaf (its **fences**),
- and, for each segment: its id, its exact length, a **hash** of its bytes, and
  a mirror of its min/max and flags.

The descriptor is the leaf's table of contents. It's deliberately tiny so that
many questions can be answered *without reading any segment bytes at all*:

- `count(*)`? The descriptor's row count answers it. Zero segment reads.
- `max(age) > 70`? The descriptor's mirrored zone map answers "no" and skips
  the whole leaf.

The segment **hash** in the descriptor does two crucial jobs we'll use in Part
7 and Part 8:

- **On write:** to decide whether a re-encoded segment actually changed, SirixDB
  compares its new hash against the stored one. Equal hash → the bytes are
  identical → *don't write anything, just reuse the old page.*
- **On read:** the hash is the integrity check. When a segment is loaded, its
  bytes are re-hashed and compared; a mismatch means corruption, and the query
  falls back to the safe path.

### 4.4 Where it all lives in the storage tree

Putting the pieces together, one projection index definition is a little
subtree inside SirixDB's page tree:

```
RevisionRootPage (one per revision)
  └─ ProjectionIndexPage
       └─ (one HOT trie per index definition)
            └─ HOTLeafPage(s)
                 ├─ slot 0  → metadata (root path, column shapes, leaf count, …)
                 ├─ slot 1  → descriptor for leaf 0  ──┐
                 ├─ slot 2  → descriptor for leaf 1    │  descriptors are tiny
                 └─ …                                  │  and live in the trie
                                                       │
                 side map: (slot, segmentId) → PageReference
                                                       │
                                                       ▼
                              ProjectionSegmentPage(s)  ← the actual column bytes
```

A **HOT trie** is just an ordered map from a key to a value — here, from
"leaf number" to "that leaf's descriptor." (HOT stands for Height Optimized
Trie; for this document you can treat it as "a sorted key→value store that
lives in pages.") The descriptors live *in* the trie's leaf pages; the bulky
column bytes live *outside*, in dedicated segment pages, connected by a small
**side map** of references. Slot 0 is special — it holds the projection's
metadata (its shape and bounds), not a leaf.

Hold onto this shape. The two kinds of pages behave **very differently** under
versioning, and that's the crux of Part 7:

- **HOT leaf pages** (holding descriptors) are versioned the normal SirixDB
  way, with fragment chains — the subject of Part 6.
- **Segment pages** (holding column bytes) are versioned like SirixDB's
  oversized-value "overflow" pages: each one is immutable, identified by its
  physical file offset, and shared across revisions simply by pointing at it.

---

## Part 5 — Versioning fundamentals: copy-on-write and pages

To understand projection writes and reads across versions, you need SirixDB's
storage model. It rests on one rule.

> **The rule: once a page is written to disk, it is never modified. Ever.**

### 5.1 Pages and the page tree

SirixDB stores everything in fixed-purpose **pages**. Data records (and index
descriptors) live in **leaf pages**; above them sit **indirect pages** that
point to other pages; at the very top is a **RevisionRootPage** for each
revision, and above *all* revisions sits a single **UberPage** that points at
the newest revision root. To find a record you start at the root and follow
pointers down to the leaf — a tree walk.

A **page reference** is a pointer to a page. Crucially, a reference can point
to a page *on disk* (by file offset) or to a page *in memory* (not yet
written).

### 5.2 What copy-on-write actually does

Suppose you change one record. Because pages are immutable, SirixDB cannot edit
the leaf page that holds it. Instead it:

1. Copies that leaf page, applies the change to the copy.
2. The copy sits at a new location, so its parent must now point at the copy —
   but the parent is immutable too, so the parent is copied and re-pointed.
3. …and so on up to the root. A brand-new RevisionRootPage is written.
4. Finally, the UberPage's pointer is swapped to the new root. That single
   pointer swap is the **commit** — atomic, all-or-nothing.

Everything *not* on the path from root to the changed leaf is **shared**: the
new revision's tree points at the *same old pages* for all the unchanged
branches. This is **structural sharing**, and it's why keeping 10,000
revisions doesn't cost 10,000 full copies.

```
Revision N                         Revision N+1
   Root                               Root'        ← new
  /    \                             /    \
 A      B                           A      B'      ← B changed, so B' is new
/ \    / \                         / \    / \
C   D  E  F                        C   D  E  F'     ← F changed → F' new
                                   ↑   ↑  ↑
                              all shared with revision N
```

The pages held in memory during a write transaction, before commit, live in
the **Transaction Intent Log (TIL)** — a staging area. On commit they're
written and the pointer is swapped; on rollback they're simply dropped, and
since nothing was written yet, there's nothing to undo. (This is why SirixDB
needs no separate write-ahead log.)

### 5.3 The catch that motivates everything in Part 6

Copy-on-write as described so far has a cost: if you copy the *whole* leaf page
every time one record changes, and a leaf holds 1024 records, you rewrite 1024
records to change one. Over many revisions that's enormous **write
amplification** and storage bloat.

The fix is to *not* always write the whole page. Instead, write only the
**changed records** as a small **fragment**, and reconstruct the full page at
read time by combining the recent fragments. *How many* fragments you keep, and
*when* you consolidate them, is exactly what a **versioning algorithm** decides
— and SirixDB gives you four to choose from.

---

## Part 6 — The four page-versioning algorithms

A versioning algorithm answers: *"When I write a page, do I write the whole
thing or just the changes? And when I read it back, how many pieces must I
combine?"* It's a trade between **write cost**, **storage**, and **read cost**.

Two functions in the code (`VersioningType.java`) implement each algorithm:

- `combineRecordPages(...)` — **read** side: given the fragments on disk,
  reconstruct the full page. Fragments are merged **newest-wins**: if slot 5
  appears in two fragments, the newer one's value is kept.
- `combineRecordPagesForModification(...)` — **write** side: prepare the next
  fragment to write.

Throughout, `revisionsToRestore` (call it **w**, the "window") is the tuning
knob. We'll use a small page of six slots `[A B C D E F]` for every example.

### 6.1 FULL — always write the whole page

Every revision writes a complete, self-contained page.

```
Rev 1: [A  B  C  D  E  F]      ← complete
Rev 2: [A  B' C  D  E  F]      ← complete (B changed, but whole page rewritten)
Rev 3: [A  B' C' D  E  F]      ← complete
```

- **Read:** load exactly one page. O(1). The fastest possible read.
- **Write:** rewrite the entire page every time. Most expensive write.
- **Storage:** highest — no sharing of unchanged records within a page.
- **Best for:** read-heavy data that rarely changes.

There are no fragments to combine, so `combineRecordPages` just returns the one
page.

### 6.2 INCREMENTAL — write only changes, snapshot periodically

Write only the changed slots as a delta. Every **w** revisions, write a fresh
full snapshot to stop the chain from growing without bound.

```
Rev 1: [A B C D E F]   ← FULL (chain anchor)
Rev 2: [_ B' _ _ _ _]  ← delta: slot 1
Rev 3: [_ _ C' _ _ _]  ← delta: slot 2
Rev 4: [A' _ _ _ _ _]  ← delta: slot 0
Rev 5: [A' B' C' D E F] ← FULL again (write spike!)
```

- **Read Rev 4:** combine Rev4 + Rev3 + Rev2 + Rev1(full), newest-wins → up to
  **w** fragments. Bounded read.
- **Write:** cheap most of the time (one delta), but every **w**-th revision
  pays a **full-page write spike**.
- **Storage:** lowest of all four (small deltas + occasional full).
- **Downside:** the periodic spike. For a 1024-record page changing one record,
  the snapshot rewrites all 1024 to reset the chain — 1000× amplification, in
  bursts.

### 6.3 DIFFERENTIAL — each delta references the last full snapshot

Like incremental, but every delta records *all changes since the last full
snapshot*, not just since the previous revision.

```
Rev 1: [A B C D E F]        ← FULL
Rev 2: [_ B' _ _ _ _]       ← diff from Rev1: {1}
Rev 3: [_ B' C' _ _ _]      ← diff from Rev1: {1,2}  (cumulative!)
Rev 4: [A B' C' D E F]      ← FULL again
```

- **Read:** always exactly **two** fragments — the latest diff and the full
  snapshot it's based on. O(1) read, and simpler than incremental.
- **Write:** the diff *grows* over time (it re-records everything changed since
  the snapshot), until the next full snapshot resets it.
- **Storage:** medium — the growing diffs cost more than incremental's minimal
  deltas.
- **Best for:** read-heavy after an initial load, where a fixed 2-fragment read
  is worth larger deltas.

### 6.4 SLIDING_SNAPSHOT — the default, and the clever one

This is SirixDB's signature algorithm and its default (the default window
`revisionsToRestore` is **3**; the worked example below uses 4 purely because
it makes the "falling out of the window" step easier to see). It gets
INCREMENTAL's bounded reads **and** avoids the periodic full-write spike
entirely.

**The problem it solves.** INCREMENTAL must periodically rewrite the whole page
to keep reads bounded. That's the spike. SLIDING_SNAPSHOT asks: *instead of
rewriting everything every w revisions, what if — each revision — we preserved
only the records that are just about to fall out of the read window and would
otherwise be lost?*

**How it works.** Reads only ever combine the newest **w** fragments (the
"window"). So a record is only in danger when the fragment holding it is about
to slide out of that window. On each write, SirixDB:

1. Builds a bitmap of which slots are present in the fragments *still in the
   window*.
2. Looks at the fragment about to fall *out* of the window. Any slot it holds
   that is **not** already covered by an in-window fragment would become
   unreachable — so those, and only those, are **preserved** by copying them
   into the new fragment being written.

Each record is thus preserved *at most once per window*, exactly when needed —
never in a big periodic burst.

**Worked example (window w = 4), page `[A B C D E F]`:**

```
Rev 1: [A  B  C  D  E  F]   ← initial full page
Rev 2: [_  B' _  _  _  _]   ← slot 1 changed.   Chain: R2→R1
Rev 3: [_  _  C' _  _  _]   ← slot 2 changed.   Chain: R3→R2→R1
Rev 4: [A' _  _  _  _  _]   ← slot 0 changed.   Chain: R4→R3→R2→R1  (window full)

Rev 5: slot ??? changes, and Rev 1 now falls OUT of the 4-fragment window.
       Which of Rev 1's slots are still covered by R2,R3,R4?
         slot 0 → in R4 ✓     slot 1 → in R2 ✓     slot 2 → in R3 ✓
         slot 3 (D) → NOT covered  ← must preserve
         slot 4 (E) → NOT covered  ← must preserve
         slot 5 (F) → NOT covered  ← must preserve
       Rev 5 writes: [_ _ _ D E F]   (its own change, plus D,E,F preserved)
       Chain: R5→R4→R3→R2   (R1 dropped)

Read Rev 5: combine R5+R4+R3+R2 = [A' B' C' D E F].  Only 4 fragments. Correct.
```

Compare the write pattern for a mostly-unchanged page:

- **INCREMENTAL:** `6, 1, 1, 1, 6, 1, 1, 1, 6, …` — periodic full-page spikes.
- **SLIDING_SNAPSHOT:** `6, 1, 1, 1, 4, 1, 1, 1, …` — the "4" is just the few
  preserved records, and there is **never a full-page rewrite again** after the
  initial one. Amortized extra cost ≈ one page's worth spread across a window,
  smoothly.

### 6.5 Summary table

| Strategy | Fragments to read | Write cost | Storage | When to use |
|---|---|---|---|---|
| **FULL** | 1 | rewrite whole page every time | highest | read-heavy, rarely updated |
| **INCREMENTAL** | up to w | cheap, but periodic full spike | lowest | simple; write spikes tolerable |
| **DIFFERENTIAL** | exactly 2 | grows until next snapshot | medium | read-heavy after load |
| **SLIDING_SNAPSHOT** | up to w | smooth, no full spikes | medium | **best all-round; the default** |

Everything in Parts 7 and 8 applies to *all four* strategies — the projection
index is built on top of this page machinery and inherits whichever one the
resource is configured with. The interesting subtlety, next, is that a
projection index has **two** kinds of pages that ride versioning differently.

---

## Part 7 — Writing a projection index

Writing happens in two situations: **building** the index the first time, and
**maintaining** it on every subsequent commit. Both funnel through the same
core idea — *re-encode a leaf, then write only the segments that actually
changed* — and both interact with the four versioning algorithms in a
two-layer way we'll pin down in 7.4.

### 7.1 The first build

`jn:create-projection-index(...)` walks the record set once, and for each group
of up to 1024 records:

1. Extracts the declared fields into a leaf (the columnar form of Part 4).
2. Encodes the leaf into its segments (`KEYS`, `BODY(c)`, `DICT(c)`).
3. Writes each segment as a `ProjectionSegmentPage`, records its descriptor in
   the trie slot, and remembers the leaf's fences.

It holds **one leaf in memory at a time** and streams — so building a
100-million-row projection doesn't need gigabytes of heap. The projection's
metadata (slot 0) is written **last**, so a crash mid-build leaves the old
metadata intact rather than a half-built index.

### 7.2 Incremental maintenance — the always-up-to-date part

Every commit that touches a resource with a projection triggers the
**change listener**. It performs a tidy piece of bookkeeping:

1. **Attribute** the commit's changed nodes to records under the projection's
   root. (If a change can't be cleanly attributed — e.g. a whole subtree was
   moved — it bails out to a full rebuild, see the "ladder" below.)
2. **Find the touched leaves.** It reads the per-leaf fences from slot 0 (one
   small read) and matches the changed record keys against them. Because node
   keys are always increasing, brand-new records always sort to the **tail** —
   appends are trivial to classify.
3. **Re-extract and re-encode** each touched leaf from the current document.
4. **Write only what changed** (next section).
5. **Update slot 0** with the new leaf count, build revision, and fences.

The **degradation ladder** guarantees a commit never fails because of the
index: if incremental maintenance can't cope (too many changed records, or an
inconsistency), it does a same-commit **full rebuild**; if even the rebuild
can't proceed (a pathological shape), it drops a **stale tombstone** over slot 0
— which just makes queries fall back to the tree walk until the index is next
recreated. Correctness is never at risk; only speed.

### 7.3 The heart of it: hash-based sharing ("carry-forward")

When a leaf is re-encoded, SirixDB does **not** blindly write all its segments.
For each segment it compares the new `(length, hash)` against the descriptor's
stored `(length, hash)`:

```
for each segment s of the re-encoded leaf:
    if s.length == prior.length(s) and s.hash == prior.hash(s):
        carry forward the OLD segment page   ← no write, no read, just reuse the reference
    else:
        write a NEW ProjectionSegmentPage for s
```

This is the payoff of splitting leaves into per-column segments. Change one
age:

| Change to a 1024-row leaf | Segments actually rewritten |
|---|---|
| in-place value update, column *c* | `BODY(c)` (+ `DICT(c)` only if the dictionary grew) + descriptor |
| append a row at the tail | `KEYS` + every `BODY` (+ any `DICT` that interned) + descriptor |
| delete a row | every segment of the leaf (row count changed) + descriptor |
| an **untouched** leaf | **nothing** — descriptor and all segments shared |

For the common case (edit one value in one column of one leaf), exactly **one**
column's `BODY` segment and the tiny descriptor are written. The other four
segments of that leaf, and *every segment of every other leaf*, are literally
the same pages as in the previous revision — shared by reference, for free.

Because the encoding is fully deterministic (dictionary ids are assigned in
first-seen order, extraction replays in a stable order), an unchanged column
*always* re-encodes to byte-identical output and therefore hashes equal. That's
why even a **full rebuild** shares every unchanged leaf's segments — only
genuinely different bytes ever hit the disk.

### 7.4 How this rides the two versioning layers

Here is the subtle, important part the deep-dive docs assume you already
understand. A projection index has **two** kinds of pages, and they interact
with the versioning algorithm of Part 6 differently.

**Layer 1 — the segment pages (the column bytes).** These do **not**
participate in fragment-based versioning at all. Each `ProjectionSegmentPage`
is like an "overflow" page for an oversized value:

- It is **immutable** and identified by its **physical file offset** — its
  address *is* where it sits in the file.
- There is **no fragment chain** and no delta reconstruction. A segment is
  whole; changing it means writing a brand-new segment page at a new offset and
  pointing the descriptor at it. This is **last-writer-wins** at the reference.
- It is shared across revisions the simplest possible way: the next revision's
  descriptor just **points at the same offset**. That's carry-forward (7.3).

So regardless of whether the resource is FULL, INCREMENTAL, DIFFERENTIAL, or
SLIDING_SNAPSHOT, the *column bytes* are always shared at whole-segment
granularity by reference. The versioning strategy does **not** delta-encode the
insides of a column.

**Layer 2 — the HOT leaf pages (the descriptors).** These *do* ride the normal
page-versioning machinery of Part 6, because a descriptor is an ordinary
key→value record in a leaf page. So:

- Under **FULL**, each revision's HOT leaf page is written whole.
- Under **INCREMENTAL / DIFFERENTIAL / SLIDING_SNAPSHOT**, only the descriptors
  that changed are written as a fragment, and reads combine the recent
  fragments newest-wins — exactly the algorithms of Part 6, now applied to
  "which leaf descriptors changed this commit."

There's one extra rule that makes this safe, worth stating plainly: when a
descriptor's segment reference changes, that fact must travel with the
fragment. So **every HOT fragment carries the complete side-map of references**,
and fragment merges are newest-fragment-authoritative. A reference update
therefore never requires dirtying a slot it didn't change, and merging
fragments can never accidentally resurrect a segment reference that was
dropped. (In the code this is the `FLAG_SEGMENT_REFS` machinery on
`HOTLeafPage` and the segment-ref carrying in `copy()` and the fragment merge.)

**Why split it this way?** Because column bytes and "which leaf changed" have
totally different change rates and shapes. Delta-encoding the *inside* of a
bit-packed column across revisions would be a nightmare (a one-value change can
shift every downstream byte). Storing whole immutable segments and sharing them
by reference sidesteps that entirely, while the *cheap, small* descriptors get
all the benefit of the sliding-snapshot machinery. The result: **a commit that
edits one value writes one small segment page + a descriptor delta** — and the
descriptor delta is subject to whichever Part 6 strategy you chose.

### 7.5 The commit ordering that makes it correct

One timing detail matters. A segment page's identity *is* its file offset,
which only exists once it's written. So during the copy-on-write commit descent,
SirixDB writes each new segment page **first** (assigning it an offset), and
only **then** serializes the HOT leaf page that references it — at which point
every reference is a resolved 8-byte offset. Segment pages are never written
before commit, so a rollback simply never wrote them. This mirrors exactly how
SirixDB already handles oversized "overflow" records.

---

## Part 8 — Reading a projection index

### 8.1 The cold-open path

The first time a query wants a projection at a given revision, SirixDB
**hydrates** it — loads and assembles it into an in-memory cache — keyed by
`(resource, definition, buildRevision)`:

1. **Read slot 0** (the metadata). Does its shape match the query's fields? Is
   it stale? How many leaves are there? If shape/staleness checks fail, fall
   back to the tree walk and remember not to retry (a "negative cache").
2. **Range-scan the descriptors** for leaves 0…N. This is where the versioning
   of Part 6 quietly happens: reading each HOT leaf page means combining its
   recent fragments (FULL = one; the others = up to *w*), newest-wins, to get
   the current descriptors. You, the query, never see this — you just get the
   descriptors.
3. **Fetch the segment pages** named by the descriptors (in parallel for large
   projections), verifying each segment's length and hash against the
   descriptor as it's loaded. Any mismatch → fall back.
4. **Assemble** the flat in-memory "raw scan form" the query routines consume.

Because hydration is cached per build revision, the *descriptors' ability to
answer questions without touching segments* (count, zone-map pruning) saves
cold-open I/O; once hydrated, every later query on that revision is served from
memory.

### 8.2 Answering the query

The vectorized routines ("kernels") then run over the raw form, 1024 rows at a
time. For `count where age > 40 and active`:

1. **Zone-map skip.** If a leaf's stored `max(age) ≤ 40`, contribute 0 and skip
   the leaf without reading its values.
2. Otherwise load `BODY(age)` values and the `BODY(active)` bitmap.
3. Vector-compare `age > 40` into a 1024-bit mask; AND it with the `active`
   bitmap and the presence bitmap.
4. **Popcount** the mask → this leaf's count. Merge leaf counts in ascending
   order (deterministic).

The same masking machinery drives filtered aggregates (`sum`/`avg`/`min`/`max`),
group-by (accumulate per dictionary id), and count-distinct.

### 8.3 The serving gates (fail-closed)

Before any kernel runs, the executor *proves* it may serve — else it silently
falls back:

- **Shape:** the query's field paths exactly match the projection's columns
  (`/[]/age` is not `/[]/pet/age`).
- **Revision:** the projection's `buildRevision` must not be newer than the
  revision the query is asking about. (This is what makes time travel safe —
  see 8.4.)
- **Provenance:** if a column is flagged `UNREPRESENTABLE`, decline it; if
  `NON_INTEGRAL`, decline integer-exact answers; and so on.
- **Presence:** aggregate semantics over missing fields must match the tree
  walk's (e.g. `sum` of an all-missing column is empty, not 0).

Failing a gate is cheap and silent: the always-correct generic pipeline
answers, and a negative cache avoids re-probing.

### 8.4 Reading the *past* — time-travel analytics

Here's where versioning pays off for the reader. Each revision's
RevisionRootPage points at its own version of the projection subtree. To run a
group-by "as of revision 42," the query engine:

1. Opens a reader bound to revision 42. That reader resolves the
   projection-index pages *through revision 42's root* — so descriptor reads
   combine exactly the fragments that existed at revision 42 (Part 6 again),
   and segment references resolve to the offsets that were current then.
2. Runs the **same kernels** over that snapshot.

No special "historical query" code path exists — time travel is just "resolve
pages through an older root," and copy-on-write guarantees those older pages are
still there. The `buildRevision` gate (8.3) ensures a reader bound to an older
revision won't accidentally use a projection that was only built later.

### 8.5 What sharing looks like across two revisions

After `replace json value of $doc[0].age with 99` — an in-place edit to
`age` in leaf 0 — revision 6's leaf-0 descriptor points at a **new**
`BODY(age)` segment, but the other four segments are the *same disk pages* as
revision 5:

```
Revision 5 leaf 0:  KEYS@V  BODY(age)@X  BODY(active)@Y  BODY(dept)@Z  DICT(dept)@W
Revision 6 leaf 0:  KEYS@V  BODY(age)@X' BODY(active)@Y  BODY(dept)@Z  DICT(dept)@W
                     └── same ──┘  └ new ┘   └────────── all shared ──────────┘
```

A query at revision 5 and a query at revision 6 run identical kernels over their
respective snapshots; four of the five segments are literally the same bytes on
disk. (There's a test that asserts exactly this by checking disk-offset equality
across revisions.) The store is append-only — nothing reclaims the old
`BODY(age)@X`; the revision history *is* the product, not garbage.

---

## Part 9 — One full worked example, end to end

Let's walk the five-record example through create, query, update, and
time-travel, on a resource using the default **SLIDING_SNAPSHOT** versioning.

**Step 1 — store and create (commit → revision 1).**

```xquery
jn:store('demo','sales.jn','[
  {"age":30,"active":true,"dept":"Eng"},
  {"age":45,"active":false,"dept":"Sales"},
  {"age":52,"active":true,"dept":"Eng"},
  {"age":23,"active":true,"dept":"HR"},
  {"age":61,"active":false,"dept":"Eng"}]')

let $doc := jn:doc('demo','sales.jn')
return jn:create-projection-index($doc, '/[]',
    ('/[]/age','/[]/active','/[]/dept'), ('long','boolean','string'))
```

On disk after commit: one leaf (5 rows), five segment pages —
`KEYS`, `BODY(age)` (FOR base 23, deltas `[7,22,29,0,38]`, 6-bit packed),
`BODY(active)` (bitmap `01101`), `BODY(dept)` (ids `[0,1,0,2,0]`),
`DICT(dept)` (`[Eng,Sales,HR]`) — one descriptor in slot 1, and metadata in
slot 0.

**Step 2 — query (served from the projection).**

```xquery
sum(for $r in $doc[] return $r.age)   (: 30+45+52+23+61 = 211 :)
count(for $r in $doc[] where $r.age > 40 and $r.active return $r)  (: 1 :)
```

`sum(age)` reads only `BODY(age)`. The filtered count reads `BODY(age)` +
`BODY(active)`; the zone map (max 61) can't skip the leaf, so it vector-compares
and popcounts → 1 (only record #2: age 52, active).

**Step 3 — update one value (commit → revision 2).**

```xquery
replace json value of $doc[0].age with 99
```

- Maintenance attributes the change to record 0 → leaf 0.
- Re-encode leaf 0. `BODY(age)` now hashes differently → write a **new**
  `BODY(age)` segment page. `KEYS`, `BODY(active)`, `BODY(dept)`, `DICT(dept)`
  all hash identically → **carried forward** (same pages, no writes).
- The **descriptor** for leaf 0 changed (new segment reference + new zone map),
  so it's written as a small fragment on the HOT leaf page — and because the
  resource is SLIDING_SNAPSHOT, that's a *delta* fragment, not a full-page
  rewrite. Slot 0 metadata is updated.

Net physical writes for this commit: **one** segment page + a descriptor delta.
Everything else is shared with revision 1.

**Step 4 — query the present and the past.**

```xquery
sum(for $r in jn:doc('demo','sales.jn')[] return $r.age)          (: 280 at rev 2 :)
sum(for $r in jn:doc('demo','sales.jn', 1)[] return $r.age)       (: 211 at rev 1 :)
```

The revision-1 query opens a reader through revision 1's root; its leaf-0
descriptor still points at the *original* `BODY(age)@X`, so it reads the old
ages and returns 211. The revision-2 query resolves through revision 2's root to
`BODY(age)@X'` and returns 280. Same kernels, two snapshots, four of five
segments physically shared between them.

That is the whole story: **columnar speed for analytics, kept correct across
every version, with per-commit write cost measured in a handful of small pages
— not a rebuild, not an export.**

---

## Part 10 — Glossary

| Term | Meaning |
|---|---|
| **Analytics / OLAP** | Queries that summarize many records (counts, sums, group-by) rather than fetch one. |
| **Row-major / column-major** | Store each record's fields together vs. store each field's values together. Columnar is faster for analytics. |
| **Projection index** | A saved, always-maintained, versioned columnar copy of selected fields of selected records. |
| **Record / record key** | One JSON object under the projection's root path; its record key is that object's permanent node key. |
| **Node key** | A permanent, ever-increasing 64-bit id for a node. Never reused or changed. |
| **Revision** | One committed, immutable, numbered snapshot of a resource. |
| **Copy-on-write (CoW)** | Never modify a page in place; write a new copy and re-point to it. Enables time travel and lock-free reads. |
| **Structural sharing** | New revisions point at the *same* old pages for everything unchanged. |
| **Page** | The fixed-size unit of storage. Leaf pages hold records/descriptors; indirect pages point down; the RevisionRootPage/UberPage sit on top. |
| **Fragment** | A partial page write containing only the slots that changed in a revision. Reads combine recent fragments. |
| **Versioning algorithm** | The rule for how much to write per revision and how many fragments to combine on read: FULL, INCREMENTAL, DIFFERENTIAL, SLIDING_SNAPSHOT. |
| **`revisionsToRestore` (w)** | The window/bound on how many fragments a read combines. |
| **Write amplification** | Writing far more bytes than the logical change size (e.g. rewriting 1024 records to change one). |
| **Leaf (logical leaf)** | Up to 1024 consecutive records' columns — the unit of extraction, encoding, and maintenance. |
| **Segment** | One column-shaped slice of a leaf's bytes: `KEYS`, `BODY(c)`, or `DICT(c)`. Stored in its own page. |
| **Segment page** | An immutable `ProjectionSegmentPage` holding one segment; identified by file offset; shared across revisions by reference. |
| **Descriptor (`PIXD`)** | The ~100–200 byte per-leaf table of contents in a HOT trie slot: row count, kinds, fences, and per-segment id/length/hash/stats. |
| **HOT trie / HOT leaf page** | The ordered key→value structure (and its leaf pages) mapping leaf number → descriptor. Rides normal page versioning. |
| **Side map** | The `(slot, segmentId) → PageReference` map on a HOT leaf page connecting descriptors to segment pages. |
| **Carry-forward / no-op share** | Skipping a segment write because its re-encoded bytes hash identically to the prior revision's — the old page is referenced instead. |
| **Zone map** | Per-column min/max kept in the descriptor, letting a query skip a whole leaf without reading values. |
| **Presence bitmap** | One bit per row per column: is the field present on this record? |
| **Provenance flags** | Sticky per-column bits (`UNREPRESENTABLE`, `NON_INTEGRAL`, …) recording anything that would make a fast answer inexact. Serving gates read them and decline rather than risk a wrong answer. |
| **Hydrate** | Load and assemble a projection from disk into the query-side cache, once per `(resource, definition, build revision)`. |
| **Fail-closed / fall back** | If the projection can't *prove* it can answer exactly, silently route the query to the always-correct tree walk. |
| **Tombstone** | A marker that a leaf was deleted (or, over slot 0, that the whole projection is stale and must be rebuilt). |
| **Transaction Intent Log (TIL)** | In-memory staging of modified pages during a write; written on commit, dropped on rollback. |
| **FOR (frame of reference)** | Store a block minimum plus small offsets instead of full-width values. |
| **Dictionary encoding** | Store each distinct string once; replace values with small integer ids into the dictionary. |
