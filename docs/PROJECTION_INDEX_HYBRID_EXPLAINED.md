# Projection Index Storage — the Hybrid Layout, Explained Simply

This is the *plain-English* companion to
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md)
(the precise design spec) and
[`PROJECTION_INDEX_STORAGE_REDESIGN.md`](PROJECTION_INDEX_STORAGE_REDESIGN.md)
(the deep dive). You do **not** need to know anything about databases, indexes,
or versioning to read this page. If you've ever used a `struct`, a pointer, or a
ZIP file, you already have every concept you need.

> **One thing has changed since this was written, and it simplifies the story.**
> Back then a leaf had **one** slot — a descriptor with the small segments
> packed into it — so "inline" meant "inside the descriptor", and two dials
> (§4) decided which segments got in and how many. Today **every segment has a
> slot of its own** (a 1:1 segment ⇔ slot mapping), so there is no packing and
> no budget: each segment is inline in *its own* slot if it is ≤ 512 bytes, and
> in its own page otherwise. Same idea — small things stay where you're already
> looking — with the "which ones fit?" competition removed, because a segment
> now shares its slot with nothing.
>
> Read §§1–3 and 6 onward as written; they are unaffected. §4's two dials and
> §5's smallest-first arithmetic are the historical version; the marked notes in
> those sections say what the numbers are now.

---

## 1. The one-sentence idea

> **Store small things right where you're already looking; give big things their
> own drawer and keep a pointer to it.**

That's it. If you've heard of **small-string optimization** — where a string
class keeps a short string *inside* the object and only jumps to the heap for a
long one — this is the exact same trick, applied to how a projection index
writes its data to disk.

```
short string  → stored inline in the object   (no heap allocation, no pointer chase)
long string   → stored on the heap + a pointer (the object just holds the pointer)

small data    → stored inline in the directory (no page, no pointer, no extra disk read)
big data      → stored in its own page + a ref (the directory just holds the ref)
```

---

## 2. The five words you need first

The projection index keeps a compact, column-oriented copy of a few fields from
your data so that certain scans are fast. To store it, it chops the data up.
Here are the only terms used below:

| Term | What it actually is | Everyday analogy |
|---|---|---|
| **Page** | A self-contained block of bytes written to disk, found later by its byte offset in the file. | A file on disk. |
| **Leaf** | One batch of up to **1024 rows**. | One page of a spreadsheet. |
| **Segment** | The encoded bytes for *one piece* of a leaf: the row keys, **or** one column's values, **or** one text column's dictionary. | One column, packed into a blob. |
| **Descriptor** | A tiny "table of contents" for one leaf. It lists every segment with its **id**, **size**, and a **checksum**. | The central directory of a ZIP file. |
| **Inline vs referenced** | *Inline* = the segment's bytes sit inside the descriptor. *Referenced* = the bytes are in a separate page, and the descriptor just holds a pointer to it. | SSO: inline vs heap+pointer. |

So one leaf = **one descriptor** + **several segments**. A 3-column leaf has
about 5 segments:

```
leaf (≤1024 rows, 3 columns: age, active, dept)
├─ segment 0   KEYS         the row keys
├─ segment 1   BODY(age)    the 1024 age values, packed
├─ segment 4   BODY(active) the 1024 booleans, packed as bits
├─ segment 7   BODY(dept)   for each row, "which department?" as a small number
└─ segment 8   DICT(dept)   the actual department strings ("Eng", "Sales", …)
```

(The id numbering — 0, 1, 4, 7, 8 — is just `KEYS=0`, and per column
`BODY = 3·c+1`, `DICT = 3·c+2`. You can ignore it.)

**Granularity, stated plainly:** a segment is *one column's ≤1024 values for one
leaf* — not the whole column. A full column is sliced across all leaves, so it
becomes roughly **one segment per leaf** (a 100-million-row column → ~97,000
segments, each ≤1024 values). So a BODY segment is genuinely small — bytes to a
few KB. (The 16 MB you'll see later is just a far-away corruption ceiling, not
the unit size.)

---

## 3. The problem: tiny things were paying big-thing prices

Before this change, **every segment got its own page** — its own little file on
disk. That's fine for a big segment. It's terrible for a small one, and *most
segments are small*:

| A real segment | Its encoded size |
|---|---|
| A boolean column, 1024 rows | ~152 bytes |
| A dictionary of 8 short words ("Eng", "Sales", …) | ~60 bytes |
| A column that's all the same value (or all empty) | ~7–33 bytes |
| A column of one repeated short string | ~25 bytes |

Now look at what a *separate page* costs, no matter how small the payload:

```
per referenced segment, the fixed overhead is roughly:
   16 bytes   a directory entry pointing at the page
 + ~14 bytes  the page's own framing (length prefix, header, alignment padding)
 +  6 bytes   a header repeated inside the page
 ─────────
  ~36 bytes   of pure overhead …to store as little as 7–60 bytes of real data
 + one extra random disk read every time you want those bytes
 + one more page, one more pointer, one more cache entry to track
```

Spending **36 bytes of overhead and a disk seek to store a 60-byte dictionary**
is like renting a whole shipping container to mail a postcard. And a projection
over 100 million rows has ~97,000 leaves, so this waste is multiplied ~97,000×.

---

## 4. The fix: a size-based hybrid

Give each segment one of two homes, chosen purely by its size:

- **Small segment → INLINE.** Its bytes are appended right after the descriptor's
  table of contents. No page, no pointer, no extra disk read — when you read the
  descriptor (which you were doing anyway), the bytes are already in your hand.
- **Big segment → REFERENCED.** Exactly as before: its own page, and the
  descriptor holds a pointer (a byte offset) to it.

**Today one number decides "small": 512 bytes.** A segment ≤ 512 B rides its own
slot; anything larger gets a page. That is the whole rule — no eligibility
threshold, no per-leaf budget, no ordering, because each segment has a slot to
itself and is not competing with its siblings for room.

<details>
<summary>The historical version: two dials, when all segments shared one slot</summary>

When a leaf was **one** slot, inlining meant packing bytes into the descriptor
alongside its table of contents, so the segments *did* compete and two dials
refereed:

| Dial | Default | Meaning |
|---|---|---|
| `inlineMaxSegmentBytes` | **192** | A segment is *eligible* to go inline only if it's ≤ this many bytes. |
| `inlineMaxTotalBytes` | **512** | A cap on the *total* inlined per leaf. Smallest segments go inline first; once the budget is used up, the rest spill to pages. |

Setting `inlineMaxSegmentBytes = 0` turned inlining off entirely — the old
"everything is a page" behavior, handy for A/B comparisons. The cap mattered
because inlined bytes made the descriptor bigger, and a descriptor so fat that
few fit in memory would defeat the point.

Both properties still exist, but no longer change what is written to disk: the
storage layer strips the descriptor's inline region, so the only inlining that
happens is the per-slot rule above. They survive as a test seam for forcing
every segment onto a page, which is the only way to *observe* page sharing.

</details>

---

## 5. A worked example

Take one leaf: **1024 rows**, three columns — `age` (numbers), `active`
(true/false), `dept` (short text). Suppose the encoder produces:

| Segment | Encoded size | ≤ 512 B? |
|---|---:|:--:|
| KEYS (row keys) | 900 B | no |
| BODY(age) | 1,600 B | no |
| BODY(active) — booleans | **152 B** | **yes** |
| BODY(dept) — "which dept?" numbers | **384 B** | **yes** |
| DICT(dept) — the 8 department words | **60 B** | **yes** |

Three segments are under 512 B, so each rides **its own slot** inline; the other
two get pages. Nothing has to be ranked or budgeted — the question is asked once
per segment, in isolation.

*(Under the historical two-dial rule only `active` and `DICT(dept)` were eligible
at all — `BODY(dept)`'s 384 B exceeded the 192 B per-segment threshold — and the
two that qualified totalled 212 B, inside the 512 B per-leaf budget. Giving each
segment its own slot is what let the 384 B one stay inline too.)*

### Before (everything a page) vs. after (hybrid)

```
BEFORE                                   AFTER (hybrid)
─────────────────────────────           ─────────────────────────────
descriptor ──► KEYS page  (900 B)        descriptor ──► KEYS page  (900 B)
           ──► age page   (1600 B)                  ──► age page   (1600 B)
           ──► active page (152 B)  ✗               ──► dept-num page (384 B)
           ──► dept-num page (384 B)                │
           ──► dict page   (60 B)   ✗               ├─ [inline] active bytes (152 B)
                                                    └─ [inline] dept dict   (60 B)

5 pages written                          3 pages written
5 pointers, 5 checksums                  3 pointers
5 random reads to reload the leaf        3 random reads (active & dept names are
                                          already in the descriptor you just read)
```

Two whole pages vanish, along with their pointers, checksums, and disk seeks —
and reading the `active` column or the department names now costs **zero** extra
I/O, because those bytes ride along inside the descriptor.

### What the descriptor bytes look like

The descriptor is a fixed-size table of 30-byte entries (one per segment),
followed by a "blob region" holding the inline bytes back to back:

```
┌───────────────────────────────────────────────────────────────────────┐
│ header: rowCount=1024, columnCount=3, kinds, row-key range …           │
├───────────────────────────────────────────────────────────────────────┤
│ entry KEYS      : id=0  size=900   [REF]   checksum, min/max           │
│ entry BODY(age) : id=1  size=1600  [REF]   checksum, min/max           │
│ entry BODY(actv): id=4  size=152   [INLINE] checksum, min/max          │  ← tagged inline
│ entry BODY(dept): id=7  size=384   [REF]   checksum, min/max           │
│ entry DICT(dept): id=8  size=60    [INLINE] checksum                   │  ← tagged inline
├───────────────────────────────────────────────────────────────────────┤
│ inline region:  << 152 bytes of active >><< 60 bytes of dept dict >>   │  ← the actual bytes
└───────────────────────────────────────────────────────────────────────┘
```

*(The picture above is the historical one-slot-per-leaf form. Today the entry
table is the same, but there is no inline region under it: each of those five
segments is its own slot, holding its own bytes when small. The descriptor's job
narrowed to "name every segment and vouch for it".)*

**How is a segment tagged inline?** *(Historical — see the note above.)* Each entry already stores the segment's
`size` as a 32-bit integer. A segment is one column's ≤1024 values for one leaf —
in practice bytes to a few KB, and *hard-capped* at 16 MB (a safety ceiling, not
a normal size). Either way it's far below 2³¹, so the top bit of that integer is
*always* zero — free real estate. We set that top bit to mean "inline." Readers
mask it off to get the true size. So we tagged the storage class **without
growing the format by a single byte**, and a descriptor with no inline segments
is byte-for-byte identical to the old format.

**Where are an inline segment's bytes?** In the blob region, the inline segments
appear in the same order as their entries. So an inline segment's bytes start at
`(end of the entry table) + (sum of the sizes of the inline segments listed
before it)`. No per-entry offset field needed — you just add up the sizes.

---

## 6. Reading and writing, step by step

**To read segment X:**

```
look up X's entry in the descriptor           (size + checksum: what X must be)
go to X's own slot
if the slot says INLINE:
    its bytes are right there in the slot      →  return them
else (REFERENCED):
    follow the slot's page reference, read it  →  return its bytes
verify the bytes against the entry's size + checksum
```

**To write a leaf:**

```
encode the leaf into segments (KEYS, one BODY per column, DICT per text column)
write the descriptor to slot (rowGroupId, 0)   — sizes + checksums, no bytes
for each segment:
    if its size + checksum match the previous descriptor's entry → write NOTHING
       (the slot and its page carry forward untouched — this is the sharing)
    else write it to slot (rowGroupId, segmentId + 1):
       ≤ 512 B → the bytes ride the slot value
       larger  → a lone marker in the slot + its own page
```

That's the whole thing. Note where the carry-forward test lives: in the
descriptor's size + checksum, so an unchanged segment is recognised *without
reading its old bytes*.

---

## 7. Two subtleties worth knowing

**A segment can switch homes over time.** Data changes. A dictionary that was 60
bytes (inline) can grow past 512 bytes and become referenced on the next write;
a column that becomes all-one-value can shrink and flip to inline. The writer
just re-checks the size each time and does the right thing — and if a segment
that *used* to have a page becomes inline, its old page is dropped so nothing
dangles.

**This is a placement optimization, not a compression trick.** Inlining doesn't
shrink your data — a 152-byte segment is 152 bytes whether inline or in a page.
What it removes is the *per-segment overhead* (the page, the pointer, the extra
header, the disk seek) that used to dwarf small segments. Big segments are
deliberately left alone in their own pages, because stuffing multi-KB blobs into
the descriptor would make descriptors huge and defeat the purpose.

---

## 8. Why this mirrors the rest of the codebase

This isn't a bespoke mechanism. It's the same inline-or-spill split that
`KeyValueLeafPage` (the main node store) already uses for records: a small
record lives inline in the page; an oversized one spills to a separate
`OverflowPage` and the page keeps a pointer. The hybrid makes the projection
index use that **same** referenced-page type (`OverflowPage`) and the **same**
idea (small inline, big referenced) — just applied per column-segment instead of
per record. If you understand one, you understand the other.

---

## 8a. The same trick, now for the metadata and the zone map

The projection also keeps two *bookkeeping* blobs, and both now use the exact
same inline-or-spill rule:

- **The metadata** (slot 0): the projection's "table of contents" — its root
  path, column names/types, how many leaves it has. It's a few hundred bytes, so
  it now lives **inline** in its slot. Opening a projection reads its shape from
  that one slot, with no extra disk hop to a separate metadata page.

- **The zone map** (the "fences"): for each leaf, the first and last record it
  covers — the little index maintenance uses to find which leaves a write
  touched. This used to sit *inside* the metadata blob, which meant every single
  save rewrote the whole thing (≈1.5 MB at scale) just because one leaf shifted
  by one row — and because the database keeps every past version forever, that
  1.5 MB was paid *again on every save*. Now the fences are cut into fixed
  **chunks** (512 leaves each). Saving only rewrites the chunk(s) that actually
  changed; the rest are recognized as unchanged and shared with the previous
  version for free. A typical save now costs a few KB instead of ~1.5 MB.

  These chunks are 8 KB each, which is "big," so — following the same rule —
  they stay **referenced** (own page), not inline. Only the small metadata
  inlines.

If you read §8, this is nothing new: *small inline, big referenced*, applied
once more. The only extra idea is **chunking** the zone map so a small change
touches a small amount of data — the same reason a text editor saves diffs
instead of rewriting the whole file.

---

## 9. The knobs, in one place

| Property | Default | Effect |
|---|---|---|
| `sirix.projection.inlineMaxSegmentBytes` | `192` | Historical: max size for a single segment to be inline-eligible *inside the descriptor*. No longer affects what is persisted (the storage layer strips the descriptor's inline region); kept as a test seam for pinning every segment to a page. |
| `sirix.projection.inlineMaxTotalBytes` | `512` | Historical: max total inline bytes per leaf. Same status as above. The live threshold is the per-slot 512 B in `ProjectionIndexHOTStorage`, which is not configurable. |
| `sirix.projection.inlineMaxSegmentBytes=0` | — | Disables inlining → the old "every segment is a page" layout. |

For the byte-level format, the read/write/hydrate wiring, and the correctness
corner cases, continue to
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md).
