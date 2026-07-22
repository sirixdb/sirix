# Projection Index Storage — the Hybrid Layout, Explained Simply

This is the *plain-English* companion to
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md)
(the precise design spec) and
[`PROJECTION_INDEX_STORAGE_REDESIGN.md`](PROJECTION_INDEX_STORAGE_REDESIGN.md)
(the deep dive). You do **not** need to know anything about databases, indexes,
or versioning to read this page. If you've ever used a `struct`, a pointer, or a
ZIP file, you already have every concept you need.

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

Two dials decide "small":

| Dial | Default | Meaning |
|---|---|---|
| `inlineMaxSegmentBytes` | **192** | A segment is *eligible* to go inline only if it's ≤ this many bytes. |
| `inlineMaxTotalBytes` | **512** | A cap on the *total* inlined per leaf. Smallest segments go inline first; once the budget is used up, the rest spill to pages. |

Setting `inlineMaxSegmentBytes = 0` turns inlining off entirely — you get the
exact old "everything is a page" behavior. (Handy for A/B comparisons.)

The cap matters: inlined bytes make the descriptor bigger, and we don't want a
descriptor so fat that few of them fit in memory. The budget keeps descriptors
small (a few hundred bytes) while still absorbing the many tiny segments.

---

## 5. A worked example

Take one leaf: **1024 rows**, three columns — `age` (numbers), `active`
(true/false), `dept` (short text). Suppose the encoder produces:

| Segment | Encoded size | ≤ 192? |
|---|---:|:--:|
| KEYS (row keys) | 900 B | no |
| BODY(age) | 1,600 B | no |
| BODY(active) — booleans | **152 B** | **yes** |
| BODY(dept) — "which dept?" numbers | 384 B | no |
| DICT(dept) — the 8 department words | **60 B** | **yes** |

Two segments are eligible: `active` (152 B) and `DICT(dept)` (60 B). Their total,
212 B, is under the 512 B budget, so **both go inline**. The other three are big,
so they stay **referenced** (their own pages).

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

**How is a segment tagged inline?** Each entry already stores the segment's
`size` as a 32-bit integer. A real segment is at most 16 MB, so the top bit of
that integer is *always* zero — free real estate. We set that top bit to mean
"inline." Readers mask it off to get the true size. So we tagged the storage
class **without growing the format by a single byte**, and a descriptor with no
inline segments is byte-for-byte identical to the old format.

**Where are an inline segment's bytes?** In the blob region, the inline segments
appear in the same order as their entries. So an inline segment's bytes start at
`(end of the entry table) + (sum of the sizes of the inline segments listed
before it)`. No per-entry offset field needed — you just add up the sizes.

---

## 6. Reading and writing, step by step

**To read segment X:**

```
look up X's entry in the descriptor
if the entry is tagged INLINE:
    its bytes are right here in the descriptor's blob region  →  return them
else (REFERENCED):
    follow the entry's pointer to the page, read the page      →  return its bytes
verify the bytes against the entry's size + checksum
```

**To write a leaf:**

```
encode the leaf into segments (KEYS, one BODY per column, DICT per text column)
classify each segment: inline if small enough and the budget allows, else referenced
build the descriptor:
    inline segments  → append their bytes to the blob region, tag their entries
    referenced segments → write each as its own page, store the pointer
```

That's the whole thing.

---

## 7. Two subtleties worth knowing

**A segment can switch homes over time.** Data changes. A dictionary that was 60
bytes (inline) can grow past 192 bytes and become referenced on the next write;
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

## 9. The knobs, in one place

| Property | Default | Effect |
|---|---|---|
| `sirix.projection.inlineMaxSegmentBytes` | `192` | Max size for a single segment to be inline-eligible. |
| `sirix.projection.inlineMaxTotalBytes` | `512` | Max total inline bytes per leaf (smallest-first; the rest spill to pages). |
| `sirix.projection.inlineMaxSegmentBytes=0` | — | Disables inlining → the old "every segment is a page" layout. |

For the byte-level format, the read/write/hydrate wiring, and the correctness
corner cases, continue to
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md).
