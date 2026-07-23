# Why We Don't Split a Segment Across Many HOT Slots — Explained Simply

This is a plain-English companion to
[`PROJECTION_INDEX_HYBRID_EXPLAINED.md`](PROJECTION_INDEX_HYBRID_EXPLAINED.md).
It answers one recurring question:

> A whole column of a leaf is stored as one **segment**. Why not chop that
> segment into several smaller pieces, each in its own HOT slot, so that
> changing a few values only rewrites the pieces that changed?

It's a good question — the instinct is exactly right in general, and we used
that same instinct elsewhere (the fence chunks, §"But we just chunked the
fences!"). It just loses *for this particular structure*. Here's the honest
why, no database background required.

---

## 1. The one-sentence answer

> **A segment is already small and is read as one whole; slicing it up makes
> reads slower and adds per-piece bookkeeping that costs more than the writes it
> saves.**

---

## 2. What "splitting a segment into slots" means

Today, one column of one leaf (up to 1024 values) is **one segment** — a single
packed blob, stored either inline in the leaf's directory or in its own page:

```
leaf 42, column "age"  →  [ one segment: 1024 packed ages, ~340 bytes ]
```

The proposal is to cut that one segment into, say, four quarters, each living in
its own HOT slot:

```
leaf 42, column "age"  →  [ slot A: ages 0..255 ]
                          [ slot B: ages 256..511 ]
                          [ slot C: ages 512..767 ]
                          [ slot D: ages 768..1023 ]
```

The hoped-for win: if a user edits one age in the first quarter, you rewrite
only slot A and leave B, C, D untouched (shared with the previous version).

That's a completely reasonable idea. It's the same idea as saving a diff instead
of re-saving a whole file. It's why the fence zone-map got chunked. So why not
segments?

---

## 3. Why it's tempting (the steel-man)

Let's give the idea its strongest case first, because part of it is genuinely
true:

- SirixDB keeps **every past version forever** (copy-on-write). So anything you
  rewrite on a save is paid *again* and kept *forever*. Rewriting less per save
  is real, permanent savings.
- For a **workload with many tiny commits** (think: a live app poking single
  rows all day, with time-travel on), write amplification is the thing that
  hurts. Finer pieces = smaller rewrites = less amplification.
- We literally did this for the fences and it was a 60–195× win.

So the versioning intuition is **not wrong**. Keep it in your back pocket — it
comes back in §6.

---

## 4. Why it loses here — four plain reasons

### Reason 1 — The HOT index is a *reading* machine, and slicing slows reading

The entire point of this structure is speed of **reads**: to answer a query, we
reassemble a leaf's columns into one flat array of bytes and let the CPU's
vector unit rip through it (thousands of values per instruction). Reassembly
wants the column's bytes **in one contiguous piece**.

Split the segment into four slots and every read has to: find four slots instead
of one, possibly fetch four pages instead of one, and stitch the four pieces
back together before the fast part can even start. You've made the thing whose
*only job* is to be fast, slower — to save writes on a structure that is read far
more often than it's written.

### Reason 2 — The bookkeeping costs more than the payload it splits

Every slot carries a little header of its own: a key, a length, an integrity
hash, a pointer. That's roughly **30 bytes of overhead per piece**. The average
segment is only ~340 bytes. Split it into four and you've added ~120 bytes of
pure bookkeeping to a 340-byte payload — a **35% tax** — *and* the versioning
engine now has four little headers to re-emit instead of one. The finer you
slice, the worse this gets: you can easily reach the point where the directory
entries weigh more than the data they describe.

### Reason 3 — The packing glues the whole column together

The values in a segment aren't stored as-is; they're **packed relative to a
shared baseline**. The column picks one "base" number and one bit-width for all
1024 values, then stores each as a small offset from the base.

Change one value to something outside the current range and the base or the
width has to change — which **re-packs the entire column**. So a "quarter" slot
doesn't actually protect the other three quarters: a single unlucky edit
invalidates all of them anyway. The encoding is holistic; slicing it fights the
encoding instead of cooperating with it.

### Reason 4 — You already get the containment that matters, for free

Here's the part people miss: updates are **already** contained at the column
level. Edit the "age" of one record and the save rewrites the `age` segment
only — the `active`, `dept`, and key segments are recognized as unchanged (same
bytes → same hash) and **shared with the previous version at no cost**. That's
the mechanism the whole design is built on.

So sub-slots would be trying to make *within one column* what we already have
*across columns*. And within one column, Reason 3 says most real edits touch the
whole packed stream anyway. The juice that's left is thin.

---

## 5. A note on the versioning engine (historical caveat)

An earlier version of this argument leaned on a fourth reason: that SirixDB's HOT
pages did a periodic **full re-emit** at every window rotation — writing the whole
page from scratch every `revsToRestore` commits — which rewrote all the extra
sub-slot pieces and their headers, so the sub-slot write savings "mostly
evaporated" and small segments came out storage-negative.

**That is no longer true.** HOT leaf pages now implement *true* sliding-snapshot
carry-forward (like the record-page path): a rotation commit re-emits only the
still-live entries of the fragment aging out, as a sparse delta — never the whole
page (`VersioningType.carryForwardAgingHOTEntries`). So there is no periodic
full-dump for the sub-slot scheme to pay extra on, and this particular argument
against sub-slotting has dissolved.

What remains are Reasons 1–4 above, which do not depend on the re-emit: sub-
slotting still slows the hot read path (Reason 1) and still adds per-piece
bookkeeping that rivals a small segment's payload (Reason 2), and intra-segment
slicing still fights the shared FOR packing (Reason 3). For *small* projection
segments those costs stand on their own. The honest verdict below is now scoped
to them — and §7's "the instinct wins for large segments" is, if anything,
stronger, since the write side no longer erases the benefit.

---

## 6. "But we just chunked the fences — why was *that* OK?"

Because the fences are the mirror-image situation, and the contrast is the whole
lesson:

| | Fence zone-map | Column segments |
|---|---|---|
| Size | **Huge in aggregate** (~1.5 MB) | **Tiny individually** (~340 B) |
| How often it changes per save | A few leaves out of thousands | The column you edited |
| Read pattern | Read once by the *writer*, never by queries | Read on **every query**, by the fast path |
| Internal coupling | **None** — a flat array of independent numbers | **Total** — one shared base/width packs all values |
| Header overhead of chunking | Negligible vs. the 8 KB chunk | Comparable to the ~340 B payload |

Chunking wins exactly when the thing is **big**, **rarely changed per save**,
**not on the hot read path**, and **has no internal coupling to fight**. The
fences are four-for-four. Column segments are zero-for-four. Same tool, opposite
material.

---

## 7. The one place the instinct *is* right — large segments

The versioning argument genuinely wins when a segment is **big**: a near-limit
column (values can pack up to megabytes) or a large string dictionary. There,
rewriting the whole thing for a small change really is wasteful, and finer
granularity would pay off — the read and bookkeeping costs are now small
*relative to* the payload.

That's why the **referenced-page mechanism is kept** for the general index /
large-blob case, and why this "don't sub-slot" conclusion is scoped to the
*projection* leaves, whose segments are small by construction (1024 rows,
tightly packed). If a future column kind routinely produced multi-KB or MB
segments, revisiting sub-slotting *for that kind* would be the right call — the
math flips with the size.

So the honest verdict isn't "sub-slotting is bad." It's:

> **For small, hot-path, tightly-packed projection segments, sub-slotting costs
> more than it saves. For large segments it would help — which is exactly the
> regime where the referenced-page mechanism already lives. Keep the segment as
> the unit here; keep the versioning intuition for where segments get big.**

---

## 8. One-paragraph summary

A projection segment is small, is read on every query as one contiguous piece,
and is packed as a single coupled unit. Splitting it across HOT slots slows the
reads, adds per-piece headers that rival the data, and is defeated by the packing
on most real edits — costs that stand on their own now that HOT sliding-snapshot
carry-forward has removed the periodic full re-emit the earlier analysis also
leaned on (§5). The same chunking trick *did* win for the fence zone-map, because
that data is big, rarely changed per save, off the read path, and internally
uncoupled — the exact opposite profile. The versioning instinct is sound; it just
points at *large* segments, which is precisely where the referenced-page
mechanism already applies.
