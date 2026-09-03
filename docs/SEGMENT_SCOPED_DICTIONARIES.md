# Segment-scoped record-page dictionaries — retiring the load-time pre-pass

Written 2026-09-03, on the user's direction *"we should do 2 next, getting rid of the prepass"*, after
the 100M trie-lane re-gate measured −10.84 GB and the objections that came with it.

## Why the pre-pass exists at all

The trie lane makes a DOCUMENT record page store dictionary **ids** instead of strings. The dictionary it
points into is the **projection's** resource-wide dictionary. That single choice is the source of every
objection raised against it:

| consequence | because |
|---|---|
| the corpus must be read twice | the projection's dictionary must be complete *before* the first record is shredded |
| the source must be re-readable | same |
| the dictionary is CLOSED — `PrebuiltGlobalDictionary` throws on an unknown value | ids are ranks; a value inserted in the middle would renumber everything after it |
| later inserts lose the rank property | appended entries land past `orderedPrefixCount` |
| the extraction was benchmark-shaped | `PrePassDictionaryBuilder` is a *"MEASUREMENT HARNESS … minus the corpus scan"* by its own javadoc |

## The change

**Give document record pages their own dictionaries, scoped to a SEGMENT of leaves, built during the
shred.** The projection keeps its dictionary and its own build path; the two are decoupled.

Nothing about the query lane changes: queries read projection columns. The only query path that touches a
document page is winner materialization, which resolves through whatever dictionary its page names.

## Why a segment is the right scope — measured, not assumed

Distinct-value bytes per scope, 1M ClickBench rows in row order:

| column | per-leaf (~9.7 rows) | 1 k rows | 10 k rows | 100 k rows | global |
|---|---|---|---|---|---|
| URL | 57.5 MB | 44.6 | 41.8 | **38.3** | 35.3 |
| Title | 61.2 MB | 34.7 | 28.8 | **20.1** | 12.6 |
| Referer | 46.2 MB | 34.2 | 32.3 | **30.2** | 28.5 |

Fraction of the global dictionary's saving captured: 1 k rows 55–68 %, 10 k rows 67–79 %,
**100 k rows 85–90 %**.

The duplication is *temporally local* — ClickBench rows arrive roughly ordered by counter and time, so a
URL's ~5.5 occurrences cluster. Our per-leaf scope catches 1.6 of them because a leaf is only **9.7 rows**
(a row is ~106 fused records; a leaf ~1024 records). For comparison: DuckDB's dictionary scope is a
~122 k-row row group, Umbra/Data Blocks' a ~64 k-tuple block, ClickHouse's a part. **We are four orders of
magnitude below every shipping system, which is the real reason a global dictionary looked necessary.**

A 100 k-row segment holds ≈ 31,000 distinct URLs ≈ **3.8 MB** — sorted in memory when the segment freezes.

## Why the existing incremental machinery becomes affordable at this scope

The streaming dictionary was rejected at 1,650 B/entry. That figure is **superlinear in D**, and the code
says why: it *"persists a forward radix … 0.81 radix nodes per entry, each carrying a 256-slot child
array"* and *"each bounded append writes a fresh set of forward radix nodes at new keys and copy-on-write
retains every one of them"* — measured 64.7 B/entry at D = 275 K, 173 at 2.62 M, 1,650 at 18 M.

At D ≈ 31 k per segment we are far below the knee. Better: a segment sorted at freeze is **rank-ordered
within itself**, and `GlobalValueDictionaryRadix.append` already takes `buildForwardIndex=false` for that
case, because *"which id holds this value is a binary search over the reverse index, which is already
sorted by value"*. So the forward radix — the whole cost — disappears.

## Shape of the implementation

1. **Collect** a segment's distinct strings while its leaves fill. `ExternalDistinctValues`
   (`de755c82b`) is the collector: arena, sort, spill, k-way merge, ordered by
   `ValueDictionaryEntryNode.compareUtf16Range`. At 3.8 MB per segment it will never spill, but the
   bound is there for corpora that are not temporally local.
2. **Freeze** at segment close: `PrePassDictionaryBuilder.build(wtx, column, Iterator<byte[]>)` — the
   streaming overload added in the same commit — with `buildForwardIndex=false`.
3. **Anchor**: each record page names its segment's dictionary instead of the resource-wide one.
   `KeyValueLeafPage.globalStringDictionaries` is the existing injection point, and
   `PageKind.serializeKeyValueLeafPage` already carries the per-page tag the lane uses.
4. **Read**: unchanged in shape — the lazy route already resolves a page's ids through the resolver it
   is given (`6909f0ccf`), and the decode-side scratch aliasing is fixed (`4a1f28d8b`).

## What it gives up, and what it removes

Gives up the 10–15 % of dedup only a corpus-wide dictionary reaches. Removes: the pre-pass, the second
read of the source, the closed corpus, the absent-value build failure, the benchmark-shaped extractor,
and the coupling between document storage and the projection.

## Before implementing — the measurement that must be taken first

**Re-measure the scope curve on the 100M row-order values.** The 85–90 % is a 1M number; at 100× the rows
a global dictionary reaches more long-tail duplicates, so the segment fraction should FALL. This campaign
has twice been wrong by carrying a ratio across scales (see `docs/ROADMAP_TO_30GB.md`, and the P2 gate
that was off by 1.6×). Do not commit to a GB figure until that curve exists.

## Levers this supersedes or rules out

- **Overflow compression** (−4.8 GB at 100M by size) — **refused**, re-measured 2026-09-03: the full
  43-query leg on the two 1M arms is hot sum 1.308 → 2.377 s (**+81.7 %**), q17 +190 %, q18 12×. The
  `lz77` native-decode fix did not make it free.
- **OriginalURL** (−1.87 GB) — unreachable: `PROJECTED_COLUMNS` derives from the columns the QUERIES
  reference and no query reads it, so no dictionary is built for it. Storage compression is currently
  coupled to the query projection, which is itself worth fixing.
- **Temporal encoding** (−3.82 GB) — still live, and orthogonal: the projection already stores those
  columns as epochs, but the record pages hold text and `NodeKind` has no temporal kind.
