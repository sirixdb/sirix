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

## The 100M curve — MEASURED, and the fraction fell

`p2gate.ScopeCurve` over the full 23.7 GB corpus (1202 s), capture of the global dictionary's saving:

| scope | 1 k rows | 10 k | 100 k | 1 M | global |
|---|---|---|---|---|---|
| URL | 43.6 % | 60.8 % | 76.7 % | 87.5 % | 100 % |
| Title | 46.5 % | 64.2 % | 79.5 % | 89.7 % | 100 % |
| Referer | 52.5 % | 66.9 % | 80.1 % | 88.7 % | 100 % |
| **all three** | **46.7 %** | **63.6 %** | **78.7 %** | **88.8 %** | 100 % |
| saving | −4.99 GB | −6.80 GB | −8.41 GB | **−9.49 GB** | −10.69 GB |

At 100 k rows the capture is **78.7 %**, against the 85–90 % the 1M curve promised — quoting the 1M ratio
would have overstated the lever by ~1 GB. **The sweet spot moved with scale**: a 1 M-ROW segment captures
88.8 % and holds ~231,000 distinct URLs ≈ 39 MB, still sorted in memory at freeze and still never
spilling.

**Cross-validation:** the corpus scan says a global dictionary saves 10.69 GB on URL+Title+Referer; the
measured 100M database delta was 10.84 GB on those three plus SearchPhrase. Two unrelated instruments,
1.4 % apart.

## What this costs and what it reaches

| | database | pre-pass | closed corpus | query cost |
|---|---|---|---|---|
| global dictionary (today) | **52.49 GB** measured | required | yes | +1.29 ln |
| segment dicts @ 1 M rows | 53.84 GB projected | **none** | **no** | unmeasured |
| + temporal encoding (−3.82) | **≈ 50.0 GB** | none | no | unmeasured |

**Giving up the pre-pass costs ~1.35 GB.** The generic route reaches the ~50 GB target with the temporal
encoding; the pre-pass route does not reach it at all (52.49 GB, and its two remaining levers are refused
or unreachable — see below).

## Levers this supersedes or rules out

- **Overflow compression** (−4.8 GB at 100M by size) — **refused**, re-measured 2026-09-03: the full
  43-query leg on the two 1M arms is hot sum 1.308 → 2.377 s (**+81.7 %**), q17 +190 %, q18 12×. The
  `lz77` native-decode fix did not make it free.
- **OriginalURL** (−1.87 GB) — unreachable: `PROJECTED_COLUMNS` derives from the columns the QUERIES
  reference and no query reads it, so no dictionary is built for it. Storage compression is currently
  coupled to the query projection, which is itself worth fixing.
- **Temporal encoding** (−3.82 GB) — still live, and orthogonal: the projection already stores those
  columns as epochs, but the record pages hold text and `NodeKind` has no temporal kind.
