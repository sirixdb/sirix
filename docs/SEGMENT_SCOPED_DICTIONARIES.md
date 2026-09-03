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

## Why a segment is the right scope — the 1M curve (superseded at scale by the 100M curve below)

Distinct-value bytes per scope, 1M ClickBench rows in row order:

| column | per-leaf (~9.7 rows) | 1 k rows | 10 k rows | 100 k rows | global |
|---|---|---|---|---|---|
| URL | 57.5 MB | 44.6 | 41.8 | **38.3** | 35.3 |
| Title | 61.2 MB | 34.7 | 28.8 | **20.1** | 12.6 |
| Referer | 46.2 MB | 34.2 | 32.3 | **30.2** | 28.5 |

Fraction of the global dictionary's saving captured: 1 k rows 55–68 %, 10 k rows 67–79 %,
100 k rows 85–90 % — **all of these are 1M figures and all of them are optimistic; see the measured 100M
curve below, where 100 k rows captures 78.7 %.**

The duplication is *temporally local* — ClickBench rows arrive roughly ordered by counter and time, so a
URL's ~5.5 occurrences cluster. Our per-leaf scope catches 1.6 of them because a leaf is only **9.7 rows**
(a row is ~106 fused records; a leaf ~1024 records). For comparison: DuckDB's dictionary scope is a
~122 k-row row group, Umbra/Data Blocks' a ~64 k-tuple block, ClickHouse's a part. **We are four orders of
magnitude below every shipping system, which is the real reason a global dictionary looked necessary.**

A 100 k-row segment holds ≈ 31,000 distinct URLs ≈ 3.8 MB at 1M; at 100M the chosen 1 M-row segment holds
≈ 231,000 ≈ **39 MB** — either way sorted in memory when the segment freezes.

## Why the existing incremental machinery becomes affordable at this scope

The streaming dictionary was rejected at 1,650 B/entry. That figure is **superlinear in D**, and the code
says why: it *"persists a forward radix … 0.81 radix nodes per entry, each carrying a 256-slot child
array"* and *"each bounded append writes a fresh set of forward radix nodes at new keys and copy-on-write
retains every one of them"* — measured 64.7 B/entry at D = 275 K, 173 at 2.62 M, 1,650 at 18 M.

At D ≈ 31 k–231 k per segment we are far below the knee (the measured points are 64.7 B/entry at 275 K). Better: a segment sorted at freeze is **rank-ordered
within itself**, and `GlobalValueDictionaryRadix.append` already takes `buildForwardIndex=false` for that
case, because *"which id holds this value is a binary search over the reverse index, which is already
sorted by value"*. So the forward radix — the whole cost — disappears.

## Two design risks, checked against the code

**1. A frozen segment dictionary needs NO persisted forward index — which removes the whole 1,650 B/entry
cost, not merely shrinks it.** `GlobalStringDictionaries` splits the two directions: `idOf` (value→id) is
called from `StringRegion.resolveGlobalIds`, i.e. while ENCODING a page, and there are two
implementations for the two sides (`TrieLaneWriteDictionaries`, `TrieLaneDictionaries`). Value→id is
therefore only ever needed while a segment is still OPEN, where the writer's resident
`GlobalValueDictionaryProbeFront` answers it. After freeze, readers need only id→value — the reverse
index. So a segment dictionary can mint ids in arrival order and persist no forward structure at all,
and rank order becomes a nice-to-have (it is not read by queries, which use the projection's dictionary)
rather than a correctness requirement.

**2. The per-page anchor already exists.** `GlobalStringDictionaries` is bound per page and per tag —
`bindForPage` with the anchor the page recorded, plus `dictionaryKey(tag)` and
`dictionaryEntryCount(tag)`, with a temporal-validity check that refuses a dictionary whose live entry
count is below the recorded one. Different pages naming DIFFERENT dictionaries is what this interface
already expresses, so pointing each segment's pages at their own dictionary needs no page-format change.

## Where a segment boundary comes from

The page trie already supplies one: `Constants.INP_REFERENCE_COUNT = 1024`, so an indirect page groups
1024 leaves. At ~9.7 ClickBench rows per leaf that is ≈ 9,900 rows — which the 100M curve prices at
**63.6 % capture (−6.80 GB)**. One level up (1024² leaves ≈ 10.2M rows) sits at or above the 1 M-row
point's **88.8 % (−9.49 GB)**, with ~1.5M distinct URLs ≈ 250 MB of values per segment — still sortable,
and the size at which `ExternalDistinctValues`' spill path stops being insurance and starts being used.

So the boundary is a choice between the trie's own two levels, and the curve says the upper one is worth
≈ 2.7 GB more. A segment need not align to the trie at all (an explicit leaf count would do), but aligning
means the anchor can be derived rather than stored.

## CORRECTION: ids are minted at page ENCODE, so the dictionary is arrival-ordered — and simpler for it

`StringRegion.resolveGlobalIds` is called from `encodeInto`, i.e. when a PAGE IS SERIALIZED. Pages of a
segment are serialized before the segment closes, so ids must exist at that moment: **freeze-time sorting
is impossible and ids are minted in ARRIVAL ORDER** as values are first seen.

That is not a compromise, it is a simplification, because of risk 1 above: after freeze only id→value is
read, and id→value on an arrival-ordered dictionary is a plain indexed lookup — no sort, no binary
search, no forward index. So the document-side segment dictionary needs **neither rank order nor
{@code ExternalDistinctValues}**: the writer's resident `GlobalValueDictionaryProbeFront` already IS the
segment's distinct set, and a value appends to the segment's dictionary on first sight.

**So the change is a SCOPING change to machinery that already exists** — bound the existing streaming
dictionary to a segment instead of to the whole load — not new machinery. And the 1,650 B/entry that
condemned the streaming path is the persisted forward radix, which a segment dictionary does not need:
`GlobalValueDictionaryRadix.append(..., buildForwardIndex=false)` applies for the opposite reason to the
rank pass's (nothing probes value→id after freeze, rather than the reverse index being sorted).

`ExternalDistinctValues` and the streaming `PrePassDictionaryBuilder` overload (`de755c82b`) remain the
right tools for the PROJECTION side, where rank order IS read (id-order zone pruning, lever E).

## The hazard the implementation must solve first: async flush versus segment identity

Ids are minted at page ENCODE (above), and pages are encoded by the async flush pool
(`sirix.asyncFlush.parallelism`). So a page belonging to segment N can be encoded AFTER the builder has
moved on to segment N+1. A resolver that simply answers "the current segment's dictionary" would then
mint that page's ids in N+1 and record N+1's anchor on a page whose neighbours point at N — plausible
and wrong, which is the failure mode `GlobalStringDictionaries`' entry-count validity check exists to
catch but cannot: both dictionaries are live and both are large enough.

The resolver must therefore key on the PAGE, not on builder state: a page's segment is derivable from
its record page key (`recordPageKey / INP_REFERENCE_COUNT` for the 1024-leaf boundary), so the write-side
resolver can select the right segment's dictionary per page rather than per moment. A segment's
dictionary consequently cannot be closed when the builder passes the boundary — only when every page of
that segment has been encoded, which is a flush-completion condition, not a row-count one.

This is the same shape as the aliasing defect that cost this campaign a night (`4a1f28d8b`: a thread-local
scratch shared between a serializer and a decoder re-entered from inside it). Anything the encoder reads
that is "the current X" is suspect when the encoder runs on a pool.

## CORRECTION 2: an arrival-ordered dictionary still needs a forward index — the FORMAT requires it

The claim above that a segment dictionary needs no persisted forward index is wrong as the format
stands, and the reason is not the read path but a header invariant. `ValueDictionaryHeaderNode:128`:

```java
if (forwardRootKey == 0 && entryCount != 0 && orderedPrefixCount != entryCount) {
  throw new IllegalArgumentException("invalid value dictionary header");
}
```

"No forward root" is legal exactly when the dictionary is FULLY ordered. An arrival-ordered segment
dictionary has `orderedPrefixCount == 0`, so it must carry a forward radix whatever anyone intends to
read. The reasoning that nothing probes value→id after a freeze still holds — it just does not buy the
saving, because the check is structural rather than usage-based.

**Priced, and affordable only because of segment scope.** At ~231 k entries per segment the forward
radix is ~65 B/entry (the measured D = 275 K point, before the superlinear regime) ≈ 15 MB per segment,
≈ 1.5 GB across a 100M load. So the lever is **≈ −8 GB net**, not −9.49.

**Two ways forward. The default taken here is the first, because it changes no format and can be
measured today:**

1. **Pay it.** No format change, works now, ≈ −8 GB. Start here so the first end-to-end measurement is
   not confounded by a format change.
2. **Relax the invariant** — permit `forwardRootKey == 0` with `orderedPrefixCount == 0` for a
   dictionary declared decode-only. Sound (nothing probes value→id on the document side) and permitted
   by the standing "no version machinery, formats may change" ruling, worth ≈ 1.5 GB — but it weakens a
   check that today catches a genuinely unreadable dictionary, so it needs its own witness and should
   follow the measurement, not precede it.

## BUILT so far (2026-09-03), and the one gap left

Six increments are committed, each with witnesses a mutant kills (36 witnesses, 17/17 mutants):

| commit | piece |
|---|---|
| `de755c82b` | `ExternalDistinctValues` — bounded distinct collection (the PROJECTION side needs it; the document side does not, see correction 1) |
| `8374de233` | `SegmentScopedDictionaries` — mint per segment, per-PAGE views (the encode hazard) |
| `a5ed6bdde` | `SegmentDictionaryAnchors` + `SegmentScopedReadDictionaries` — resolve a segment-anchored page |
| `387d361f1` | `SegmentSealController` — seal when the last page is ENCODED (the sealing hazard) |
| `c48037bc3` | `installDocumentStringDictionaryFactory` — a page's resolver chosen per page |
| `80a9cbc99` | `installDocumentPageEncodedListener` — the encode-completion signal |
| `3d29efd17` | `SegmentDictionaryFlusher` — write a sealed segment mid-load, with the id-alignment guard |

Both writer seams are **byte-identical** on `gate1mT.sh converted16k` (537,338,008 B), which is the
property a change to the commit path has to prove.

**The gap: the anchor table is not persisted.** `SegmentDictionaryAnchors` is in-memory, and a reader
in a later transaction needs `(segment, column) -> header key`. The lane's own anchors live in
`ProjectionIndexMetadata.valueDictionaryHeaderKeys()` — a `long[]` indexed by COLUMN, read back through
`ProjectionIndexHOTStorage.readBlob(reader, indexDef.getID(), 0L)` in
`NodeStorageEngineReader.collectTrieLaneAnchors`. Segment anchors need the same thing indexed by
`(segment, column)`: at 100M with 1024-leaf segments that is 10,000 x 4 longs plus their sealed counts,
≈ 640 KB — affordable, but a metadata format addition (permitted: no version machinery).

**The format constraint that shapes step 1, found before starting it:** `ProjectionIndexMetadata.parse`
ends with a hard `if (pos[0] != payload.length) throw` — the blob rejects ANY trailing bytes. A segment
section therefore cannot be appended and skipped by older readers; they throw. Permitted by the standing
"no version machinery" ruling, but it dictates the shape: parse must treat the new section as OPTIONAL
WHEN ABSENT so today's databases stay readable, while writing it makes blobs unreadable by older code.
The existing dictionary section is the template — a `short` count, then per entry `(column short,
header key long)`, sparse and self-describing.

**Remaining, in order:**
1. Extend `ProjectionIndexMetadata` with the segment anchor/count table (per the constraint above);
   write it at load end, read it in the reader's segment-mode sibling of `collectTrieLaneAnchors`.
2. `bindSegmentLane` in `ProjectionBulkLoad`, gated (default off): construct the three objects, install
   the factory (`adopted` + `viewFor`) and the encoded listener, and at load end `drain()` ->
   `SegmentDictionaryFlusher.write` -> `anchors.seal`. Seal at DRAIN first — sealing from the encoded
   listener would run inside the sequential append pass, on the writer's critical section.
3. A `gate1mT.sh` arm with the switch on: a 1M number in ~40 s, no rebuild, nothing destructive. The
   curve predicts it lands between the per-leaf baseline (612.8 MB) and the trie lane's 537.3 MB.

## VERSIONING TYPES and INCREMENTAL BUILDS — measured, and one answer is NO

Asked to confirm the lane works under every versioning type and supports incremental building. Measured
at 1M rather than argued.

**Versioning types: YES for load + read.** `gate1mT.sh seg16kbig` under each of the four, then
`BadSlotCensus 0 1024000`:

| versioning | database | OBJECT_NAMED_STRING |
|---|---|---|
| FULL | 1,174,866,907 | 309,129 bad=0 |
| DIFFERENTIAL | 1,166,478,315 | 309,129 bad=0 |
| INCREMENTAL | 1,166,478,313 | 309,129 bad=0 |
| SLIDING_SNAPSHOT | 1,174,866,931 | 309,129 bad=0 |

Caveat that keeps this honest: a single-revision load writes WHOLE pages, so the versioned COMBINE — which
only runs once a page has fragments — is not exercised by these four runs. What they prove is that the
lane does not break the write or read path under any versioning type, not that the combine is safe.

**Incremental building: NO — and not for the trie lane either.** `UpdateThenReadProbe` (a second
revision over the loaded database, then a full read):

| database | pre-pass | lane | second revision |
|---|---|---|---|
| `base` | no | no | **works** (20 changed, 0 unreadable, 0 stale) |
| `segbase` | no | no | **works** |
| `prebuiltnochunk` | **yes** | no | **works** |
| `converted16k` | yes | **trie lane** | FAILS |
| `seg16kbig` | no | **segment lane** | FAILS |

The pre-pass is exonerated; the LANE is the discriminator. Any database whose document pages store
dictionary ids refuses incremental maintenance with `Projection index 0 incremental maintenance found
inconsistent persistent units` — `ProjectionIndexChangeListener:1842`, raised when `applyIncremental`
returns false. **This is a PRE-EXISTING limitation of the shipped trie lane that segment dictionaries
inherit, not one they introduce.**

**Located precisely, in two instrumentation cycles (both reverted).** `applyIncremental` fails at
`applyColumnOnlyUpdate` (`:2382`), and inside it at **`:2862` — `extractInto(extractor, rtx, recordKey,
…)`**, identically for BOTH lanes. That is the record EXTRACTION: the maintenance cannot read the
record's values through the write transaction's reader, and it reports that as a `false` rather than an
exception, which is why the symptom surfaced as "inconsistent persistent units" far from its cause.

So the blocker is the read side after all, and it is the one the trie lane's own parking note names:
`lazyEligible = trxIntentLog == null && …`. The `trxIntentLog == null` clause is a CORRECTNESS rule — a
write transaction's CoW hands the page to a combine that reads every slot — so a write transaction never
takes the lazy route, and a converted page expanded eagerly cannot resolve its ids. Incremental
maintenance runs inside exactly such a transaction.

**This also connects the two halves of the question**: making the lane incremental and making it safe
under a versioned COMBINE are the same problem, because the combine is what the eager path exists for.

Two things must be true before either lane can claim incremental support, and both are design work:
1. **The maintenance path must be able to READ a converted page.** `lazyEligible` requires
   `trxIntentLog == null`, so a WRITE transaction never takes the lazy route, and a converted page
   expanded eagerly is refused by design.
2. **It must be able to MINT into a sealed segment.** `SegmentSealController.adopted` throws there, on
   purpose: the dictionary is written and a value minted afterwards could never resolve. A new revision
   therefore needs its own segment (a natural fit — a revision's new pages are new pages) rather than a
   reopened one.

**Small slots/records: already satisfied.** The lane's unit is the record page, whatever its size — a
leaf is ~9.7 ClickBench rows here — and the segment boundary is configurable
(`sirix.projection.segmentDict.leaves`, a power of two). Nothing in the design needs large pages; the
measured trade is only that smaller segments capture less dedup while paying one dictionary each.

## Shape of the implementation

1. **Mint** ids in arrival order against the segment's resident `GlobalValueDictionaryProbeFront`,
   appending each new value to the segment's dictionary — the existing streaming path, scoped.
2. **Close** the segment at the boundary: flush its final generation and record its anchor in
   `SegmentDictionaryAnchors`. A forward index IS written (correction 2); the mid-load shape to copy is
   `ProjectionIndexBuilder.flushStreamingDictionaryGeneration`, which already appends a generation bound
   to the LOAD's writer without committing — the primitive `PrePassDictionaryBuilder` cannot provide,
   because it commits per generation.
3. **Anchor**: each record page names its segment's dictionary instead of the resource-wide one.
   `KeyValueLeafPage.globalStringDictionaries` is the existing injection point, and
   `PageKind.serializeKeyValueLeafPage` already carries the per-page tag the lane uses.
4. **Read**: unchanged in shape — the lazy route already resolves a page's ids through the resolver it
   is given (`6909f0ccf`), and the decode-side scratch aliasing is fixed (`4a1f28d8b`).

## What it gives up, and what it removes

Gives up the dedup only a corpus-wide dictionary reaches — **11.2 % at 1 M-row segments, 21.3 % at
100 k** (measured at 100M below). Removes: the pre-pass, the second
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
