# Storage size and query speed — one plan (ClickBench 100M, projection-index track)

Status: **DRAFT 2 — 2026-08-30 14:10.** Draft 1's premise ("numeric projection columns cost 8 B/row on disk")
was a misreading of a heap-residency projection; the on-disk measurement below replaces it. Two measurements
are still pending (▢). Scope per the user's direction: the query path stays on the projection index in this
branch; the base-store/PAX-per-path direction is parked (but see §2 — it decides whether M2 is reachable).
Layout changes are made in place (V0, rebuild) — no format-version machinery.

## 1. Where the bytes are (measured)

**100M:** `sirix.data` = 131.9 GB = **1,319 B/row** (trie + projection + global-dictionary pages + carriers, one file).

**1M (`storage1m`, `hits-1m.json.gz`, 977 row groups):**

| component | bytes/row | how measured | notes |
|---|---|---|---|
| node trie (`KeyValueLeafPage`) | **~1,089** | writer profile (105,702 leaf writes ≈ 2 % rewrites) | ~105 field records per row at **~10.4 B each**; hashes NONE, DeweyIDs off, versioning FULL, regions LZ77 inside the page |
| projection segments, on disk | **109.6** | `ProjDiskDump` (descriptor segment lengths: body + dict + bloom + keys) | numerics FOR bit-packed: `IsRefresh` 0.16, `IsLink` 0.09, `CounterID` 0.03, `RegionID` 2.0, `ResolutionWidth` 1.4, `UserID` 6.9; the three 64-bit hashes 8.03 each (incompressible); `Title` 21.9 and `Referer` 24.7 (per-leaf FSST dictionaries + blooms); `SearchPhrase` 2.2; `URL` 2.1 and `EventTime` 2.1 as **global** codes (their dictionaries live in `GlobalValueDictionary` pages, not in this number; at 100M the promotion budget declines them and they cost ~20-25 B/row each as per-leaf dictionaries); **keys chain 12.8 B/row** |
| everything else in the 1.86 GB file | ~660 (written) | writer profile `OverflowPage` 740 minus the live 110 | superseded row-group versions from the incremental build, global-dictionary pages, carriers for the ~7 % of fused records over the 512-byte cap |

So at 100M the projection is on the order of 150–250 B/row and **the node trie is ~85 % of the file.**
Reference (`data_size`, c6a.4xlarge, all 105 columns): **Umbra 83 B/row**, CedarDB 85, ClickHouse 153, DuckDB 205.

▢ Pending: the inside-page split of a trie leaf (`-Dsirix.pageSectionDiag=true`: header/bitmap, compact
directory + template pool, heap, regions per kind, overlong entries, sidecar) and `ProjDiskDump` on the 100M DB.

## 2. Targets — and the one decision they hinge on

| milestone | 100M size | B/row | trie must be | reachable in the projection-index track? |
|---|---|---|---|---|
| M1 "below half" | ≤ 55 GB | ≤ 550 | ≤ 400 B/row (≤ 3.8 B per field record) | yes — T1 (a)(b)(c) + P3 |
| M2 DuckDB-class | ≤ 25 GB | ≤ 250 | ≤ ~120 B/row (≈ 1.1 B per field) | **not as a node store**: a field record needs a slot, a name key and structure; ~1 B/field is a bit-packed per-path column, i.e. the parked direction (columnar value regions per path inside the leaf) |
| stretch | ≤ 15 GB | ≤ 150 | — | only with per-path regions + global dictionaries |

Decision for the user, later: M1 is the projection-index track; M2 needs the trie's *values* stored per
path (the leaf becomes a column block for its 1,024 records, with the record structure as a compact
template) — the same idea that was parked this morning, now with a number attached to it.

Speed targets ride on the same changes. Σ cold at 8 GB: 3,803 s pre-lever → 1,327 s this morning (964 s
without q19's interpreter time); the final leg is running now (q19 in context 0.08 s). P2 is expected to take
the remaining slow tier (q5 45 s, q20 23 s, q23–q26 58–117 s, q33/q34 ~24 s) down by an order of magnitude;
the target Σ cold is ≤ 250 s.

## 3. Levers

### T1 — Trie leaf compaction (the storage lever: 1,089 → ≤ 400 B/row)
Per field record today ≈ 10.4 B on disk. Where it goes is exactly what the pending section split answers;
the candidates, in the order they will most likely matter:
- **(a) Directory and templates.** The compact directory carries (kind, field count, heap offset, length)
  per slot; with every record of a ClickBench leaf built from the same handful of templates, the
  per-slot entry should collapse to a template id + a varint length (or nothing, when lengths are
  template-implied). Target: ≤ 1 B per slot.
- **(b) Values once, bit-packed.** Value elision (heap value dropped when its region holds it) exists —
  confirm it covers every kind ClickBench uses and every page (a leaf with a mixed template must not fall
  back to inline values). Then pack the NUMBER region with per-page FOR (min + width, like the projection
  body) instead of LZ77 over raw longs, and FSST the STRING region (the page already carries a symbol table
  id). Target: ≤ 2 B per numeric field, ≤ 0.5 B per flag.
- **(c) Structure.** Parent-key and right-sibling-key columns exist (`recordParentKeyColumn`); the
  remaining per-record structure (kind, name key) belongs in the template. Target: ≤ 0.5 B per record.
- **(d) The 512-byte fused cap.** ~7 % of records overflow into carriers (an extra page each, ~1–2 KB of
  framing); raise the cap (the directory needs offsets that fit) so they stay inline.
Witnesses: golden-byte pins of the page layout, the record read/write differentials, the region-only read
tests, then the 1M storage profile (leaf ≤ 400 B/row) and the 1M leg (row-path queries unchanged).
Risk: medium-high — the leaf serializer/deserializer are the most complex code in the store; each
sub-step ships with its own pin.

### P2 — Fat string columns: disk-resident, order-preserving global dictionaries (the speed lever; −30…50 B/row)
Today a global dictionary must be resident to be promoted, so at 100M `URL`/`Title`/`Referer`/`SearchPhrase`
fall back to per-leaf dictionaries that "barely dedupe" (codec javadoc) — 20–25 B/row each at 1M, more at
100M. Design: one dictionary per column built by the bulk load (sort the distinct values → **codes are
rank order**), entries stored as FSST-compressed blocks (e.g. 4 K entries) with an offset index; scans read
only the codes (bit-packed per leaf, ~2–4 B/row), never the entries; entries are decoded per winner or
streamed once for a predicate. Incremental appends take codes above the bulk range (not order-preserving);
sorts and extrema fall back to entry comparison only across that boundary.
What it buys: q23–q26 become numeric top-k over codes (no per-leaf dictionary decode, the whole sorted-scan
tier), q5 = a code bitmap, q20/q21/q28 evaluate `LIKE`/regex once over the dictionary then test codes,
q21/q22 `MIN(URL)` = min code, q35 groups by code, q13/q14/q16/q17 hash codes instead of dictionary entries.
Builds on `GlobalValueDictionary` (promotion, `ReadView`, `valueDictionaryHeaderKey`, the `STRING_GLOBAL`
kind and its kernels). New: disk residency of entries, the promotion budget no longer a gate, the sorted
build, the mixed-range fallback. Witnesses: the `GlobalValueDictionaryServing*` differentials extended to
every route the per-leaf kind serves today; dump byte-identity at 100M. Risk: high (an external sort of
~10⁸ strings per fat column inside the 2× rider budget; every `STRING_GLOBAL` route parity). Two review
rounds first, like P1/P2 this morning.

### P3 — Row-group framing: keys chain 12.8 → ~1 B/row, compact zone maps/fingerprints (−12 B/row)
Record keys are dense and strided by ~106 per row; delta-FOR per leaf is ~1 B/row. Cheap, codec-local.

### R1 — Residency management (speed and stability, no bytes)
The retained-fill ledger sits within bytes of the 2 GB budget after q0–q6 and never shrinks: 594 Full GCs in
this morning's leg and the q14/q17 cold-try variance are this knife-edge. Add LRU eviction of retained
fills when a residency decision needs headroom; drive every budget from `HeapHeadroom` (done today for the
pass budgets and the distinct ceiling).

### P4 — Row-group size 1,024 → 4,096/8,192 (measure at 1M/10M after T1/P2)
Per-leaf dictionaries dedupe better, fewer descriptors and segments, larger I/O units; coarser zone
pruning and larger decode windows per worker. A measurement, not a commitment.

### T2 — Per-section page compression as a fallback (body blob and region table independently)
`sirix.compression=lz4` exists but is whole-page; the region-only read needs the body skippable by its
length prefix, so compression stays per section. Low priority once T1(b) packs the regions.

Dropped from draft 1: "P1 numeric bit-packing" — already implemented and effective on disk.

## 4. Order of work and gates
1. **Measure (today):** ▢ section split at 1M, ▢ `ProjDiskDump` at 100M; distinct cardinalities of the four
   fat columns at 100M — known from the dumps: **SearchPhrase 6,019,103 distinct of 99,997,497 rows (6 %)**,
   UserID 17,630,976; ▢ URL / Title / Referer via count-distinct queries. At 6 % distinct a global
   SearchPhrase dictionary is ~6 M entries (tens of MB FSST'd) against 2.2 B/row of per-leaf cost today —
   P2's storage gain is in URL/Title/Referer (20–25 B/row each per-leaf), its speed gain in every fat column.
2. **T1(a)(d) then T1(b)(c)** — each with a layout pin, the record and region-only differentials, the 1M
   profile and leg; then one 100M rebuild (load 46 min) + the full leg: 43 dumps byte-identical, `sirix.data`,
   Σ cold. This is the M1 step.
3. **P3** alongside T1 (codec-local). **R1** alongside (query side only, no rebuild).
4. **P2** — design, two review rounds, build, serve, gates. This is the speed step.
5. **P4 / T2** by measurement. **M2** only after the per-path-regions decision.
Every step is general (no benchmark-only mechanism), witnessed with a mutation, rebuilt in place.

## 5. Risks and what would change the plan
- If the section split shows the heap (values) dominating rather than the directory, T1(b) moves first.
- P2's sorted build must not break the 2× rider budget on load; an external sort per fat column is the
  design cost. Order-preserving codes hold only for the bulk range — every sort/extremum path needs the
  mixed-range witness.
- The 100M trie number (▢) may differ from 1M (templates dedupe differently at scale); the targets are
  restated after the 100M measurement.
