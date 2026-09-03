# Roadmap: 69.5 GB → ~30 GB at 100M rows

Written 2026-08-31, on the day the first fresh build came out smaller than its baseline
(`P2_SEGMENT1_BRIEF.md`, Deliverable 1: −6.6 % at 1M, 43/43 byte-identical). The user's target: **at or below
30 GB for the 100M ClickBench database** (currently 69.51 GB = 695 B/row; leaderboard median 15.3 GB, Q3 31.9,
DuckDB 20.5 — 30 GB lands just inside the third quartile, NOT mid-table).

## The stack

| # | lever | Δ GB | cumulative | status / gate |
|---|---|---|---|---|
| A | P2 fresh-build, four fat columns: per-leaf DICT/BLOOM/DICT_HASHES → id lanes + ONE rank-ordered dictionary | −5.6 (gate-measured) | 63.9 | mechanism PROVEN at 1M; blocked on Deliverable 2 (O(D)-per-execution family), then A1 + length table for SearchPhrase/Referer |
| B | Trie lane (P2 segment 5): the trie's string region (17.3 GB) stores ids into the SAME dictionary | −14 to −15 (derived) | ~49 | never built; needs a per-transaction resolver; GATED on point-read + reconstruction latency |
| C | Overflow compression on the post-P2 remainder | −3 ± 1 (levers overlap, measured 2026-08-31) | ~46 | needs the q16/q17 hot regression solved (decode-through-page-cache); native-decode fix makes this plausible |
| D | Column-major leaves: per-record directory/name-key/ordinal/framing over ~12 GB of non-string leaf bytes | −8 to −12 (derived) | ~34–38 | never built; own campaign |
| E | Bigger leaves (2^17 slots), net of overlap with D | −2 to −4 (measured, overlap-adjusted) | **~30–36** | screened earlier |

**Central estimate with everything on the table: ~33–36 GB. The stack touches 30 only if B and D land near
their derived maxima** — and this campaign's record is that derived numbers SHRINK when measured (the P2 gate
was wrong 1.6×; the streaming dictionary was 27× worse than its estimate). Plan on ~35; treat 30 as the
stretch of the current ledger.

## What gets BELOW 30 with margin

**Route 1 of `STORAGE_TO_MID_TABLE.md`: fewer records per row.** A ClickBench row is ~106 fused records, each
paying a directory entry, name-key, ordinal, zone-map participation and body framing. Storing a JSON object's
scalar fields as ONE record divides that overhead by ~100: worth a further ~−8 to −15 GB beyond D, and it is
the only lever that attacks the multiplicand. It is a data-model campaign (node model, cursor API, versioning
granularity, every index) — schedule it as one, after A+B are measured at 100M.

## Order of work (GB per effort, honoring every standing gate)

1. **Deliverable 2** (in flight): derived structures once per (headerKey, revision); verdict cache; then
   Referer converted. Gate: the per-query clause, hot AND cold.
2. **100M fresh build** with three columns → decides ≤27 B/row and measures A at scale. ~46 min + ~65 GB disk.
3. **B, the trie lane** — biggest single lever, prerequisite (the dictionary) now exists. Gate: point-read and
   reconstruction latency, per-query leg.
4. **C** re-verdicted post-P2 (the remaining class is different bytes with a different access pattern).
5. **D/E** as one leaf-overhead campaign.
6. **Route 1** if the stack stalls above the target — and for mid-table (15.3) it is required regardless.

Every stage keeps the standing rules: no per-query regression hot or cold, results byte-identical, written
bytes measured on disk (never derived, never a ratio, never a slope through two scales), `# served` and
`route=` on every A/B.

## Stage B sizing, measured (census 2026-08-31, 1M, `residual=0 B`)

The per-tag census (impl-ingest's instrument, `4db2bd1a5`) splits the raw string region — 268.5 MB at 1M —
as **91.7 % dictionaries / 4.4 % id lane / 3.9 % framing**, and by column:

| slice | share | disposition |
|---|---|---|
| Title + URL + Referer (converted) | **63.0 %** | ids into the EXISTING dictionaries — the core of stage B |
| SearchPhrase (planned, behind segment-2 A1) | 0.9 % | joins when its arms exist |
| **OriginalURL — NOT in any plan** | **8.5 %** | 10× SearchPhrase; URL-shaped; must be priced by the election rule (per-leaf removed − dictionary added) and screened for serving arms — likely zero-latency-risk if no query touches it |
| **temporal-as-strings** (LocalEventTime, EventTime, ClientEventTime, EventDate) | **18.1 %** | needs NO dictionary — wants a temporal encoding that round-trips the fixed "YYYY-MM-DD[ HH:MM:SS]" format bijectively; a NEW sub-lever (B-t), not in the stated trie-lane design |
| everything else | ~8.5 % | tail |

**Id-lane arithmetic (2):** page-wide width 11.68 MB vs per-tag FOR 4.87 MB = **58.3 % lane saving**; the
three converted columns' 169.1 MB of in-trie dictionary bytes become **~909 KB** of FOR-packed lane — a
99.5 % cut of that slice at 1M, which is the measured shape of the −15 GB lever.
**(3) the point-read tax: 417 ns random / 75 ns sequential** (from 24.0 µs / 140 ns pre-residency).

Premise correction: the earlier "~1.52 B/record string region" figure was EventDate's; the fat columns run
**47–63 B/value**, 30–40× larger — that is what the lever replaces.

### CORRECTION to the stage-B sizing above: those figures are RAW — written = raw × 0.644

The census addendum above sizes slices in raw bytes despite the instrument printing the scaling warning —
the exact staged-vs-encoded error this campaign already made once, made by the lead this time. Reproduced
independently at `039e2f8ba` (two runs, different commits, identical to the digit, `residual=0` both):
**raw→written ratio 0.644.** Written bytes at 1M:

| slice | raw MB | **WRITTEN MB** |
|---|---|---|
| three converted columns | 169.1 | **108.9** |
| OriginalURL | 22.8 | **14.7** |
| temporal (3 timestamps; EventDate EXCLUDED, see below) | 47.2 | **30.4** |
| whole region | 268.5 | **172.9** |
| FOR id-lane re-pack | 6.81 | **4.39** — 2.5 % of the region; real, but NOT worth stage-B design effort |

**Consequence for the stack table: stage B's −14/−15 GB must be re-derived from WRITTEN region bytes before
the stage is committed.** If the 100M "string region 17.3 GB" figure in `STORAGE_TO_MID_TABLE.md` was itself
raw-derived, the honest stage-B value is nearer **−10 GB** and the stack's central estimate moves ~+4 GB.
Re-derive at the 100M build (the census runs there anyway); do not quote −15 until then.

**Also measured: per-leaf dictionaries barely deduplicate on the fat columns** (values per entry: URL 1.64,
Referer 1.86, Title 2.24 at ~9.7 rows/leaf) — so the resource-wide dictionary's prize is almost entirely
CROSS-leaf dedup and the election rule's "per-leaf removed" term is nearly "all of it". **EventDate is
already optimal** (exactly one 1 B entry per leaf — every row in a leaf shares a date) and is excluded from
the temporal sub-lever, which is therefore the three timestamp columns (17.6 %), not four. Caveat pinned:
the census's `localDictEntries` is the SUM of per-leaf sizes, never a resource-wide distinct count — global
cardinality still comes from the HLL pass.

### Correction to the correction: both region figures are WRITTEN — and the measured 100M value is 19.31 GB

Traced to source (read-only): `P2_GLOBAL_DICTIONARY_DESIGN.md:149` records the 100M string region as
**written 19,313,068,264 B (193.1 B/row)** with raw 29.63 GB and LZ77 **0.652** kept beside it, cited to the
load log; and `STORAGE_TO_MID_TABLE.md`'s 17.3 GB is the 1M WRITTEN figure (1.52 B/record × 113.6 — the
census independently reproduces 1.522 B/record). **So the raw-derivation fear above does not apply; the
−15 GB does not need re-deriving on that ground.** The real error is smaller and favourable: **17.3 was a 1M
measurement ×100 where 19.31 is the measured 100M value — the extrapolation was 11.6 % LOW**, because the
per-row cost genuinely grows with scale (173 → 193.1 B/row). The no-slope-through-two-scales rule is
vindicated by its own violation. Stage B quotes **19.31 GB measured** and gains ~2 GB of headroom.

**What remains 1M-derived and is confirmed for free at the 100M build:** the per-column SHARES (63.0 % /
8.5 % / 17.6 %) — the census instrument rides the build, so the 100M split arrives with the run; the total
no longer waits on it. Cross-validation banked: the load log's 0.652 region ratio against the census's
0.644 at 1M — two instruments sharing no code, two scales, 1.2 % apart — is the census's strongest witness
yet.

## Stage 2 status after the 2026-08-31→09-01 night session

**Banked, measured:** the 100M three-column DOCUMENT is **52,152,369,152 B, reproduced byte-for-byte across
two independent 37-minute loads** (baseline WITH its projection: 69.63 GB); the resource-wide dictionaries
cost **2.91 GB for 7.32 GB of distinct values (0.40)**; the 100M census closed every 1M-derived share
(converted columns 59.0 %, OriginalURL 9.7 % and growing, temporal 19.8 % and growing, FOR lane −60.0 %);
per-leaf duplication measured (URL 3.65×, Title 6.5×, Referer 2.9×); the cold sweep ratio measured at 0.184.
The 1M latency gate: **zero stable regressions, cold and hot, nine pairs.**

**NOT measured:** the finished 100M storage number (the projection build was killed at 58 % by the harness —
the sandbox reaps even `setsid` process groups — and its relaunch could not finish before the deadline) and
the 100M query leg. The ~64 GB completion estimate is a one-scale extrapolation, labelled as such. The
current database carries **~4.05 GB of orphaned bytes** from the killed walk; whoever finishes must subtract
(credible via measured determinism) or reload.

**Next session, in order:** 1) finish/redo the projection build and take the storage number (raw + adjusted,
or pristine via one reload); 2) the 100M query leg, two interleaved pairs, baseline re-legged on the same
build; 3) then the roadmap resumes at the trie lane, with OriginalURL's screen and the temporal encoding as
the census-mandated additions. Handoff: end of `P2_SEGMENT1_BRIEF.md` (41ac1967c).

## Stage B priced in WRITTEN bytes at 100M (census on clean data, residual=0, ratio 0.652 triple-validated)

| slice | written GB at 100M |
|---|---|
| whole string region | **19.32** (matches the design doc's 19.31 measured independently) |
| three converted columns | **11.40** — the dictionary-lane core of stage B |
| OriginalURL | **1.87** — the census-mandated addition |
| temporal ×4 | **3.82** — the encoding sub-lever (EventDate excluded from effort, included in bytes) |
| FOR id-lane re-pack | 0.448 (2.3 %) — confirmed at scale: not worth design effort |

So stage B's honest decomposition: **~−11 GB** for the three converted columns' trie bytes → ids (minus a
small lane), **+~−1.8 GB** OriginalURL if its screen passes, **+~−3.5 GB** temporal encoding — the earlier
single "−14/−15" is superseded by this three-part, measured form. **And the mechanism strengthens at scale:
per-leaf dedup FALLS with corpus size** (URL 1.64 → 1.47 values/entry; temporal → 1.05, within 5 % of one
entry per value), so the cross-leaf prize is larger than the 1M numbers implied and the election rule's
"per-leaf removed" term is near-total.

Also recorded from the same close: **a negative result must state the scope searched** ("no match under X",
never a bare "it does not exist"), and **an instrument built from the assumption under test cannot falsify
it** — a waiter watching the wrong directory corroborates the wrong-directory assumption with silence.


## The temporal sub-lever, scoped (2026-09-03)

The only MEASURED route left to ~50 GB at 100M. The three timestamp columns plus EventDate are 3.82 GB
of written string region; the projection already stores them as epochs (`ClickBenchProjection.
projectionType` maps DATE/DATETIME to `date`/`timestamp`), but the DOCUMENT record pages still hold the
text, and `NodeKind` has no temporal kind.

**It does NOT need a new node kind or a new region encoding.** The extension point is the per-TAG lane
that already exists inside the region: the trie lane marks a tag with `globalTag[r]` and stores packed
ids plus a per-tag `globalEntryCount` / `globalDictionaryKey` instead of bytes
(`StringRegion.resolveGlobalIds`, `encodeInto`). A TEMPORAL lane is the sibling of that: a per-tag flag
saying "every value of this tag is the canonical `YYYY-MM-DD[THH:MM:SS]`", the values stored as packed
epochs (bit-packed deltas within a page, which is where the real win is — the values inside one leaf are
seconds apart), and the text reconstructed on read.

**Why it is a better lever than the dictionary lanes, not merely another one:**
- **No dictionary, so no anchor, no sealing, no closed corpus.** A value either matches the canonical
  shape or the tag keeps its bytes — decided per page, per tag, with no cross-page state at all.
- **Incremental by construction.** Nothing to seal and nothing to carry forward in metadata, so none of
  the machinery that made the dictionary lanes hard to update applies.
- **Every versioning type for free**, for the same reason: a fragment's tag is self-describing.

**Sizing:** 3 columns x 100M values at ~19 B of text against a packed epoch is the 3.82 GB the census
measured; delta-packing within a leaf should beat a flat 8 B/value comfortably. 52.49 - ~3.5 = **~49 GB**,
which is the target.

**Where to start:** `StringRegion.encodeInto` (the per-tag decision, beside `resolveGlobalIds`) and the
matching parse. The trie lane's density assertion is the model for the guard: refuse loudly rather than
write a value that reads back plausibly wrong.
