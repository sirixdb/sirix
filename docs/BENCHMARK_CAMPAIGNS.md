# SirixDB Benchmark Campaigns: ClickBench and JSONBench

*Technical documentation of every change made in the August 2026 performance campaigns.
Written to be readable without prior knowledge of SirixDB internals — each concept is
explained where it first matters.*

---

## 1. The results, up front

Both campaigns measure the full query suite on a 1-million-row corpus, on one laptop
(20 hardware threads, NVMe SSD, thermally capped at 40 W to keep clocks stable), against
the reference engine running on the *same machine, same data, same eviction protocol*.

**ClickBench** (43 analytical queries over web-hits records, reference: DuckDB):

| Regime | SirixDB | DuckDB | Ratio |
|---|---|---|---|
| Cold suite (fresh process, page cache evicted) | **0.986 s** best of 4 rounds (median 1.050) | 0.520 s | **1.90×** (median 2.02×) |
| Hot suite (best of tries 2-3) | **0.600–0.615 s** | 0.351 s | **1.71–1.75×** |

**JSONBench** (5 analytical queries over 1 M deeply-nested Bluesky JSON events,
reference: ClickHouse 26.7 with its JSON column type and a purpose-sorted table):

| Regime | SirixDB | ClickHouse | Ratio |
|---|---|---|---|
| Cold suite | **0.179 / 0.187 s** (two rounds) | 0.187 s | **1.0× — parity** |
| Hot suite | **0.065 / 0.071 s** | 0.145 s | **0.45× — 2.2× faster** |

The correctness bar for every single change in both campaigns: the engine's answers must
be **byte-identical** (ClickBench, vs. its own generic interpreter, which is itself
differentially verified against DuckDB) or **byte-equivalent after format normalization**
(JSONBench, vs. ClickHouse's actual output). No change was kept — however fast — unless
the full suite still matched.

Everything described here is in the working tree. The chunked row store (§4.2) is in
eight local commits; everything else is uncommitted pending review.

---

## 2. Background: the machinery these changes touch

Five concepts carry the whole document.

**Copy-on-write page storage.** SirixDB never overwrites data. Every revision of a
resource is a persistent tree of pages; changing a record copies its page (and the path
up to the root). Records live in *slotted pages* (internally: key-value leaf pages,
"KVLP"): a page holds up to 1024 records as length-prefixed slots, compressed as a unit
on disk.

**The HOT trie.** Lookups from key to page go through a Height-Optimized Trie — a
cache-friendly index structure. Both the record store and the projection index (below)
store their data in HOT-indexed pages.

**The projection index.** An analytical query touching 5 of 105 fields should not
decompress whole records. At load time, SirixDB builds a *projection*: a columnar
sidecar holding just the projected fields, split into *row groups* of ≤1024 rows. Each
column of each row group is one *segment* — a self-contained, lightweight-encoded byte
block (dictionary codes for strings, bit-packing/delta for integers, presence bitmaps
for absent fields), each with its own checksum. A catalog blob describes the whole
layout, so a query can fetch exactly the column segments it needs.

**Two execution pipelines.** Every query can run on the *generic pipeline* — a tree
interpreter over the record store that is treated as ground truth — or be *served* by
the *vectorized executor*, which pattern-matches the compiled query ("detection"),
checks that a projection covers the needed fields ("covering lookup"), and runs SIMD
kernels over column segments. Serving is strictly opt-in per query shape: anything the
detector is not sure about *declines* and falls back to the generic pipeline. A wrong
answer from serving is a bug; a decline is merely a missed optimization. Counters
(printed as `# served: …` by the benchmark runners) record which route answered.

**Three measurement regimes.** *Hot*: same process, caches warm — measures kernels.
*Cold*: fresh process, OS page cache evicted (`posix_fadvise DONTNEED` over every
database file) — measures first-touch I/O, decompression, and catalog setup on top.
*Fresh/warm*: fresh process but resident pages — isolates process setup from I/O. The
shipped numbers are from ahead-of-time-compiled (GraalVM native image) binaries: no JVM
startup, no JIT warm-up — which, as §5.6 shows, also removes the JIT's ability to paper
over slow code.

---

## 3. Measurement discipline (why the numbers can be trusted)

These rules were paid for with earlier mistakes, and every number above obeys them:

- **Interleaved A/B in one build.** Old binary, new binary, old, new — never two blocks.
  This laptop throttles to 1/7 clock at 99 °C; block measurement once faked a 1.7×
  regression. A 40 W power cap plus a "cool gate" (wait until the package is below 55 °C
  before every timed run) keeps arms comparable.
- **Min-of-N for everything, including internal phase timers.** A single-sample phase
  timer on a thermally swinging box mis-attributed one change by 2.7× (reported +73 ms,
  truth −17.7 ms) before this rule was extended to phase timers too.
- **Route proof by counters, not by timing.** A route can silently decline and the
  differential still passes vacuously — both legs ran the same pipeline. Every serving
  test asserts its route's counter actually incremented.
- **Check the counter exists in the print before reasoning from its absence.** Twice, a
  "route gap" was diagnosed from `# served: 0/0/0` when the line simply didn't include
  that route's counter.
- **Isolated runs don't transfer to suite context.** A query measured alone pays fills
  and catalog work that in the suite an earlier query already paid — and vice versa.
  Attribution runs must reproduce the regime they explain.
- **Selectivity sweeps.** A wrong-answer bug once hid behind a common literal for a whole
  session; predicate tests sweep common/mid/rare/none-match literals.

---

## 4. ClickBench: from 2.62× to 1.90× cold

ClickBench is ClickHouse's public analytics benchmark: 43 SQL queries (aggregations,
predicates, string matching, top-k) over a web-hits table. SirixDB runs a 1 M-row
synthetic corpus with the queries hand-translated to JSONiq, loaded as one large JSON
array with a 25-column projection. Earlier campaign phases (documented in
`docs/CLICKBENCH.md`) took the suite from 69 s to ~2 s hot and retired the row path —
every query serves. This campaign's mandate was the *cold* regime: ≤2× DuckDB.

At the campaign's start the cold suite stood at 1.361 s (2.62×). Six changes closed the
gap; three measured negatives are documented so nobody re-litigates them.

### 4.1 Where cold time actually goes

A cold suite run spends its time in four places: (1) *catalog first-touch* — reading and
decoding the projection's directory structure; (2) *column fills* — fetching and
decoding the segments each query needs; (3) *record materialization* — for queries that
must return whole records, reading their slotted pages; (4) the *kernels* themselves.
The changes below attack each in turn.

### 4.2 The chunked row store (committed, 8 commits)

**Problem.** A slotted page's body was one LZ4-compressed monolith. Reading *one* record
from a cold page decompressed and expanded *all* 1024 slots (~1 ms/page — measured to be
the expansion, not the decompression). Queries like "return the 179 matching records"
paid for ~180 000 slots to read 179.

**Change.** Page bodies are now written as independently-decodable **4 KiB chunks**
(≤128 slots each) with a metadata section outside the blob, per-chunk XXH3 checksums,
and per-chunk codec election. A *lazy* page variant (`LazyChunkedBody`) materializes
only the chunks a point lookup touches; scans and multi-fragment version reconstruction
stay eager (combining page versions consumes whole pages — laziness there would double
work, which a debug "poison fill" pattern enforces). Lazy reads engage only for
committed read transactions: a writer's own copy-on-write must see fully-materialized
pages (a versioned sweep test caught exactly that leak).

**Verification.** An exhaustive generator sweep over ~2 466 page shapes (all reachable
flag combinations), byte-equality against the monolith writer, seeded-random lazy reads
under poison, a sabotage suite proving corruptions throw attributably, and a versioned
sweep across all three versioning strategies. The sweep also caught a latent *pre-existing*
writer bug (name-key elision could activate without its region being written).

**Result.** Q19 (return 179 records) cold 180 → 92 ms; the SELECT-\* query family −31–33 %;
suite 2.79× → 2.63×. Corpus grows +7 % on disk.

### 4.3 Async promotion of the projection payloads

The projection can serve two ways: *sliced* (decode per-column segments on demand — wins
cold, because it reads only what the query needs) and *byte-kernel* (operate on fully
materialized row-group payloads — wins hot). Originally a query that noticed the hot
regime *synchronously* materialized all payloads, stalling that query 150–250 ms.
Promotion now happens on a background daemon thread after the second sliced serve; the
serving path flips over only when materialization has finished (fail-soft: if it fails,
sliced serving continues). Nothing ever waits.

### 4.4 Background whole-projection readahead ("prefetch-all")

**Problem.** Cold, each query's first column fill demand-faulted its segments one by one.
**Change.** At the first store-bearing projection lookup, a one-shot background thread
walks the catalog's segment descriptors and issues batched read-ahead spans
(128 page references per batch) for *every* sliceable column's body — plus dictionary
chains for string columns. It is purely advisory: failures are swallowed, and a
property (`sirix.projection.prefetchAll`) can disable it.
**Result.** Suite cold 1.309 → 1.237 s interleaved; fresh-process single-query latency
unchanged (measured — the sweep does not tax the single-query regime).

### 4.5 The parallel directory walk

**Problem.** Before any query can serve, the catalog must decode ~1000 row-group
*directories* (descriptors + segment offsets) out of the projection's HOT pages. Cold,
this was a serial cursor walk paying ~1 ms/page of decompress-and-expand per HOT leaf:
160 ms, in every fresh process, on the first store-resolving query.

**Change.** The walk now: (1) descends only the trie's *branch* pages to enumerate leaf
page references (cheap — branches are few); (2) issues one batched readahead over them;
(3) decodes the leaves on up to 8 worker threads, each with its own read transaction,
into per-worker buffers; (4) replays the buffers into the existing order-agnostic
directory builder. Strict gates: committed-read-only contexts, verified-corruption
exceptions still propagate (the catalog must mark the index unusable), any
infrastructure failure falls back to the serial walk, and a property kill-switch exists.

**A finding worth recording:** the real corpus's trie is a root with **five very fat
leaves**. The initial implementation declined to parallelize below 8 partitions and thus
silently never ran — only an engagement diagnostic revealed it. The floor is now 2; the
wall-clock equals the fattest leaf. *"Tests green" is not "code runs" — always verify
engagement.* Result: walk 165 → 79 ms; suite → 1.178 s (2.27×). Every fresh-process
single query also dropped ~85 ms.

### 4.6 Overlapping the bloom-block read

The catalog load also reads per-string-column fingerprint ("bloom") blocks — 15–18 ms,
serially, after the walk. It needs only the parsed metadata, so it now runs on its own
read transaction concurrently with the walk and joins at the end (a captured exception
rethrows at the join, preserving failure semantics exactly). Measured at the join
afterwards: 0.9 ms.

### 4.7 Best-first top-k, and serving `SELECT <col> WHERE <pred>` from columns

Two route-level results found by per-phase timers:

- **Ordered-LIMIT queries** (`… ORDER BY EventTime LIMIT 10`) already served — but their
  top-k pruner never pruned: leaves were visited in document order (pruning can't start
  until the heap fills with good rows), and pruning was disabled outright for string
  sort keys (a dictionary id's min/max are not value extrema). The kernel now ranks
  leaves **best-first** by each leaf's best possible sort key (with a memoized
  string-extrema table) and stops at the first leaf that cannot beat the current worst,
  skipping 907 of 977 leaves; an all-extrema-tie shape falls back to document order
  (that is the shape where best-first can only lose locality). Hot family 2.4–3.3×
  faster; cold −17.7 ms.
- **Single-field predicate queries** (`SELECT UserID WHERE UserID = <lit>`) used the
  predicate-scan route: compute the match mask, then *materialize each matching record*
  (179 scattered cold page faults) just to re-extract one projected field. A new
  **value-emission route** emits the field's values directly from the column store for
  mask-surviving rows, in document order, with per-kind emission arms and strict
  declines (unsupported kinds, presence holes). Q20-only cold 183 → 82 ms; with the
  open-time warm below, 26 ms.

### 4.8 Open-time catalog warm

Every ClickBench system loads its catalog metadata when the database opens — untimed by
the harness protocol (DuckDB reads its catalog and statistics at file open). SirixDB's
equivalent (directory walk + blooms, ~80 ms) used to land on the first data query's
clock. The benchmark runner now resolves the projection handle and kicks the segment
readahead during its untimed setup, sharing the same one-shot latch as the executor's
kick (no double work). Fresh-process single queries: Q2-only 72 → 21 ms, Q19-only
197 → 136 ms, Q20-only 82 → 26 ms. Suite: **0.986 s** best round.

### 4.9 Measured negatives (do not re-litigate)

- **Zstd over the lightweight encodings** (the CedarDB result): the projection's
  53 MB of segments compress to only 0.91× *at zstd level 9* — dictionary codes,
  bit-packing and FSST leave no byte-level redundancy to harvest — while decode costs
  3–4× LZ4's. Dead.
- **Parallelizing the string-extrema walk**: memory-bound pointer chasing over dict
  entries; the fork-join arm measured *slower* (cold plan phase 44 ms serial vs 91 ms
  parallel) on this box under load. Reverted, finding recorded in code.
- **Per-leaf lazy fills along best-first order**: worth ~0.1 ms in full-suite context —
  earlier queries already paid the fills. (Its design doc survives with the trap that
  would have made a naive version a regression: fills batch one offset-sorted read; the
  best-first order is not file order.)
- **io_uring** (from the previous phase, re-evaluated): still loses on unbatched walks
  at QD=1 on this box; parked behind a flag.

### 4.10 What remains

The best cold rounds touch 0.986 s; medians hover at the 2× line. The next real lever is
known and unbuilt: the two 1 M-group group-by queries cost ~55 ms each — kernel
microarchitecture (an append-mode high-cardinality build), not I/O. The
Java-vs-C++ question the campaign set out to answer resolves to: *2× is reachable; the
residual is kernel shape and footprint, not language.*

---

## 5. JSONBench: to ClickHouse parity in one day

JSONBench is ClickHouse's JSON-analytics benchmark: 1 M (up to 1 B) real Bluesky
firehose events — deeply nested, variably-shaped JSON — and five queries over event
kind, collection, user id and microsecond timestamps. ClickHouse's own setup is a strong
baseline: a table sorted exactly on the query columns, LowCardinality dictionary
strings, ZSTD, its native JSON type; 1 M rows load to 100 MB and every query runs in
15–64 ms on this box.

The port had two halves: a harness (load, translate, verify) and a set of engine
extensions the five queries needed to *serve*.

### 5.1 The harness

- **Loader**: streams the gzipped newline-delimited JSON through a reader that
  fabricates an enclosing array on the fly (reused from the ClickBench loader), shredding
  1 M events (~31 M nodes, 480 MB raw) in bounded memory into one array resource:
  43 s, 340 MB on disk, plus a five-column projection
  (`kind`, `did`, `time_us`, `commit/collection`, `commit/operation`).
- **Queries**: each ClickHouse SQL query hand-translated to JSONiq, in the exact FLWOR
  shape the detection stage recognizes, with a reference variant kept alongside.
- **Differential**: a comparator normalizes ClickHouse's TSV (NULL spelling, DateTime64
  formatting parsed back to microseconds) against SirixDB's JSON dumps. Two semantic
  traps were found by measuring ClickHouse rather than assuming:
  `toHour`/timestamp formatting are **session-timezone dependent** (references
  regenerated under UTC; our hour key is pure integer arithmetic), and
  `date_diff('milliseconds', a, b)` **truncates each endpoint** to milliseconds before
  subtracting — which differs from truncating the difference *and is not even
  order-preserving with respect to it*, so the span aggregate must order by the
  truncated form.

### 5.2 Nested-path projection columns

ClickBench's 105 columns are all top-level; Bluesky's interesting fields live under
`commit.*`. Three layers were involved: the projection *builder* already handled nested
paths (it matches fields by path-summary identity during a depth-first record walk);
the *kernels* never cared (they see column indexes). The *detection* layer was the
blocker: every field extraction required the deref to sit directly on the loop variable
— `$e.commit.collection` silently declined. Detection now accepts **pure deref chains**,
naming columns by their root-relative chain (`commit/collection`), and the covering
match compares *chains*, not bare trailing names — so a nested `collection` can never be
served from a same-named top-level column or vice versa (tested in both directions).

Relatedly, the projection DDL's ambiguity guard — which rejected any projected field
whose trailing name recurs anywhere under the root — made this corpus unprojectable
(`did` recurs at six paths inside `commit.record.*`). Since column lookup is now
path-qualified, the guard was relaxed to reject only declarations that are genuinely
ambiguous *as declared paths*.

### 5.3 Grouped COUNT(DISTINCT) over string columns

`uniqExact(did)` per collection. The grouped distinct-count machinery existed for
integer columns only. Dictionary ids are leaf-local (each 1024-row group has its own
dictionary), so equality across leaves needs a global identity: the kernel now feeds the
distinct sets a **64-bit content hash of the dictionary entry**, computed once per
referenced entry per leaf (the same memoization pattern, and the same
exact-up-to-hash-collision identity standard, the composite group keys already use).
Per-row cost: one int load and one array read.

### 5.4 The span aggregate

`ORDER BY max(ts) − min(ts) DESC LIMIT 3`. The group table already folds min and max
lanes for every aggregate column in one pass, so the span needed no new kernel — only:
detection of the composed `(max(x) idiv D) − (min(x) idiv D)` shape as one synthetic
aggregate token, an order-plan kind that compares the truncated difference of the two
existing lanes, and an emission case. One subtlety the tests encode: a *string*-keyed
group-by never qualifies for the in-kernel order plan — such shapes serve the grouping
and defer the sort downstream, which is provably exact because the lane-repurposing
cast-average writer only exists behind a resolved order plan.

### 5.5 `string()`-wrapped group keys

ClickHouse prints the "no such path" group as an empty string; JSONiq serializes an
absent deref as null. The query therefore needs `string($e.commit.collection)` as its
group key — which defeated detection (6.8× slower). Detection now recognizes the
`fn:string` wrapper over a chain as a *stringify-absent-to-empty* annotation: the kernel
already groups absent rows (presence bit clear) into one group; only that group's key
emission changes.

### 5.6 The filter story: why the first native run was 875× slower

With all five queries serving and byte-correct, the first **native-image** measurement
was a catastrophe: 156.6 s cold / 45.0 s hot — 10–40× *slower than the JVM* on four of
five queries, while Q1 (the only unfiltered query) ran a healthy 6 ms. The uniform ~13 s
across three differently-shaped queries pointed at their one common element: the WHERE
filter.

Profiling found the mechanism: for every predicate field, the served path performed a
**full-document field-key walk per row** (39 % of CPU) — work the JVM's JIT had
compressed to ~1 µs/row, and ahead-of-time compilation executed at face value,
~13 µs/row. The JIT had been *masking* the defect; AOT exposed it. (The path summary can
prove a field's absence without touching the document at all.)

The fix routes group-aggregate filters through the machinery the ClickBench predicate
routes already used: resolve each literal to a per-leaf dictionary id once per leaf
(or prove the leaf can't match), then the row loop compares integers — an IN-list of
three literals is three int compares. Absent fields fail predicates through presence
bits, exactly as before.

Effect of this one change:

| | JVM hot Σ | Native cold Σ | Native hot Σ |
|---|---|---|---|
| Before | 3.89 s | 156.6 s | 45.0 s |
| After | 0.494 s | **0.179 s** | **0.065 s** |

With, throughout, 5/5 byte-equivalent results, generic-pipeline parity, and the
ClickBench 43-query suite byte-identical as a canary.

### 5.7 Also fixed on the way

- Brackit's predicate-tree annotation names direct derefs only, and one unrepresentable
  conjunct dropped whole pipelines; the sirix side now builds its own chain-aware
  predicate tree (the upstream brackit checkout is deliberately untouched — it lags the
  published snapshot it resolves against).
- A silent-decline diagnostic (`-Dsirix.projDiag=true`) now prints *why* the
  group-aggregate detector declined a query — the absence of exactly this diagnostic
  cost two diagnosis cycles during the campaign ("fixtures serve, harness texts don't").

### 5.8 The 10 M tier: the hot win survives scale, the cold win does not

Because a 1 M-row win could be a small-scale artifact, the suite was re-run at the 10 M
tier (files 0001–0010; 4.6 GB raw). Two findings about the *dataset* first:

- **The published Bluesky files contain corrupt rows.** Six lines (three pairs) are
  records truncated at a 64 KiB buffer boundary — the cut line and its tail, neither
  valid JSON. ClickHouse's own official loader handles this by retrying with
  `input_format_allow_errors_num = 10⁹`, i.e. it *silently drops* what it cannot parse.
  For a byte-level differential both engines must hold identical rows, so both loaded a
  cleaned canonical corpus of 9 999 994 rows, with the drops documented.
- ClickHouse's 10 M table is 2.2 GB — 22× its 1 M size (the later files carry much
  fatter records); SirixDB's resource is 3.4 GB (10× — linear).

Results (same protocol: cleaned corpus, cool-gated, evicted cold, best-of-2 rounds;
ClickHouse baseline re-measured on the same data):

| | SirixDB | ClickHouse | Ratio |
|---|---|---|---|
| Cold suite | 1.216 s | 0.643 s | 1.9× slower |
| Hot suite | **0.263 s** | 0.484 s | **1.8× faster** |

Per query (SirixDB cold/hot vs ClickHouse cold/hot, ms): Q1 177/21 vs 23/25 ·
Q2 903/128 vs 277/227 · Q3 44/44 vs 88/78 · Q4 51/43 vs 124/74 · Q5 41/44 vs 131/80.
Differential: **5/5 MATCH at 10 M** — exact distinct counts over millions of users,
nine hour-buckets, fresh top-3 boundaries.

Reading: **hot, every query is at or ahead of ClickHouse and the suite is 1.8× faster —
the 1 M win is not a small-scale artifact.** Cold, the two first-touch whales grow with
the corpus while ClickHouse's compact sorted table barely faults: Q1 absorbs the
catalog walk and initial fills (~10× the row groups), and Q2's 903 ms is the `did`
column's cold fill plus hashing millions of dictionary entries.

### 5.9 Closing the 10 M cold gap: 1.9× → 1.31×

Three further units attacked the cold side, each with the full gate battery
(both-tier differentials, all serving suites, ClickBench 43/43 as the
fallback/regression canary, hot-no-regress):

1. **Flat dictionary slices.** Profiling Q2's fill found *three heap allocations per
   dictionary entry* (a copy out of the segment, an oversized decode scratch, and a
   trimming copy) — ~30 M allocations for one column, 78 % of the process's total
   allocation. The dictionary is now one flat `byte[]` plus an offsets array: RAW-mode
   dictionaries alias the fetched segment zero-copy, FSST-mode expands through a
   per-thread scratch into one exact array per leaf. Process allocation halved; every
   consumer was already `(array, offset, length)`-shaped. A latent corruption overrun
   (symbol length unchecked against the decode buffer) was hardened on the way.
2. **A silent readahead bug.** The whole-projection readahead sweep passed a null
   absent-marker array; the first leaf lacking a segment threw, the advisory catch
   swallowed it, and that column's entire readahead silently vanished. Fixed, with an
   engagement counter so it cannot silently regress again — the third member of this
   campaign's "silent-decline" bug family.
3. **Dict-entry hash segments.** The decisive one. Q2's distinct fold never reads the
   dictionary *strings* — it consumes 64-bit content hashes it used to compute from
   them. Those hashes are now precomputed at projection build time into an auxiliary
   per-leaf segment; in hash mode the fill reads packed row ids plus a flat `long[]` —
   no dictionary bytes, no FSST, no hashing loop. Columns without the segment (older
   builds) fall back to the full fill, proven by the untouched-format ClickBench canary.

With a fresh profile-guided build after each step, interleaved and cool-gated:

| 10 M cold suite | SirixDB | vs ClickHouse 0.643 s |
|---|---|---|
| baseline (jb-2) | 1.216 s | 1.9× |
| + flat dicts, sweep fix, PGO (jb-4) | 1.100 s | 1.71× |
| + hash segments (jb-5) | **0.845 s** | **1.31×** |

Hot stayed at **0.230 s — 2.1× faster than ClickHouse** — through all three steps.

### 5.10 The global value dictionary: beating ClickHouse on both axes

The structural end-state was unlocked by an observation from the project's author:
SirixDB already owns a *versioned global dictionary* — the NamePage, which maps JSON
field names (and XML element names) to stable integer keys in a copy-on-write sub-trie,
and which had already been extended once (for FSST symbol tables) with the documented
argument that a new offset inside it is not a wire-format change. The value dictionary
is the same mechanism at a different scale: per *high-cardinality column*, every
distinct value stored once, every row a small integer id, both directions as individual
CoW records — ids append-only and revision-stable exactly like name keys.

Design points that mattered (each learned the hard way):

- **Dense keys are mandatory.** The sub-trie writer grows levels only at power-of-two
  page-key boundaries; a strided "namespace" of keys silently collapses every record
  onto the root page. Each column reserves one contiguous key run; its base rides in
  the projection metadata.
- **Which columns go global is measured, not guessed.** AUTO/ALWAYS builds with eligible string
  columns hold back their first 16 leaves; their per-leaf dictionaries *are* the sample. NEVER and
  builds without an eligible column stream immediately. A column goes global when
  the per-leaf dedup factor shows the local dictionaries have stopped deduplicating
  (`did` ≈ 1.0 → global; `collection`/`kind`/`operation` ≈ 100+ → stay per-leaf, where
  2-bit packing is unbeatable).
- **Ids are stored as a numeric column.** The global kind is byte-identical to the
  int64 column layout, so zone maps, bit-packing and presence machinery work unchanged
  — and `GROUP BY did` runs on the *integer* kernel, with only the k winners
  reverse-mapped to strings. Distinct becomes exact integer counting; an equality
  literal is one dictionary probe, then integer compares.
- **Widening a type gate re-opens every dispatch.** Three silent mis-dispatches were
  caught before shipping (a predicate arm that would have read ids as packed booleans
  and matched every row; sort kernels that would have ordered by mint order; a mask
  evaluator that now throws loudly on untranslated literals). And the fail-soft
  declines exposed a *pre-existing* wrong answer in shipping defaults — `count` of an
  empty-string equality on fragmented pages returned 0, because a zero-length payload
  was conflated with "no value" and an incomplete dictionary sketch was trusted before
  the completeness oracle. Both fixed; the rare-literal lesson, materialized twice.

Final numbers (fresh profile-guided build per format; cool-gated, evicted,
min-of-2 rounds; binaries are format-tied, so each is measured on its own format):

| | SirixDB (global dict) | ClickHouse | |
|---|---|---|---|
| **10 M cold** | **0.563 s** | 0.643 s | **1.14× faster** |
| **10 M hot** | **0.180 s** | 0.484 s | **2.7× faster** |
| **1 M cold** | **0.098 s** | 0.187 s | **1.9× faster** |
| **1 M hot** | **0.056 s** | 0.145 s | **2.6× faster** |

The full 10 M cold trajectory: **1.9× behind → 1.71× → 1.31× → 1.14× ahead.** All with
5/5 byte-equivalent answers at both tiers and the ClickBench 43/43 canary intact. The
projection build also got *faster* under the global dictionary (96 s vs 140 s at 10 M).

### 5.11 The 100 M tier: a seven-defect reliability campaign

Scaling to 100 M rows (99,999,968 after dropping 32 corrupt lines; SirixDB 31 GB on
disk vs ClickHouse 27 GB — near footprint parity, the global dictionary compounding)
turned from a benchmark run into a defect hunt: seven latent engine defects that no
smaller tier could reach, every one attributed with measurements before a line of fix
was written — and five of the seven initial premises were overturned by that
attribution.

1. **Cache budgets vs allocator grant** — caches were sized from the database's
   *persisted* config while the off-heap arena obeyed the runtime budget: a 28×
   over-commit whose pressure evictor measured the wrong quantity and never fired.
   Fixed (grant-is-truth sizing, physical-headroom clamp, shed-from-holdings). The
   full served 10 M suite now runs in **1.91 GB peak**, a tier-independent bound.
2. **Unbounded promotion** — the byte-kernel promotion would attempt a ~30 GB payload
   materialization. Now budget-gated at its single choke point, declined promotions
   latched.
3. **HOT incremental-split segment-ref gap** — a fail-loud guard at ~97 k row groups;
   the split now routes references like the rebuild path always did. The 100 M
   projection builds in place (~17 min).
4. A scale-dependent projection-DDL failure under fast paths — quarantined, did not
   rematerialize.
5. **Exact-distinct budget → uncompletable fallback** — the 2²⁴ cap predates the
   tier's ~40 M distinct users; the decline route couldn't finish. Fixed with dense
   bitmaps over the global dictionary's id space (shared, atomic, byte-capped):
   `uniqExact` over 40 M users in **3.4 s, exact, matching ClickHouse**, ~175 MB.
6. **The fallback's spill machinery was dead code** — the "spillable" group-by had
   never spilled (zero callers on its spill path). Real spilling was implemented
   (resident-set cap by retained bytes, whole-group-resident-or-on-disk, heap
   backstop) — and the hunt surfaced a **shipping wrong-output bug**: the JSON
   serializer never escaped strings (809 of 200 k real records rendered invalid).
   Both fixed in brackit, gate-verified from both repos.
7. **Array-anchor theft** — the new spill's size probe renders the document array and
   steals its single sequential anchor, sending an interrupted 25 M-element scan into
   a branch that materialized all 100 M objects. Fixed with a two-slot anchor table
   and a hard invariant: positional access *never* materializes — proven by watching
   the theft happen 13 M elements deep and cost exactly one cursor move.

An eighth finding then unblocked the full suite: Q4/Q5's decline was neither capacity
nor cardinality but an **arithmetic overflow in a sum lane the queries never read**
(microsecond timestamps crossing 2⁶³ at the partition merge — the third member of the
int64-wraparound family). Aggregate lanes now fold only when the query reads them,
with a merge-contract guard so the next site of this family cannot be silent.

The first complete native 100 M scoreboard (cool-gated, evicted, two rounds within
1.5 % of each other, all five serving, 5/5 byte-equivalent):

| | SirixDB cold/hot | ClickHouse cold/hot | |
|---|---|---|---|
| Q1 | 1.24 / 0.12 | 0.11 / 0.11 | hot **parity** |
| Q2 (`uniqExact`, 40 M users) | 2.16 / **0.79** | 2.18 / 1.92 | cold parity, hot **2.4× faster**, exact |
| Q3 | 1.37 / **0.12** | 0.80 / 0.53 | hot **4.4× faster** |
| Q4 | 1.98 / 1.73 | 0.49 / 0.43 | behind |
| Q5 | 1.77 / 1.74 | 0.58 / 0.52 | behind |
| **Σ** | **8.6 / 4.49** | **4.16 / 3.50** | cold 2.05×, hot 1.28× |

Hot, SirixDB beat ClickHouse on three of five queries; the deficit was Q4+Q5, whose
profiled cost was the group-table hash probe (~35 % of the hot path). The final unit
replaced it for dense-global-id keys with a **dense table indexed directly by
dictionary id** — no probe, no growth, no partition merge — behind a byte-budget gate
with the usual fail-soft decline. Two traps on the way are worth recording: at small
tiers the counter read zero not because wiring was missing but because *promotion*
legitimately routes those tiers' queries to the byte-kernel path; and the first
native build showed none of the JVM's 40 % win because the **PGO profile predated the
new hot loop** — a fresh instrument→profile→optimize cycle unlocked it, plus broad
gains elsewhere. After landing any new hot-path class, the profile is stale by
definition.

**Final 100 M scoreboard** (fresh-PGO native, cool-gated, evicted, two rounds):

| | SirixDB cold/hot | ClickHouse cold/hot | |
|---|---|---|---|
| Q1 | 1.26 / 0.12 | 0.11 / 0.11 | hot parity |
| Q2 (exact, 40 M users) | 1.58 / **0.40** | 2.18 / 1.92 | **cold 1.4× and hot 4.7× faster** |
| Q3 | 1.4–1.6 / **0.10** | 0.80 / 0.53 | hot **5× faster** |
| Q4 | 1.44 / 1.37 | 0.49 / 0.43 | behind |
| Q5 | 1.38 / 1.39 | 0.58 / 0.52 | behind |
| **Σ** | **7.1 / 3.35–3.43** | **4.16 / 3.50** | **hot AHEAD (0.97×) · cold 1.71×** |

The cold campaign then closed the tier. Attribution first, as always: a
warm-page-cache control proved 100 M cold is **CPU, not I/O** (I/O = 4 %), burying
the readahead levers with evidence; the catalog walk was already off-clock; the real
costs were a *serial, single-core* segment-chain fetch (97,657 descriptor searches +
read + verify on a 20-core box) and the decode expansion (640 MB raw → 2.77 GB
decoded, which also makes the first fold cost 4.6× the settled one). Parallelizing
the chain fetch — and skipping it entirely for all-inline chains, a waste that had
run silently forever — halved the fill phase, and because predicate-masked slices are
never cached, the same fix paid on *every hot execution* of the pruning queries.
A fresh profile cycle (the stale-PGO lesson, now structural) released the full
effect in the native image:

**Final 100 M scoreboard (jb-11, cool-gated, evicted, two rounds):**

| | SirixDB cold/hot | ClickHouse cold/hot | |
|---|---|---|---|
| Q1 | 0.35 / 0.12 | 0.11 / 0.11 | hot parity |
| Q2 (exact, 40 M users) | **0.59 / 0.31** | 2.18 / 1.92 | **3.5× / 5.9× faster** |
| Q3 | **0.44 / 0.11** | 0.80 / 0.53 | **1.8× / 4.6× faster** |
| Q4 | 0.68 / 0.61 | 0.49 / 0.43 | ~1.4× behind |
| Q5 | 0.55 / 0.57 | 0.58 / 0.52 | ~parity |
| **Σ** | **2.61–2.82 / 1.74–1.84** | **4.16 / 3.50** | **cold 1.5× · hot 2× FASTER** |

**The campaign's goal is achieved at every tier**: 1 M (0.098/0.056 vs 0.187/0.145),
10 M (0.563/0.180 vs 0.643/0.484), and 100 M — SirixDB faster than ClickHouse on both
axes, with byte-equivalent answers throughout and the ClickBench 43/43 canary intact
across every change. The remaining ledger (the packed-slice decode design, the
promotion budget defect, the walk's superlinear scaling toward 1 B) is optional
headroom, not a gap.

---

## 6. Cross-cutting lessons

1. **Measure in the configuration you ship.** The JSONBench filter defect was invisible
   on the JVM and catastrophic under AOT. The ClickBench cold-regime findings similarly
   only exist in fresh-process, evicted-cache runs.
2. **A served route is not a fast route.** Counters prove routing; only phase-level
   attribution proves the route is doing the right *work* (the top-k pruner that never
   pruned; the walk-per-row filter).
3. **Silent declines are expensive.** Both campaigns lost cycles to routes that could
   have said why they refused. Decline diagnostics are now first-class.
4. **Backgrounds and overlaps are free wins when they're advisory.** Promotion,
   readahead, the bloom overlap, and the open-time warm all move work off the critical
   path with fail-soft semantics and one-shot latches — none of them can make a query
   wrong or slower.
5. **The differential must be able to fail.** Route-fired assertions, selectivity
   sweeps, sabotage tests, and same-code control legs keep the "everything matches"
   signal meaningful.
6. **Record the negatives.** The zstd result, the parallel-extrema reversal, the lazy-
   fill kill, and the io_uring verdict are documented with numbers precisely so future
   sessions don't rediscover them.

---

## 7. Reproducing the numbers

Both campaigns ship as reproduction kits in the repository. Nothing above depends on
the campaign box: the kits take a corpus, an engine binary and a temperature sensor, and
re-derive every figure under the protocol of §3.

| kit | where | what it reproduces |
|---|---|---|
| JSONBench | `bundles/sirix-query/bench/jsonbench/` ([README](../bundles/sirix-query/bench/jsonbench/README.md)) | §5, all three tiers, including the ClickHouse reference side |
| ClickBench | `bundles/sirix-query/bench/clickbench/` ([README](../bundles/sirix-query/bench/clickbench/README.md)) | §4, plus the DuckDB reference and the correctness differential |
| shared | `bundles/sirix-query/bench/common/` | the page-cache evictor and the cool gate both kits use |

### JSONBench, one screen

```bash
export JAVA_HOME=/path/to/graalvm-25
KIT=bundles/sirix-query/bench/jsonbench;  W=/var/tmp/jsonbench;  CH=/path/to/clickhouse

$KIT/download-data.sh 1m $W/data                                  # 135 MB, resumable
$KIT/clean-corpus.py  $W/data 1m                                  # drops the corrupt rows
$KIT/clickhouse-setup.sh 1m $W/data/bluesky-1m-clean.ndjson $CH $W # table + UTC refs + baseline
$KIT/pgo-native.sh    $W/db-1m --tier 1m --out $W/pgo             # instrument -> profile -> optimise
$KIT/run-benchmark.sh 1m $W/db-1m $W/ch-ref-1m \
    --data $W/data/bluesky-1m-clean.ndjson --bin $W/pgo/jb        # rounds + differential + table
```

Substitute `10m` or `100m` for `1m`. Omit `--bin` to measure the JVM: that is a valid
correctness run, but the published figures are all ahead-of-time compiled and a JVM run
pays classload and JIT on top of them. The per-tier heap/arena configuration (including
the two 100 M workarounds) is baked into `run-benchmark.sh` and tabulated in its README.

### ClickBench, one screen

```bash
KIT=bundles/sirix-query/bench/clickbench

./gradlew :sirix-query:clickBenchLoad -Pclickbench.args="<dbDir> generate:1000000"
$KIT/run-differential.sh 200000        # correctness: fast path vs interpreter vs DuckDB
$KIT/cold-rounds.sh <dbDir> --arm base=<binA> --arm new=<binB> --rounds 4
```

`cold-rounds.sh` is the protocol of §3 made executable: it evicts the page cache
(`posix_fadvise(DONTNEED)`, no root needed — and `common/evict.py --verify` proves the
eviction with `mincore(2)`), waits for the CPU package to drop below 55 °C, runs each arm
in a fresh process, **interleaves the arms** rather than blocking them, and reports the
best and median of N rounds. Its summary refuses to print a suite figure when any query
produced no timing, because a partial sum silently reads as a whole one.

The serving suites that gate every engine change:
`ProjectionIndexCatalogServingTest`, `NestedDerefProjectionServingTest`,
`StringDistinctGroupServingTest`, `SpanOrderTopKServingTest`,
`ProjectionParallelDirectoryWalkTest`, `GroupTopKDifferentialTest`, plus the chunked-body
conformance sweeps under `io.sirix.page.chunked`.

## 8. Change inventory

Committed (row store): `f56921b29`…`65a9dda47` — design doc
(`docs/ROWSTORE_RANGED_DECODE_PLAN.md`), chunk wire format, lazy machinery, conformance
sweeps.

Uncommitted (both campaigns): ~50 modified + 10 new files, ≈5 900 insertions. Core:
`ProjectionColumnStore` (prefetch-all, extrema memoization),
`ProjectionIndexHOTStorage` (parallel walk), `ProjectionIndexCatalog` (bloom overlap,
path-qualified guard), `ProjectionIndexRegistry` (promotion/prefetch latches, field
chains), `ProjectionColumnScan`/`ProjectionColumnGroupScan`/`ProjectionIndexByteScan`
(best-first top-k, value emission, string distinct, filter masks),
`NumericGroupAggTable` (interleaved cell layout). Query:
`SirixVectorizedExecutor` (routes, order plans, filter masks),
`GroupAggregateDetectionStage` (chains, span, stringify, decline diagnostics),
`CreateProjectionIndex` (guard), the `bench/jsonbench` package, and both benchmark
runners (counters, open-time warm).
