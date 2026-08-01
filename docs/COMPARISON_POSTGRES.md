# SirixDB vs PostgreSQL: Versioned-Document Benchmark

An honest, same-machine comparison of SirixDB's native versioning against the standard
PostgreSQL pattern for versioned JSON documents (jsonb column + trigger-maintained history
table). Both systems run the **identical logical workload**; results are cross-checked
(identical field-history checksums) to prove both ended up with the same 5,001-version
history. **PostgreSQL wins most raw numbers in this small-document regime — that is the
finding, and the analysis below explains where each system's advantages actually are.**

Date: 2026-06-11. **Re-run 2026-07-30 — see [§0](#0-re-run-2026-07-30-current-dev-build) below;
the June numbers are retained as the historical record of the original hardware.**

---

## 0. Re-run: 2026-07-30 (current dev build)

Same workloads (W1–W6, identical ~2.4 KB document spec, 5,000 durable single-field commits,
5,001 retained versions), re-measured on a **different, slower machine** — numbers below are
comparable *within this table only*, not to the June table:

| | |
|---|---|
| Machine | 4-vCPU cloud container, 15 GB RAM, virtualized storage (single shared device) |
| Durability floor | `pg_test_fsync`: **fdatasync 1,337 ops/s (748 µs/op)** — vs 4,778 ops/s (209 µs) on the June NVMe box, so both systems' absolute write numbers are proportionally lower here |
| SirixDB | current dev build (per-slot value elision, region wire compression, FSST string compression), embedded, `-Xms1g -Xmx4g`, defaults otherwise |
| PostgreSQL | **16.13** (distro package, no Docker), local unix socket, `shared_buffers=1GB`, `synchronous_commit=on`, `fsync=on` |

Cross-checks passed again on both sides: 5,001 versions; W4 field-history sum
**12,502,500 on both**; W6 identifies exactly the `counter` field.

| Workload | SirixDB (full) | SirixDB (lean) | PostgreSQL 16 | Winner |
|---|---|---|---|---|
| **W1 ingest**: 5,000 durable commits | 41.2 s = **121/s** (8.23 ms) | 29.6 s = **169/s** (5.91 ms) | server-side 4.58 s = **1,093/s** (0.92 ms) · client-driven 6.38 s = **784/s** | **PostgreSQL 4.6–9×** (was 5.5–10.7×) |
| W1 initial insert | 254 ms | 199 ms | 4.0 ms | PostgreSQL |
| **W2**: 1,000 random PIT full-doc reads | 452 ms (452 µs/read) | 657 ms | batched 34.5 ms (**34.5 µs/read**) | PostgreSQL (batched) |
| W2 fixed mid-history | 111 ms | 136 ms | 34.5 ms | PostgreSQL ~3× |
| **W3**: history listing (5,001 timestamps) | **4.37 ms** | 4.38 ms | 4.94 ms | **now a tie / slight SirixDB edge** (was PostgreSQL 2×) |
| **W4**: one field across all versions | 75.2 ms (axis) / 68.6 ms (loop) | 70.6 / 77.9 ms | 14.3 ms | PostgreSQL ~5× (was ~7×) |
| **W6**: diff of adjacent versions | **0.58 ms** — node-level semantic diff, 164-char patch | 1.93 ms | 1.91 ms — top-level compare only | **SirixDB on both speed and capability** |
| **W5**: storage for full history | 14.2 MiB (was 16.4) | **9.9 MiB** (was 11.8) | 4.66 MiB (unchanged) | PostgreSQL 2.1× (was 2.5–3.5×) |

What moved since June (same workload, so the deltas are engine deltas plus the machine change):

- **The ingest gap narrowed** (PostgreSQL 9.4× → 6.5× against the lean config, server-side):
  the recent write-path work shows up even though this box's fsync is 3.6× slower. PostgreSQL
  again sits at ~82 % of the measured fdatasync floor — its number remains honestly fsync-bound.
- **SirixDB's storage dropped ~14–16 %** on the identical workload (per-slot value elision +
  region compression); PostgreSQL's footprint is unchanged. The storage gap is now ~2×.
- **W3 flipped**: history listing is no longer a PostgreSQL win.
- All June caveats (§3) apply unchanged; the client-driven PostgreSQL variant again quantifies
  the process boundary (1,093 → 784 commits/s here, unix socket, no Docker overhead).

Reproduction: the original `/tmp/wave5-b` harness did not survive the June machine; the re-run
used faithful re-implementations of §1's spec (a single-file `SirixVersionedDocBench` driver
plus a psql schema/procedure script).

### 0.1 Follow-up: the durable-commit fast path is now the default (same day)

Profiling the re-run's W1 showed **63 % of commit wall time in synchronous I/O** — five device
round-trips per durable commit (grow-file `force(true)` barrier, an O_DSYNC revision-record
write, and two FUA uber-beacon writes). Two existing, formerly opt-in optimizations remove two
of them without touching the durability contract, and are now **on by default**:

- `sirix.commit.preallocated` — files preallocate ahead of the write frontier in adaptive
  chunks (256 KiB floor, roughly doubling per grow to an 8 MiB cap; the revisions file uses
  fixed 64 KiB chunks) so per-commit writes never extend `i_size`; the write-ahead barrier
  becomes a journal-free `fdatasync`. FILE_CHANNEL backend only — MEMORY_MAPPED readers
  remap on physical growth, so that backend keeps the legacy path.
- `sirix.commit.bufferedBeacons` — both uber-beacon copies become durable with ONE buffered
  `fdatasync` instead of two O_DSYNC writes; write-ahead ordering and two-copy redundancy are
  unchanged.

Also: the revision-root page cache default grew from a fixed 5,000 entries to 20,000 and
became configurable (`sirix.cache.revisionRoot.max.entries`) — the old fixed cap silently
thrashed any history longer than 5,000 revisions under random point-in-time access, which is
exactly this benchmark (its 5,001 revisions sit just past the old cap; raise the property for
histories beyond 20k).

Paired same-box, same-session measurements (5,000 durable commits, lean config):

| | legacy path | new defaults |
|---|---|---|
| W1 durable commits | 118/s (8.49 ms) | **175/s (5.71 ms)** |
| Gap to PostgreSQL 16 (1,093/s server-side) | 9.3× | **6.2×** |
| W2 random PIT reads (3,000, final warm run) | 452–565 µs/read, with multi-second eviction cliffs | **311 µs/read, monotonic warm-up, no cliffs** |
| W2 fixed mid-history | 111 µs/read | 117 µs/read (unchanged) |

Remaining W1 syncs per commit: the data barrier, the beacon fdatasync, and the DSYNC
revision-record write — a designed follow-up can fold the revision record under the barrier
with recovery-side reconstruction (the record is checksummed and derivable from the
beacon-anchored uber page), taking the floor to two round-trips. Note on W5 with the new
defaults: preallocation leaves a zero tail per file at rest — physically allocated (the
zero-fill+fsync is what keeps later in-place writes journal-free), persisting across sessions
by design, and bounded by the adaptive chunk (≈ the resource's own size, floor 256 KiB, cap
8 MiB per file). This padding is excluded when comparing to PostgreSQL's storage number,
which likewise excludes its WAL.

### 0.2 Follow-up: the third round-trip is gone, and the read path got its setup back

§0.1 ended with a designed follow-up — "fold the revision record under the barrier with
recovery-side reconstruction … taking the floor to two round-trips". That is now implemented
(`sirix.commit.lazyRevisionRecord`, on by default with preallocated commits): the revisions
channel is buffered and each commit's 32-byte record rides a checksummed 16-entry ring embedded
in the trailing pad of BOTH uber-beacon slots, staged before the write-ahead barrier that already
hardens the data file. A committed revision's record is therefore durable exactly when its beacon
is. Eviction from the ring forces the revisions file first, so no committed record ever depends on
the ring alone; a record lost from the file inside the ring window is salvaged and healed back in
place; outside the window it fails loudly. See `docs/DISK_FORMAT.md` for the layout and the
invariants, and `LazyRevisionRecordRecoveryTest` for the coverage.

Alongside it, four read-path costs were removed: the page cache and revision-root cache were
re-reading and re-deserializing pages they already held (`asMap().compute()` runs its mapping
function on a hit, and every caller is a pure load-if-absent); the writer re-derived its logical
write frontier from the durable revision graph on the first write of every commit; the shared
reader channel pool closed the instant its borrow count hit zero, so back-to-back short read
transactions paid two `open(2)`/`close(2)` pairs each; and every transaction loaded the NamePage at
open whether or not it read a name. Finally, `JsonSerializer.Builder` gained a
`(JsonNodeReadOnlyTrx, Appendable)` overload so a caller that already holds a transaction can lend
it instead of making the serializer open and close one.

Measured with the in-repo `DurableCommitBenchmark` (JMH, `AverageTime`, µs/op, 5 warmup + 12
measurement iterations, 1 fork), same box as §0/§0.1, against the immediately preceding commit as
the baseline:

| Probe | before | after | change |
|---|---|---|---|
| Open a read transaction, read one value, close | **10.314 ± 0.302** | **2.757 ± 0.133** | **3.7× faster** |
| Serialize the full document, serializer opens its own transaction | 49.674 ± 2.861 | 46.540 ± 2.791 | no significant change (intervals overlap) |
| Serialize the full document through a client's open transaction | n/a (new) | **21.706 ± 0.751** | **2.3× faster than opening one** |
| Point read on an already-open cursor | 0.057 | 0.051 | unchanged — control |

Reading this table honestly matters more than the headline:

- **The control is the point.** Nothing in this work touched trie traversal, and the held-cursor
  read is flat at ~50 ns across both builds. So the 3.7× on the first row is not measurement drift
  or a faster machine: it is per-transaction setup that genuinely stopped happening. At ~2.8 µs, an
  open-read-close round trip is now roughly one eighth of PostgreSQL's 34.5 µs batched per-read
  figure in §0 — though note those measure different things (that one is a full-document PIT read
  over a socket, this is one value in-process), so they are not directly comparable.
- **The serializer's owning path did NOT measurably improve.** Its interval overlaps the baseline's;
  the ~6 % gap is not resolvable at this sample size, and it would be wrong to bank it. What is
  resolvable is that lending the serializer an already-open transaction is worth 2.3×, which is the
  whole reason the overload exists.
- **The commit probe is not reportable on this box.** `durableCommit` measured 7,506 ± 7,848 µs
  before and 5,789 ± 1,936 µs after — the error bar on the baseline exceeds its own mean. This is
  virtualized shared storage whose fsync latency wanders by more than the effect being measured, so
  no commit-side claim is made here. The round-trip count dropping from three to two is a
  structural property of the protocol (verifiable by inspection and by the crash tests), not
  something this box can time; re-measure on the dedicated NVMe box before quoting a number.

### 0.3 Reproduction

```
./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*DurableCommitBenchmark.*' \
    -Pjmh.warmupIterations=5 -Pjmh.iterations=12
```

For a before/after, run the same harness in a worktree at the baseline commit (the benchmark file
copies over cleanly except for the borrowed-cursor probe, whose Builder overload does not exist
there).

---

## 1. Setup

| | |
|---|---|
| Machine | Intel i7-12700H (20 threads), 32 GB RAM, WD SN810 1 TB NVMe, ext4, Linux 6.8 |
| SirixDB | dev build `1.0.0-alpha22` + uncommitted working-tree changes (`/tmp/sirix-fix`, prebuilt classes), **embedded in-process**, GraalVM JDK 25.0.3, `-Xms1g -Xmx4g`, `StorageType.FILE_CHANNEL`, `VersioningType.SLIDING_SNAPSHOT` |
| PostgreSQL | 17.10 (official `postgres:17` Docker image), data on a named Docker volume on the **same ext4 NVMe**, `shared_buffers=1GB`, `synchronous_commit=on`, `fsync=on`, everything else default |
| Workload driver | SirixDB: single Java process (`/tmp/wave5-b/SirixVersionedDocBench.java`). PostgreSQL: `psql` inside the container via unix socket; hot loops run **server-side** (plpgsql procedure/functions) so PostgreSQL pays no client round trips during measurement (see caveat #1) |
| Document | deterministic ~2.4 KB JSON: 50 top-level fields (`counter` first, 36 strings, 8 ints, 4 bools) + one nested array of 20 item objects. Identical bytes fed to both systems |
| Execution order | strictly sequential (sirix full config → sirix lean config → PostgreSQL); no disk/CPU contention between systems |

Two SirixDB resource configurations were measured, because several sirix features have
per-commit cost that PostgreSQL's pattern simply doesn't have an equivalent for:

- **full** (= the defaults): path summary, child counts, rolling hashes, per-node history
  index (`RECORD_TO_REVISIONS`), per-commit stored diff files.
- **lean**: all of the above disabled (`hashKind(NONE)`, `storeDiffs(false)`,
  `storeNodeHistory(false)`, `buildPathSummary(false)`, `storeChildCount(false)`).

### Durability parity (verified, not assumed)

- PostgreSQL: `synchronous_commit=on`, `fsync=on` → one `fdatasync` of the WAL per commit.
  `pg_test_fsync` on the same volume: **fdatasync 4,778 ops/s (209 µs/op)** — PostgreSQL's
  measured 4,015 commits/s is at 84 % of that hardware floor, i.e. its W1 is fsync-bound
  and honestly tuned.
- SirixDB: each commit ends with a `dataFileChannel.force(true)` write-ahead barrier plus
  three O_DSYNC (FUA) write-through writes (revision record + dual uber-page beacons) —
  verified in `bundles/sirix-core/.../io/filechannel/FileChannelWriter.java`. That is, if
  anything, a *stronger* per-commit durability protocol than PostgreSQL's single
  fdatasync (~0.85 ms/commit durability floor on this disk vs ~0.21 ms).

Both data directories live on the same physical filesystem; nothing runs on tmpfs.

### The workloads

| | Semantics | SirixDB implementation | PostgreSQL implementation |
|---|---|---|---|
| W1 | insert doc, then 5,000 single-field updates, **each one its own durable transaction**, full history retained → 5,001 versions | `wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(doc))` then loop `wtx.moveTo(counterNodeKey); wtx.setNumberValue(i); wtx.commit()` | `doc(id, doc jsonb)` + `doc_history(id, rev, valid_from timestamptz, doc jsonb)` maintained by an `AFTER INSERT OR UPDATE` trigger in the same transaction; (a) server-side `CALL bench_w1(5000)` — plpgsql loop `UPDATE … jsonb_set(doc,'{counter}',to_jsonb(i)); COMMIT;`; (b) client-driven: 5,000 autocommit `UPDATE` statements via `psql -f` |
| W2 | 1,000 random point-in-time reads, each fetching + serializing the whole document | random revision ∈ [1, 5001]; `session.beginNodeReadOnlyTrx(rev)` + `JsonSerializer` to a `StringWriter` | random timestamp ∈ [min, max]; `SELECT doc::text FROM doc_history WHERE valid_from <= t ORDER BY valid_from DESC LIMIT 1` (plpgsql loop; index `(valid_from)`; verified plan: Index Scan Backward, 3 buffer hits) |
| W3 | list all 5,001 version timestamps | `session.getHistory()` and iterate the `RevisionInfo` list | plpgsql loop over `SELECT rev, valid_from FROM doc_history ORDER BY valid_from` |
| W4 | one field's value across all 5,001 versions | (a) native `AllTimeAxis` from the counter node; (b) manual loop `beginNodeReadOnlyTrx(rev) + moveTo(nodeKey)` | `SELECT count(c), sum(c) FROM (SELECT (doc->>'counter')::bigint AS c FROM doc_history ORDER BY valid_from) s` |
| W5 | bytes on disk for the full history | bytes of the database directory (apparent = Σ file sizes, and allocated = `du`) | `pg_total_relation_size('doc') + pg_total_relation_size('doc_history')` after `CHECKPOINT` (**WAL excluded** — see caveat #6) |
| W6 | diff between version N/2 and N/2+1 | native `new BasicJsonDiff(db).generateDiff(session, 2500, 2501)` | **no native diff** — representative top-level compare: `jsonb_each` of both revisions, `FULL OUTER JOIN … WHERE a.v IS DISTINCT FROM b.v` (not semantically equivalent, see caveat #5) |

Timing: reads = 1 untimed warm-up pass + 3 timed runs, **median** reported (all runs in the
raw logs). W1 = single timed pass (5,000 commits warms the JVM as it goes; per-1,000-commit
window rates reported). PostgreSQL statement times via `\timing` (psql in-container, unix
socket, one round trip per measured statement).

Correctness cross-checks (passed): both systems report 5,001 versions; W4 sum of the counter
across all versions = **12,502,500 on both**; final counter = 5,000 on both; the W6 diff on
both identifies exactly the `counter` field (2499 → 2500).

---

## 2. Results

5,000 single-field updates on one ~2.4 KB document, 5,001 retained versions, same NVMe,
both warm, durability verified on both sides. Medians of 3 for reads.

| Workload | SirixDB (full) | SirixDB (lean) | PostgreSQL 17 | Winner |
|---|---|---|---|---|
| **W1 ingest**: 5,000 durable single-field commits | 13.32 s = **375 commits/s** (2.66 ms/commit; steady-state windows 444–477/s) | 11.65 s = **429 commits/s** (2.33 ms/commit; peak window 555/s) | server-side: 1.245 s = **4,015 commits/s** (0.249 ms/commit) · client-driven: 2.10 s = **2,376 commits/s** | **PostgreSQL, 5.5–10.7×** |
| W1 initial insert (version 1) | 71.3 ms (first-ever commit, cold JIT) | 75.0 ms | 3.0 ms | PostgreSQL |
| **W2**: 1,000 random point-in-time full-doc reads | 75.7 ms (**75.7 µs/read**) | 74.5 ms | batched: 17.5 ms (**17.5 µs/read**) · client-driven, per-statement: **~104 µs/read** | **PostgreSQL 4.3× (batched)** — but **SirixDB wins per-statement** (75.7 vs ~104 µs) |
| W2 fixed mid-history (1,000 reads @ rev 2500) | 63.1 ms | 60.6 ms | 18.6 ms (batched) | PostgreSQL 3.3× |
| **W3**: history listing (5,001 timestamps) | 4.57 ms | 3.36 ms | 1.99 ms | PostgreSQL ~2× |
| **W4**: one field across all 5,001 versions | 55.4 ms (AllTimeAxis) / 49.4 ms (manual loop) | 48.8 / 47.5 ms | 6.91 ms (**1.4 µs/version**) | **PostgreSQL ~7×** |
| **W6**: diff of adjacent versions | **0.30 ms** — node-level semantic diff (exact node keys, 163-char JSON patch) | 0.40 ms | 0.15 ms — top-level field compare only (would need app-side recursion for parity) | sub-ms tie on speed; **SirixDB on capability** |
| **W5**: storage for full history | 16.43 MiB apparent / 37.2 MB allocated (5,000 diff files × 4 KB blocks) | 11.81 MiB apparent / 12.4 MB allocated | **4.66 MiB** (history table 4.6 MB incl. 256 KB index; WAL excluded) | **PostgreSQL 2.5–3.5×** |
| W5 per version | 3,444 B | 2,476 B | **978 B** (2.4 KB doc pglz-compressed to 836 B/row) | PostgreSQL |

Raw logs: `/tmp/wave5-b/sirix-full.log`, `/tmp/wave5-b/sirix-lean.log`,
`/tmp/wave5-b/pg-results.log`, `/tmp/wave5-b/pg-reads.log`.

---

## 3. Honest caveats

1. **Process boundary.** SirixDB ran embedded (zero IPC). PostgreSQL's headline numbers
   use server-side plpgsql loops / a single `CALL`, i.e. they deliberately remove client
   round trips — the most favorable honest setup for PostgreSQL. The client-driven
   variants quantify the boundary: W1 drops 4,015 → 2,376 commits/s, and W2 drops
   17.5 µs → ~104 µs/read (1,000 individual `SELECT`s through psql over the **unix
   socket**, measured as wall time minus a separately measured ~65 ms `docker exec`+psql
   startup overhead; TCP/JDBC from another host would be slower still). An application
   that reads single documents one statement at a time sits on the client-driven line,
   where embedded SirixDB (75.7 µs) is actually *faster* than PostgreSQL.
2. **No JDBC driver available** in the local caches, so PostgreSQL was driven by psql
   inside the container. All quoted PG numbers are psql `\timing` of single statements
   (one round trip each) or wall-clocked `psql -f` runs as labeled — methodologically
   this is *better* for PostgreSQL than JDBC would have been.
3. **Narrow regime.** One ~2.4 KB document, one writer, 5,001 versions, fully cached.
   Nothing here measures large documents, millions of versions, many documents, or
   concurrent writers. The regime choice favors PostgreSQL (see §5).
4. **W2 randomness mapping.** SirixDB picks revisions uniformly; PostgreSQL picks
   timestamps uniformly over the history span. Equivalent only if commit rate is roughly
   constant within each run (it was, after the first JIT-warm window).
5. **W6 is not apples-to-apples.** SirixDB's diff is a recursive node-level semantic diff
   with stable node keys (directly usable as a patch); the PostgreSQL query only compares
   top-level fields — a change inside `items[7].qty` would report "items changed" without
   localization. Equivalent functionality in PostgreSQL means fetching both versions and
   diffing application-side. (With `storeDiffs(true)` — the default — sirix additionally
   persists per-commit diff files at write time; the REST layer serves those without any
   tree traversal.)
6. **W5 excludes PostgreSQL's WAL** (`pg_total_relation_size` only). SirixDB has no
   separate WAL — its data files *are* the entire on-disk story, so the asymmetry favors
   PostgreSQL slightly. PostgreSQL's number is after `CHECKPOINT`; the history table is
   append-only (no bloat); the 5,000 dead tuples in `doc` round to 32 KB. SirixDB's
   *allocated* full-config number (37 MB) is inflated by 5,000 tiny per-commit diff files
   each occupying a 4 KB block — a filesystem packing artifact; the apparent sizes are the
   fair comparison, and the lean config shows the no-diff-files footprint.
7. **Compression asymmetry, not a tuning trick.** PostgreSQL stores a *full copy* per
   version and gets pglz for free (2.4 KB doc → 836 B/row). SirixDB stores *changed page
   fragments* plus fixed per-revision metadata (revision root, indirect pages, …) — at
   2.4 KB documents that fixed floor (~2.4–3.4 KB/version) exceeds a compressed full copy,
   so full-copy-with-compression wins. This inverts as documents grow (see §5).
8. **JIT.** Sirix W1 includes JVM warm-up (first 1,000 commits at 223/s, steady state
   444–555/s); read workloads had an untimed warm-up pass. PostgreSQL has no JIT-equivalent
   cold tax here.
9. **Dev build.** SirixDB is `1.0.0-alpha22` plus uncommitted in-progress changes, not a
   tagged release. PostgreSQL 17.10 is a GA release with two decades of tuning.
10. Serialized text differs cosmetically (`jsonb::text` reorders keys and adds spaces:
    2,665 vs 2,404 chars for the same content) — read-volume checksums differ accordingly;
    the W4 numeric checksum is identical, which is the cross-system correctness proof.

---

## 4. Where each system wins

**PostgreSQL wins, in this benchmark's regime (small docs, modest history):**

- **Durable commit throughput** — 4,015/s vs 375–429/s (5.5–10.7×). PG's per-commit work is
  one compact WAL record + one fdatasync; sirix writes a CoW page-tree (several pages),
  an fsync barrier *plus* three FUA writes, hashes, and (full config) a diff file.
- **Scan-shaped history reads** (W4: 7×, W3: 2×) — a heap scan over 5,001 compressed rows
  beats opening 5,001 revision contexts (~10 µs each).
- **Batched point-in-time reads** (4.3×) — one B-tree probe + detoast vs page-fragment
  reconstruction.
- **Storage at small doc sizes** (2.5–3.5×) — compressed full copies beat structural
  sharing when the document is barely larger than sirix's per-revision metadata floor.
- **Concurrency model** (not measured here): MVCC supports many concurrent writers across
  documents; a sirix resource has a single writer (concurrent readers).

**SirixDB wins:**

- **Per-statement read latency from an application** — 75.7 µs embedded vs ~104 µs for
  client-server PostgreSQL even over a local unix socket. There is no batching trick for
  an app that needs one document now.
- **Versioning as a first-class capability, not a pattern you build.** The PostgreSQL side
  required a trigger, a manually maintained revision sequence, an index choice, and gets:
  timestamp-only addressing, no node identity across versions, no native diff. SirixDB
  gives numbered revisions *and* timestamps (`getHistory()`), stable node keys, time-travel
  axes (`AllTimeAxis` et al.), per-node history indexes, and audit-grade rolling hashes.
- **Semantic diffs** — 0.3 ms for an exact node-level patch between any two revisions;
  PostgreSQL needs application code (and shipping both full documents to it) for the same
  answer.
- **Write/storage cost shape**: per-version cost is O(changed nodes) + ~fixed metadata,
  independent of document size — PostgreSQL's is O(document) per version (full jsonb copy
  in heap *and* WAL). At 2.4 KB this is PG's win; it cannot stay that way as documents grow.

---

## 5. What this means for SirixDB positioning

1. **Don't pitch SirixDB as "faster than PostgreSQL for keeping history of small
   documents." It is not.** For ≤ a-few-KB documents with full history, a jsonb column,
   a trigger, and an index is faster on every server-side metric and smaller on disk.
   This benchmark is exactly PostgreSQL's home turf, and it shows.
2. **The honest pitch is the cost *shape*, the capability set, and embedding.**
   - PostgreSQL's per-version cost is a full document copy (~0.35× raw after pglz here):
     a 1 MB document updated 5,000 times costs it on the order of GBs of history and MBs
     of WAL per update. SirixDB's measured per-version cost was ~2.5–3.4 KB *for a 2.4 KB
     document* and is dominated by fixed metadata, not document size — the storage and
     write-amplification crossover plausibly sits in the tens-of-KB document range.
     **That claim needs its own benchmark (100 KB / 1 MB / 10 MB docs) before quoting
     numbers — measure it, don't extrapolate in public.**
   - Sub-document time travel (field history, node-level diffs, per-node revision index)
     has no native PostgreSQL equivalent at any document size.
   - Embedded, SirixDB answers single point-in-time reads faster than client-server
     PostgreSQL can be reached at all.
3. **Commit throughput is SirixDB's weakest measured axis** (375–429/s vs ~4,800/s fsync
   floor): ~0.85 ms of the ~2.3 ms/commit is the (deliberately strong) durability
   protocol; the rest is CoW page serialization, hashing, and bookkeeping. If
   high-frequency tiny commits matter, batching updates per commit is the documented
   answer; an optional group-commit / relaxed-durability mode would be the engineering
   answer.
4. The lean-vs-full spread (429 vs 375 commits/s, 11.8 vs 16.4 MiB) quantifies the price
   of hashes + per-node history + stored diffs + path summary: ~13 % commit rate and
   ~39 % storage at this scale — worth surfacing as a tuning knob in docs, since these
   features are exactly what PostgreSQL doesn't offer.

---

## 6. Reproduction

```bash
# PostgreSQL (cleaned up after the run)
docker run -d --name sirix-bench-pg -e POSTGRES_PASSWORD=bench -p 15432:5432 \
  -v sirix-bench-pgdata:/var/lib/postgresql/data postgres:17 \
  -c shared_buffers=1GB -c synchronous_commit=on
# scripts: /tmp/wave5-b/pg/{01-schema,02-w1,03-reads}.sql, w1-client.sql, run-pg.sh

# SirixDB (embedded, prebuilt classes; classpath captured in /tmp/sirix-test-cp.txt)
javac --enable-preview --release 25 --add-modules jdk.incubator.vector \
  -cp "$(cat /tmp/sirix-test-cp.txt)" -d /tmp/wave5-b/classes \
  /tmp/wave5-b/SirixVersionedDocBench.java
java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
  -Xms1g -Xmx4g -cp "/tmp/wave5-b/classes:$(cat /tmp/sirix-test-cp.txt)" \
  SirixVersionedDocBench /tmp/wave5-b/run 5000 full   # and: ... 5000 lean

# durability floor on the same volume
docker exec sirix-bench-pg pg_test_fsync -s 2
```

---

## 7. Re-run after the W3/W4 read-path changes (2026-06-14)

The history-scan read paths were reworked to close W3/W4 (branch
`claude/versioning-gaps-postgresql-hpsbcq`). This section re-measures **W3 and W4 only**, on a
**different machine** than §1, so the numbers here are **not comparable to the table in §2** —
only to each other and to the PostgreSQL baseline re-measured alongside them.

| | |
|---|---|
| Machine | cloud VM (shared), OpenJDK 25, ext4 NVMe-backed volume — slower than §1's i7-12700H |
| SirixDB | this branch, **embedded**, default ("full") config, `StorageType.FILE_CHANNEL`, `VersioningType.SLIDING_SNAPSHOT` |
| PostgreSQL | **16.13** (local apt install, not Docker — the sandbox's docker daemon was unavailable), `shared_buffers=1GB`, `synchronous_commit=on`, `fsync=on` |
| Workload | identical to §1: one ~2.4 KB JSON doc (`counter` first field), 5,000 single-field durable commits → 5,001 versions. Cross-checks pass on both: 5,001 versions, counter sum **12,502,500**, final counter 5,000 |

Reads: 1 warm-up + 3 timed, **median** reported.

### W3 — list all 5,001 version timestamps

| Path | Time | vs PostgreSQL |
|---|---|---|
| PostgreSQL 16 (`ORDER BY valid_from`, server-side) | ~2.2 ms | — |
| SirixDB `getHistory()` (full `RevisionInfo`, optimized warm path) | ~2.4–2.8 ms | ~on par |
| **SirixDB `getHistoryTimestamps()` (new bulk API)** | **~0.05 ms** | **~40× faster** |

The new timestamp-only API serves the whole history from the resident in-memory `RevisionIndex`
(`long[]` + one `arraycopy`), with no page reads, no per-revision transactions, and no async
fan-out — so it beats a PostgreSQL heap scan by ~40× and the previous SirixDB history path
outright. **W3 gap: closed (and reversed) for the timestamp-only case; at parity for full
`RevisionInfo`.**

### W4 — one field's value across all 5,001 versions

Two regimes, because the cost shape differs by how often the field changes:

| Field | Path | Time | Record reads |
|---|---|---|---|
| `counter` (changes **every** revision) | PostgreSQL 16 | ~10.4 ms | 5,001 rows |
| | SirixDB OLD: per-revision `beginNodeReadOnlyTrx` + `moveTo` | ~82 ms | 5,001 |
| | SirixDB NEW: `scanRecordHistory` (lightweight reader) | ~76–78 ms | 5,001 |
| | SirixDB NEW: `scanValueRuns` | ~75–80 ms | 5,001 |
| `s01` (set once, **never** changes) | PostgreSQL 16 | ~9.8 ms | 5,001 rows |
| | SirixDB OLD: per-revision loop | ~80 ms | 5,001 |
| | **SirixDB NEW: `scanValueRuns` / change-set** | **~0.05 ms** | **1** |

Honest reading:

- **Dense case (`counter`):** the change set is every revision, so there is nothing to prune —
  all three SirixDB paths still read 5,001 records. The new lightweight-reader path shaves only
  ~7% off the old per-revision-transaction loop, and **PostgreSQL still wins this scan (~8×)**: a
  heap scan over 5,001 compressed rows beats opening 5,001 revision contexts. Closing the dense
  case fully needs the deferred fragment-chain scan (below).
- **Sparse case (`s01`, the common real-world shape):** `getRecordChangeRevisions` returns a
  single revision, so `scanValueRuns` reads **one** record and reconstructs the value across all
  5,001 versions — **~0.05 ms vs PostgreSQL's ~9.8 ms (~200×) and the old SirixDB loop's ~80 ms
  (~1600×)**. This is the O(changes) cost shape: PostgreSQL must scan one full-copy row per
  version regardless of whether the field changed; SirixDB reads only where it changed.

**W4 gap: closed decisively for fields that change rarely (the cost-shape win); for a field that
changes on every commit, modestly improved but PostgreSQL still leads.**

### Still deferred

A physical **fragment-chain single-pass scan** (read each distinct value once directly from the
leaf-page fragment chain, avoiding even per-revision root-page navigation) would also close the
dense case; it rewrites the storage fragment layer under SLIDING_SNAPSHOT and is left as future
work. Numbers above are medians from a shared cloud VM and PostgreSQL 16 (not 17); treat them as
indicative of the *shape* of the change, not as hardware-grade absolutes.

### Reproduction (this run)

```bash
# PostgreSQL 16, local cluster (docker daemon unavailable in the sandbox)
initdb -D $PGDATA -A trust
pg_ctl -D $PGDATA -o "-c shared_buffers=1GB -c synchronous_commit=on -c fsync=on" start
# schema: doc(id,doc jsonb) + doc_history(id,rev,valid_from,doc jsonb) via AFTER INSERT/UPDATE trigger
# ingest: CALL bench_w1(5000)   (plpgsql loop: UPDATE jsonb_set(...,'{counter}',i); COMMIT)
# W3: SELECT count(*) FROM (SELECT rev,valid_from FROM doc_history ORDER BY valid_from) s;
# W4: SELECT count(c),sum(c) FROM (SELECT (doc->>'counter')::bigint c FROM doc_history ORDER BY valid_from) s;

# SirixDB (embedded): build 5,001 versions, then time
#   session.getHistory() / session.getHistoryTimestamps()                        (W3)
#   per-revision loop  vs  session.scanRecordHistory(k,..) / scanValueRuns(k,..)  (W4)
```
