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

> **Status: the comparisons in this document are not sound.** Four independent defects were found
> in the methodology — a cold JVM (§0.9, §0.10), a RAM-resident corpus that cannot measure I/O at
> all (§0.14), an arbitrary commit granularity reported as engine throughput (§0.15), and a
> hand-rolled timing loop where JMH was needed (§0.10). Individual numbers below are annotated where
> they are known wrong, but treat NO cross-system ratio here as quotable until the matrix in
> `BENCHMARK_DESIGN.md` has been run. The SirixDB-vs-SirixDB optimisation measurements (§0.5, §0.6)
> are unaffected: those were JMH, before/after, same box and session.

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

### 0.4 Re-run: 2026-08-01, after the commit and read-path work

Full W1-W6 re-run against a same-box PostgreSQL 16, following §1's spec. The SirixDB driver is
now IN-TREE (`PostgresComparisonBench`, run via `./gradlew :sirix-benchmarks:postgresComparison`)
and the PostgreSQL side is a schema + plpgsql script — §0's re-run had to re-implement the spec
from prose because the original `/tmp` driver did not survive its machine, and that is not a
mistake worth making a third time.

| | |
|---|---|
| Machine | 4-vCPU cloud container, virtualized storage — same class as §0's box, NOT the same as §1's NVMe machine |
| Durability floor | `pg_test_fsync`: **fdatasync 1,675 ops/s (597 µs/op)** (§0's box: 1,337 ops/s / 748 µs) |
| SirixDB | this branch, embedded, `-Xms1g -Xmx4g`, `FILE_CHANNEL`, `SLIDING_SNAPSHOT`, defaults otherwise |
| PostgreSQL | **16.13**, local unix socket, `shared_buffers=1GB`, `synchronous_commit=on`, `fsync=on` |
| Document | 2,387 bytes, byte-identical input to both systems |

Cross-checks passed on both sides: **5,001 versions**; W4 count 5,001 and sum **12,502,500 on
both**; W6 identifies exactly the `counter` field.

| Workload | SirixDB (full) | SirixDB (lean) | PostgreSQL 16 | Winner |
|---|---|---|---|---|
| **W1**: 5,000 durable commits | 21.95 s = **228/s** (4.39 ms) | 16.28 s = **307/s** (3.26 ms) | server-side 4.46 s = **1,122/s** (0.89 ms) · client-driven **962/s** | PostgreSQL 3.1-3.7× |
| W1 initial insert | 170 ms | 122 ms | — | — |
| **W2**: 1,000 random PIT full-doc reads | 104.1 ms (**104 µs**/read) | 100.8 ms (**101 µs**/read) | server-side **27.6 µs**/read · client-driven **~275 µs**/read | **split — see below** |
| **W3**: history listing (5,001) | **2.27 ms** | 3.85 ms | 1.71 ms | PostgreSQL 1.3× |
| **W4**: one field across all versions | axis 24.1 / loop 26.6 ms | axis **21.6** / loop 22.7 ms | 9.5 ms | PostgreSQL 2.3× |
| **W6**: diff of adjacent versions | **0.44 ms** — node-level semantic diff | 1.08 ms | 0.46 ms — top-level compare only | **SirixDB** (equal speed, strictly more) |
| **W5**: storage for full history | 17.48 MiB | 16.56 MiB | **4.23 MiB** | PostgreSQL 3.9× |

What moved since §0. The two runs are on different machines (this box's fdatasync is ~1.25×
faster), so these deltas mix engine and machine and none of them is a clean engine measurement.
They are directionally large enough to be worth stating anyway:

- **W1 roughly doubled** (lean 169/s → 307/s) and the gap to PostgreSQL closed from ~6.5× to
  3.7×. A repeat run of the same configuration measured 356/s (2.81 ms), so treat the commit rate
  as ~300-360/s on this box, not a single figure — the per-1,000-commit windows also ramp from
  198/s to 418/s as the JIT warms, which is why W1 is reported as one timed pass over 5,000
  commits rather than a steady-state number.
- **W2 improved ~6.5×** (657 µs → 101 µs per random point-in-time full-document read), the
  largest single move, and the one most directly attributable to this branch's read-path work:
  the caches no longer re-read pages they already hold, transaction setup no longer loads the
  NamePage eagerly, and the channel pool no longer reopens per transaction.
- **W4 improved ~3×** (70.6 ms → 21.6 ms).
- **W5 got WORSE**: 9.9 → 16.56 MiB (lean). This is preallocation, and it is real bytes, not an
  artifact: `du` reports allocated ≈ apparent (17,004 KiB vs 16,961 KiB), because the zero-fill +
  fsync that keeps later in-place writes journal-free is precisely what makes those blocks
  physically allocated. Essentially all of it is one file (`sirix.data` at 16.38 MiB; the
  revisions file is 0.19 MiB). The padding is bounded per file by the adaptive chunk (cap 8 MiB),
  so at this scale it is a large fraction of the total — a 5,000-commit history of a 2.4 KB
  document is small enough that an 8 MiB tail dominates the ratio. It amortizes away for larger
  resources, but at THIS workload's size the storage gap to PostgreSQL genuinely widened from
  ~2.1× to ~3.9×, and quoting the old 2.1× would be wrong.

#### W2: which PostgreSQL number is the right one to compare against?

Both, for different questions — and the answer flips depending on which you pick, so it is worth
being explicit rather than quoting one.

| | µs per random PIT full-document read |
|---|---|
| SirixDB (embedded, in-process — no client boundary exists) | **101** |
| PostgreSQL, server-side plpgsql loop (no client round trip) | **27.6** |
| PostgreSQL, client-driven (one statement per read, unix socket, one session) | **~275** |

The client-driven figure is 320 ms of wall time for 1,000 statements minus 45 ms of `psql`
startup, and it is consistent with the 232-252 µs per-statement cost measured earlier on this
branch. The difference between PostgreSQL's two numbers is almost entirely the round trip: the
read itself is ~28 µs, so a ~250 µs client boundary dominates it by ~10×.

So: **against PostgreSQL as an application actually reaches it — over a socket, one query per
read — SirixDB is ~2.7× FASTER here** (101 µs vs ~275 µs). Against PostgreSQL's engine work with
the client boundary removed, SirixDB is 3.7× slower. SirixDB is embedded, so it has no equivalent
boundary to remove; there is no way to make this comparison perfectly symmetric, which is exactly
why §1's methodology chose the server-side framing (caveat #1) as the version least flattering to
SirixDB. The table above reports the server-side number for that reason, but reporting it ALONE
understates SirixDB's position for anyone deploying PostgreSQL over a network, where the round
trip is larger still than a unix socket's.

W1 does not have this ambiguity: it is fsync-bound at ~0.89 ms per commit, so PostgreSQL's client
round trip costs it only 1,122 → 962 commits/s and it wins under either framing.

Where this leaves the comparison: PostgreSQL wins W1, W3 and W5 outright, by margins that are now
single-digit rather than one-to-two orders of magnitude. W2 splits on framing — PostgreSQL's
engine is 3.7× faster at the read, SirixDB is ~2.7× faster than what a client actually
experiences. SirixDB wins W6 outright, at the same wall time as PostgreSQL's top-level compare
while producing an actual node-level semantic diff — and the versioning itself is free rather
than a hand-maintained history table plus trigger.

### 0.5 Follow-up: the serializer stopped converting text the output never needed

§0.4's W2 breakdown put ~87 % of a full-document read in serialization rather than navigation, so
that is where the next round went. Three things were wrong with it, all of them conversion or
allocation that the emitted bytes did not require:

- **The output chunk was allocated at its ceiling, per serialize call.** A serializer is
  constructed per call, and each one allocated 8 KiB of `char[]` (16 KB) up front whatever the
  document's length. On a 2.4 KB document that single array was **93.8 % of everything the
  operation allocated** (JFR, 400k iterations: 10.73 GB of 11.44 GB). The chunk now starts at 256
  and doubles to the same ceiling, so a document that fits still reaches the target in one
  downstream write and the allocation tracks what is actually written.
- **Object keys were decoded, wrapped, escaped and quoted — four allocations per named node.**
  `getName()` builds a `String` from the dictionary's UTF-8 bytes and a `QNm` around it, then the
  serializer escaped and quoted that. It kept only the last result, in a per-call hash map that
  cannot hit on a document whose field names are distinct: on the 29-member benchmark document the
  map took 29 misses, two rehashes and one quoted `String` each, and never a hit.
- **String values took the same route on the char pipeline.** The byte sink already had a
  raw-UTF-8 fast path; the char sink had none, so every value paid a decoded `String` plus an
  escape-scan copy.

Keys and values now start from the bytes the node stores. A pre-scan classifies them and each sink
copies verbatim when it passes — the byte sink for any escape-free UTF-8 run, the char sink for
plain ASCII, where widening byte→char *is* the UTF-8 decode. Anything the scan rejects falls back
to the exact escape path unchanged.

JMH, `AverageTime`, µs/op, 5 warmup + 12 measurement iterations, one fork. **Both rows were
measured in the same session on the same box**, because this box wanders: the borrowed-cursor
baseline reads 19.748 here where `569cadf` recorded 17.583 for the very same code, a 12 % session
drift that would swamp a smaller claim than this one.

| Probe | before | after | change |
|---|---|---|---|
| Serialize the full document through a client's open transaction | 19.748 ± 1.676 | **12.401 ± 0.585** | **−37 %** |
| Serialize the full document, serializer opens its own transaction | 42.274 ± 0.994 | **35.328 ± 4.544** | **−16 %** |

Both intervals are non-overlapping. The owning row moves less because most of what it adds over
the borrowed row is transaction setup, which this work did not touch.

> **Correction (see §0.6).** That last sentence was an assumption, and it was wrong. Nothing here
> measured the transaction open. When §0.6 finally did, it came to 2.4 µs against a gap of about
> 20 µs; almost all of the rest was the revision's name dictionary being rebuilt on the first name
> lookup of every read transaction.

What is left is no longer the serializer. After the change, roughly 60 % of a borrowed-cursor
serialization is the cursor and page layer — `moveToSingleton`'s flyweight bind, the per-field
`getFirstChildKey`/`getRightSiblingKey` interface calls, and `MemorySegment` bounds checks — and
the emitter's own share (escape scan, chunk buffer, bracket emission) is about a quarter.


### 0.6 Follow-up: the cursor, and the read a fresh transaction was paying for three times over

§0.5 left two things: the cursor and page layer, now about 60 % of a borrowed-cursor
serialization, and an unexplained gap — an owning-transaction read cost roughly three times a
borrowed one. Both are addressed here, and the second turned out not to be what §0.5 assumed.

#### The cursor

Two changes, each measured on its own against the commit before it.

**The four structural keys were decoded on every ask.** A full-document serialization asks for the
first-child and right-sibling key about three times per node each — once in
`DescendantAxis.nextKey`, once in the emitter, once in `AbstractSerializer.serializeRevision` — and
each ask paid an `instanceof`, a megamorphic `StructNode` call over a dozen singleton types, and a
delta-varint decode out of the page's `MemorySegment`. Nothing but a reposition can change those
values, so the second and third ask read what the first had already found. Each key is now decoded
at most once per position into a primitive field, with a bitmask cleared by every repositioning
entry point. It is populated lazily rather than eagerly at `moveTo` time because a point read
touches no structural key at all. Write transactions never cache: a writer mutates the record under
its own cursor in place, without repositioning, so there is no moment at which the mask could be
invalidated.

**The read path allocated a `MemorySegment` view per `moveTo`.** `KeyValueLeafPage.getSlot` builds
a slice over the record; the flyweight path — the one JSON takes — reads one kind byte out of it
and then binds the singleton straight to the page, so the slice was allocated and discarded on the
single hottest line of the read path. The write-side move already read its slot in place; this is
the same treatment on the read side.

| Probe | before | struct-key cache | + slot in place |
|---|---|---|---|
| Serialize full document, borrowed cursor | 12.541 ± 0.364 | **10.103 ± 0.284** | 9.613 ± 0.178 |
| Serialize full document, `maxLevel(2)` | 23.888 ± 0.965 | **20.928 ± 0.762** | 21.153 ± 1.255 |
| Point read on a held cursor | 0.041 ± 0.001 | 0.042 ± 0.001 | **0.033 ± 0.001** |
| Open a transaction and point read | 2.572 ± 0.069 | 2.576 ± 0.054 | 2.485 ± 0.089 |

Read the diagonal. The struct-key cache moved the two serialize rows and left the point read flat —
it only removes *repeat* asks, and a point read makes none. The slot change then moved the point
read by 21 %, which is one `moveTo` and nothing else. Each change is visible on the probe the other
could not touch, which is what makes either number attributable. The third column carries the
allocation profiler's overhead, so it is if anything pessimistic against the second.

One hypothesis was tried and **rejected**: dispatching `FlyweightNode.bind` on the already-decoded
kind, to replace a megamorphic interface call (twenty-one implementations, once per traversal step)
with a monomorphic one. It cost 6 % on the single-`moveTo` probe and 10 % on the owning serialize —
two independent probes, both non-overlapping. The tableswitch is worse than the itable stub it
replaces. Do not retry it.

#### The gap, and what was actually in it

The owning-vs-borrowed gap was attributed in §0.5 to transaction setup. Nothing had measured it, and
the probes then in the tree could not: `openTransactionAndPointRead` conflates the open with a read,
and the two serialize probes conflate the cursor with the emitter. Six probes now split it — five of
them new — each differing from its neighbour by exactly one thing:

| Probe | µs/op | B/op |
|---|---|---|
| Open a transaction and close it, no traversal | 2.38 | 3,896 |
| Walk every node, held cursor, no serializer | 1.97 | 120 |
| Walk every node, own transaction | 6.14 | 5,016 |
| Read one **value** through a fresh transaction | 2.66 | 3,952 |
| Read one **name** through a fresh transaction | **16.22** | **22,312** |
| Read the same name on a held cursor | 0.05 | 72 |

The open is 2.4 µs. A cold cursor costs the traversal itself about 1.8 µs more than a warm one.
Together that is nowhere near a 20 µs gap — and the last three rows say where it went. A value read
through a fresh transaction costs 2.66 µs; a *name* read through one costs 16.22 µs and allocates
22 KB, against 0.05 µs and 72 B once the transaction is warm.

`NamesCache.get(key, mappingFunction)` was `cache.asMap().compute(key, mappingFunction)`, and
`compute` invokes the mapping function on a **hit**. That function is
`Names.copy(Names.fromStorage(…))` — walk the revision's whole name dictionary out of storage, then
copy it — so every read transaction rebuilt the dictionary on its first name lookup and wrote the
result back over the cached entry. This is the same defect §0.2 fixed in `PageCache` and
`RevisionRootPageCache`; it had survived in a third cache, on the path every serialized field name
goes through.

| Probe | before | after |
|---|---|---|
| Read one name through a fresh transaction | 16.219 ± 0.409 | **6.046 ± 0.261** |
| — allocation | 22,312 B/op | **6,920 B/op** |
| Serialize full document, own transaction | 29.470 ± 0.996 | **17.435 ± 0.732** |
| — allocation | 35,344 B/op | **19,976 B/op** |
| Serialize full document, borrowed cursor | 9.486 ± 0.561 | 9.790 ± 0.227 |

The borrowed row is the control and does not move: a held cursor resolved the dictionary once, long
ago. The owning row is what a request-per-transaction API pays, which is exactly the framing the
client-driven PostgreSQL number is measured in.

Every remaining cache was then audited for the same defect. `PathSummaryCache` had it (no live
caller yet, fixed as a trap); the `Cache` interface default had a variant of it — it wrote the
computed value twice on a miss, and reached it through `asMap()`, whose own default throws, which
made the method unusable on `LRUCache` and `EmptyCache`. `ShardedPageCache`
and `PerResourceRevisionFileDataCache` were already correct.

Taken together, across the session and on one box: an owning-transaction full-document read went
**29.3 → 17.4 µs**, and a borrowed-cursor one **12.5 → 9.8 µs**.

#### …and the page underneath the dictionary

Fixing the cache left a residue: a name read through a fresh transaction was still 6.05 µs against
2.62 µs for a value read on the same node. The dictionary was no longer rebuilt, but the page
holding it was. `NamePage` sits on `PageCache`'s index-root exclusion list — correctly, because
sharing one index-root instance would let a time-travel read of revision N follow revision N+1's
root — so every transaction deserialized it afresh on its first name access, found its dictionary
field null, and re-entered `getNames`. That is also *why* the defect above cost what it did: it was
reached once per transaction, not once ever.

The dictionary needs no page. `NamesCacheKey` is `(database, resource, revision, offset)` — the
revision in that key is exactly what the reference-keyed `PageCache` lacks, and what makes the
dictionary safe to share where the page is not. A reader now consults that cache first and only
falls back to the page on a miss, or when it is a writer (whose uncommitted names live in its
transaction-intent log, which `NamePage.getNames` already refuses the shared cache for).

Which dictionary a kind belongs to is now decided in two places that must agree — the cache key and
the page's own switches — so the mapping is single-sourced as `NamePage.dictionaryOffset`, and
`NameResolutionCachePathTest` reads every name twice, once through the transaction that builds the
dictionary and once through transactions that take the cached path, across all four XML
dictionaries and JSON's. A disagreement would resolve names against the wrong dictionary silently,
and only once warm.

The first cut of that mapping threw for kinds it did not know, and 53 tests said no. The three sets
are deliberately not the same: `getName` answers `ARRAY` and `OBJECT` with the synthetic
`__array__` / `__object__` literals and consults no dictionary at all — the path summary asks it for
exactly those — while `getRawName` does not accept them. So the mapping *declines*
(`NO_DICTIONARY`) instead, and the shortcut falls back to the page whenever it does. That keeps the
page the authority for every kind the shortcut does not name, and makes a kind added there but
forgotten here merely slow rather than wrong.

| Probe | before | after |
|---|---|---|
| Read one name through a fresh transaction | 6.046 ± 0.261 | **2.217 ± 0.066** |
| — allocation | 6,920 B/op | **4,088 B/op** |
| Serialize full document, own transaction | 17.435 ± 0.732 | **13.456 ± 0.419** |
| — allocation | 19,976 B/op | **17,120 B/op** |
| Read one value through a fresh transaction | 2.624 ± 0.264 | 2.464 ± 0.053 |
| Read the same name on a held cursor | 0.053 ± 0.001 | 0.050 ± 0.002 |
| Serialize full document, borrowed cursor | 9.790 ± 0.227 | 9.952 ± 0.414 |

The first row is now *below* the value-read row: resolving a name through a fresh transaction costs
what resolving a number does, which is what it should always have cost. Both controls are flat.

#### Where the session ends up

On one box, across the whole round:

| Probe | before | after |
|---|---|---|
| Serialize full document, own transaction | 29.283 ± 0.605 | **13.456 ± 0.419** — **−54 %** |
| Serialize full document, borrowed cursor | 12.541 ± 0.364 | **9.952 ± 0.414** — **−21 %** |
| Point read on a held cursor | 0.041 ± 0.001 | **0.033 ± 0.001** — **−20 %** |

The owning row is the one that matters for the PostgreSQL framing: it is what an API that opens a
transaction per request pays, which is how the client-driven PostgreSQL number is measured. **Every
W1–W6 read figure in §0.4 predates all of this and must be re-run before any read ratio against
PostgreSQL is quoted again.**


### 0.7 Re-run: W1-W6 after the serializer, cursor and cache rounds

§0.4's table predated §0.5/§0.6 entirely, so every read number in it was stale. Re-run here on the
same box, same spec, PostgreSQL 16 rebuilt from scratch (`shared_buffers=1GB`,
`synchronous_commit=on`, `fsync=on`). Cross-checks passed on both sides again: 5,001 versions, W4
count 5,001 and sum **12,502,500 on both**, W6 identifying exactly the `counter` field.

| Workload | SirixDB full | SirixDB lean | PostgreSQL 16 | vs §0.4 (lean) |
|---|---|---|---|---|
| **W1**: 5,000 durable commits | 264/s (3.78 ms) | **347/s** (2.89 ms) | server-side **1,123/s** (0.89 ms) | 307 → 347/s |
| **W2**: 1,000 random PIT full-doc reads | **81.9 µs**/read | 90.4 µs/read | server-side **23.5 µs** · client-driven **~266 µs** | 101 → 90 µs |
| **W3**: history listing (5,001) | **2.44 ms** | 4.61 ms | 1.82 ms | 3.85 → 4.61 ms |
| **W4**: one field across all versions | axis 32.0 ms | axis **28.9 ms** | **8.69 ms** | 21.6 → 28.9 ms |
| **W6**: diff of adjacent versions | **0.49 ms** (node-level) | 1.32 ms | 0.60 ms (top-level only) | 1.08 → 1.32 ms |
| **W5**: storage for full history | 17.49 MiB | 16.56 MiB | **4.23 MiB** | unchanged |

**The read-path gains do NOT show up here at anything like their micro-benchmark size, and that is
the most useful thing in this table.** §0.6's probes have a full-document serialization through a
fresh transaction at 42.3 → 13.5 µs (-68 %) and through a borrowed cursor at 19.7 → 10.0 µs. W2
moved 101 → 90 µs, about -10 %.

The explanation is the workload shape, and it is worth internalising before optimizing further.
W2 reads a revision picked at random from 5,001, so nearly every read has to RECONSTRUCT that
revision from sliding-snapshot page fragments on disk. The JMH probes read one warm, recent
revision out of cache. So W2 is dominated by page reads and version reconstruction, not by the
cursor and emitter work those rounds improved — the ~10 % it did move is roughly the share the
serializer had left in this shape. **Random-point-in-time reconstruction, not serialization, is
what would move W2 next.**

W3, W4 and W6 came out slightly WORSE than §0.4 (W4 most visibly, 21.6 → 28.9 ms). Nothing in
§0.5/§0.6 touches those paths, and the box's own run-to-run drift is documented at ±12 % on
unchanged code (§0.6) with the W2 breakdown probes swinging 151 → 288 µs for identical code. Read
these three as noise, not regression — but they are reported as measured rather than quietly
re-run until they looked better.

Unchanged from §0.4 and still true: W2 splits on framing (SirixDB at 82-90 µs is ~3× faster than
what a PostgreSQL client actually experiences at ~266 µs, and ~3.5-3.9× slower than PostgreSQL's
engine work in isolation), and W5's regression is real preallocated bytes, not an accounting
artifact.

### 0.8 Where W2's 89 µs actually goes (10 runs, and two ruled-out fixes)

§0.7's single-run numbers were within this box's drift, so W1-W6 was re-run **ten times** (lean).
Median and mean agree closely, so no outlier is carrying the result:

| metric | median | mean | min | max | spread |
|---|---|---|---|---|---|
| W1 commits/s | **329.5** | 328.5 | 297 | 348 | 15 % |
| W2 µs/read | **89.2** | 91.9 | 76.9 | 128.0 | 57 % |
| W3 ms | 3.3 | 3.3 | 2.2 | 4.2 | 60 % |
| W4 axis ms | 26.1 | 27.2 | 21.7 | 34.0 | 47 % |
| W6 ms | 0.9 | 0.9 | 0.8 | 1.2 | 48 % |

The spread means **this harness cannot resolve anything smaller than ~50 %** — use JMH for that, and
read §0.7's "W3/W4/W6 got slightly worse" as the noise it is.

W2 decomposes cleanly, and the decomposition says the engine is doing what the micro-benchmarks
promised:

| | µs/read |
|---|---|
| Same revision read repeatedly (`W2 FIXED`) | **~40** |
| Random revision out of 5,001 (W2 proper) | **~74-89** |

- **The ~40 µs base is exactly as predicted.** §0.6 measures a full-document serialization through a
  fresh transaction at 13.5 µs on a ~45-member document; W2's document has 131 nodes, so ~3× is
  ~40 µs. There is no unexplained cost in a SirixDB read — it is serializer-dominated and that is
  the lever already being worked.
- **The remaining ~35-49 µs is the price of landing on a random revision**, and two obvious
  explanations are now **ruled out by measurement**:
  - *Not* sliding-snapshot fragment reconstruction. `VersioningType.FULL` stores every revision
    complete so nothing has to be rebuilt on read — and it is **worse**: 143 µs/read and 24.56 MiB
    (against 74-89 µs and 16.56 MiB). Removing the CPU work adds enough bytes to read that it loses.
  - *Not* the `PageCache` entry cap. Raising `sirix.cache.page.max.entries` from 50,000 to 500,000
    changed W2 by less than the noise floor (73.2 vs 74.0 µs).

  What is left is bytes actually read for pages of an old revision that are not resident — record-page
  cache misses, i.e. real I/O. **That is where a further W2 win has to come from**, and it is a
  different subsystem from the serializer, the cursor and the metadata caches this branch has worked.

One framing point worth keeping in view when reading W2 at all: **the two systems are not doing
equal work.** PostgreSQL fetches ONE jsonb blob via an index scan and prints it. SirixDB materializes
a 131-node tree out of a versioned page trie and serializes it. That SirixDB lands within ~3.8× of
PostgreSQL's in-process engine while doing structurally more — and ~3× FASTER than what a PostgreSQL
client over a socket actually experiences (~266 µs) — is the honest summary of this workload.

### 0.9 Correction: W2 was measuring a cold JVM, and this harness should be JMH

**Every W2 number in §0.4, §0.7 and §0.8 is overstated by roughly 30 %.** The harness ran ONE
untimed warm-up pass before three timed ones and reported their median. Printing the individual
passes shows that was not enough:

| warm-up passes | timed passes (µs/read) |
|---|---|
| 1 (what those sections used) | 96.1 → 79.6 → **73.3**, still falling steeply |
| 8 | 75.6 → 61.2 → **61.0**, converged |

So W2 (lean) is **~61 µs/read**, not the 89-90 µs reported. The median of three still-declining
passes is an artifact of where JIT compilation happened to stop. The driver default is now 8
(`-Dpgcmp.w2.warmups=N` to change it), and it prints every timed pass so the question is answerable
from the output rather than assumed.

Corrected, W2 stands at **~61 µs (SirixDB) vs 23.5 µs (PostgreSQL server-side) and ~266 µs
(PostgreSQL client-driven)** — 2.6× behind PostgreSQL's in-process engine, ~4.4× ahead of what a
PostgreSQL client over a socket experiences.

**These workloads should be JMH benchmarks.** Warm-up, forking, dead-code elimination and
statistically honest reporting are exactly what JMH does and what a hand-rolled timing loop gets
wrong — this section is that mistake, found only because the per-pass timings were finally printed.
`DurableCommitBenchmark` (JMH) has been the trustworthy half of this document all along, which is
why its numbers moved consistently while the W1-W6 numbers wandered. Porting W1-W6 is the right fix;
raising the warm-up count is a patch.

Two further methodology gaps, recorded rather than quietly ignored:

- **The JVM is stock OpenJDK 25 (HotSpot C2), not GraalVM**, which §1's methodology specifies. The
  original June baseline ran GraalVM JDK 25.0.3. Nothing in §0 onward has been measured on the
  compiler the comparison was designed around, and GraalVM's JIT typically differs on exactly this
  kind of code — heavy virtual dispatch, allocation, string handling.
- **Native image with profile-guided optimization has never been measured at all.** For an embedded
  engine that is a plausible deployment, and it removes JIT warm-up from the picture entirely — the
  very thing that made these numbers wrong.

One correction to §0.8's framing while it is being corrected: it said PostgreSQL "fetches ONE jsonb
blob via an index scan and prints it", implying the printing is trivial. It is not — `doc::text`
walks the binary jsonb tree and produces text, which is structural work of the same kind SirixDB
does. The genuine difference is one contiguous blob against a versioned node trie, not work against
no work.

### 0.10 W1-W6 as JMH, and HotSpot C2 vs the Graal JIT

`VersionedDocWorkloadBenchmark` ports the read workloads to JMH, which is what §0.9 concluded they
needed. W5 is deliberately not ported — bytes on disk is not a per-operation latency. W1 is present
but excluded from these runs: it commits, so it grows the history as it measures, and over a full
JMH run that adds tens of thousands of revisions underneath the read probes.

Same box, same 5,001-revision history, 3 warm-up + 6 measurement iterations, one fork.
**OpenJDK 25.0.3 (HotSpot C2)** against **Oracle GraalVM 25.0.4+7.1** — the newest available; the
`graalvm/26` and `/27` download paths both 404.

| Workload (lean) | HotSpot C2 | Graal JIT | |
|---|---|---|---|
| W2 fixed revision | 33.95 ± 3.48 | 36.84 ± 4.03 | indistinguishable |
| W2 random point-in-time | 95.95 ± 24.14 | 119.59 ± 47.17 | **unresolvable** — see below |
| W3 history listing | 110.86 ± 12.79 | **64.32 ± 4.84** | **Graal 1.7× faster**, non-overlapping |
| W6 adjacent diff | 68.54 ± 6.21 | 69.18 ± 5.36 | indistinguishable |

**The Graal JIT is not a general win here.** It is clearly faster on W3 and statistically
indistinguishable on everything else. That is worth knowing precisely because the opposite was the
working assumption — §1's methodology specifies GraalVM, and §0 onward had been running HotSpot
without anyone noticing.

**Two numbers in the earlier sections were badly wrong, and JMH is how that surfaced:**

- **W3 was reported as 3.3 ms; it is ~65-110 µs.** Thirty times off. The hand-rolled harness timed
  it three times per run against a cold JIT.
- **W6 was reported as ~0.9 ms; it is ~34-69 µs.** Also more than ten times off.

Both were measured the same way W2 was, and §0.9 only caught W2. The whole read half of §0.4/§0.7/§0.8
should be read as an artifact of cold measurement, not just its W2 column.

**W2's random-PIT number is the one JMH does NOT settle**, and the reason is instructive. Its error
bars are ±25-40 % on both JVMs because each invocation draws a *different* revision, so per-op cost
genuinely varies with what is resident. Note it also comes out HIGHER under JMH (96-120 µs) than the
61 µs §0.9 arrived at — because §0.9's loop re-read the same 1,000-revision sample several times, so
that sample became cache-warm. JMH's shape, where a fresh revision is drawn per invocation, is the
more honest model of a random point-in-time workload, and it says the real cost is ~96-120 µs with
wide variance. **§0.9's 61 µs was itself optimistic; treat ~100 µs as the number.**

### 0.11 Native image with PGO: not done

Requested, attempted, not delivered — recorded rather than quietly dropped. The toolchain is present
and working (`native-image` on this box builds a hello-world in 49 s with Oracle GraalVM 25.0.4), so
this is not a tooling gap. What makes it a separate project rather than another benchmark run:

- `sirix-core` uses `jdk.incubator.vector`, the FFM API (`Arena`, `MemorySegment`) throughout the
  page and node layer, and SPI/reflection for storage-provider lookup — each needs native-image
  configuration before the image will build at all;
- JMH generates and reflectively loads its own harness classes, so benchmarking under native image
  needs either generated reflection config or a different harness;
- PGO is a three-step cycle per configuration (instrumented build → representative run → optimized
  rebuild), and each build of a codebase this size is long.

It is a genuinely interesting question for an embedded engine — a native image has no JIT warm-up at
all, which is precisely the failure mode that made §0.4-§0.8's numbers wrong. It just is not a thing
that fits in the tail of a session.

### 0.12 Correction: W2 is NOT I/O bound — §0.8's conclusion was wrong

§0.8 concluded that W2's random-revision penalty was "bytes actually read for pages of an old
revision that are not resident — record-page cache misses, i.e. real I/O". That was reasoning, not
measurement, and it is **wrong**. Running the identical benchmark with the whole database on tmpfs,
so that no read can possibly touch a disk:

| (lean) | on disk | on tmpfs (entirely RAM) |
|---|---|---|
| W2 random point-in-time | 119.6 ± 47.2 | 124.0 ± 17.1 |
| W2 fixed revision | 36.8 ± 4.0 | 39.0 ± 11.2 |

Indistinguishable. The ~85 µs that separates a random-revision read from a fixed-revision one is
**CPU**: page decompression and node materialization for pages that are not resident in the in-JVM
caches. The bytes were already in RAM the whole time — a 16.56 MiB file on a box with gigabytes of
page cache was never going to be disk-bound after the first pass.

The same reasoning applies to the other side of the comparison, and is worth stating because it
frames every read number in this document: **PostgreSQL is not I/O bound here either.** Its dataset
is 4.23 MiB against `shared_buffers=1GB`, so after the first pass every read is a buffer hit. Its
23.5 µs server-side W2 is pure CPU: a b-tree descent plus `jsonb` → text of a contiguous blob.

So the honest statement of the read comparison is: **both systems are CPU-bound in these
measurements, and SirixDB spends ~4× more CPU per random point-in-time read than PostgreSQL does.**
That is a fair fight and a real gap — it is not explained away by I/O, by the client boundary, or by
SirixDB "doing more work", though the last is partly true (131 nodes materialized against one blob
decoded). The lever is per-page decode and cache residency, not storage.

### 0.13 Cold-start latency: requested, not yet measured

What every number in this document measures is **warm** steady state. The complementary question —
what does the FIRST read cost after a cold start, with the OS page cache dropped and PostgreSQL's
shared buffers empty — is not answered anywhere here, and it is the question a deployment actually
faces on restart.

It needs a harness neither driver currently has, because JMH is built to eliminate exactly the
effect being measured:

- SirixDB: build the resource, close the database, `echo 3 > /proc/sys/vm/drop_caches`, reopen, and
  time the first N reads individually (not an average over a pass — the shape of the curve IS the
  result);
- PostgreSQL: same cache drop plus a server restart to empty `shared_buffers`, then time the first N
  statements;
- both need per-read timings rather than aggregates, and a single measured run rather than a median
  over passes, since by the second pass the state is warm and the measurement is gone.

Worth doing: a system whose warm read is 4× slower can still win a cold start if it reads fewer
bytes to answer the query, and this document currently cannot say either way.

### 0.14 The dataset is far too small, and that limits what any of this can claim

The most consequential problem with everything above: **the working set fits entirely in RAM on
both sides.** SirixDB's full history is 16.56 MiB and PostgreSQL's is 4.23 MiB, on a box with 15 GB
of memory and a 1 GB PostgreSQL buffer pool. §0.12 established that neither system touches a disk
during a measured read — that is not a property of the engines, it is a property of a benchmark whose
entire corpus is smaller than a CPU's share of L3-adjacent memory bandwidth would care about.

So what this document actually measures, honestly stated, is **cache-resident CPU cost on a toy
corpus**. That is a legitimate thing to measure — it is where the serializer and cursor work in
§0.5/§0.6 paid off, and those gains are real. It is NOT a database comparison, and no claim about
how either system behaves under a realistic working set can be drawn from it.

What a meaningful run requires, from this document's own measured rates:

| | per revision | revisions for 2 GB | for 20 GB |
|---|---|---|---|
| SirixDB (lean, sliding snapshot) | 3.39 KiB | ~618,000 | ~6.2 M |
| PostgreSQL (jsonb + history table) | 0.87 KiB | ~2.4 M | ~24 M |

Two things to get right when doing it, or the run answers nothing:

1. **2 GB is the minimum, and it only defeats the caches, not the RAM.** It exceeds PostgreSQL's
   1 GB `shared_buffers` and any plausible in-JVM cache, so buffer-miss paths finally execute — but
   this box has 15 GB, so the OS page cache still absorbs the physical I/O and the comparison stays
   CPU-bound at the device level. For a genuinely I/O-bound measurement the corpus has to exceed
   available RAM (~20 GB here), or the run has to drop caches / use a cgroup memory limit / open
   O_DIRECT.
2. **The two systems must be sized by BYTES, not by revision count.** SirixDB stores ~3.9× more per
   revision (§0.7's W5), so matching revision counts hands PostgreSQL a working set 3.9× smaller and
   quietly makes the cache comparison meaningless. Equal-bytes and equal-revisions are different
   experiments; run whichever, but say which.

Cost estimate so this is not started blind: at the measured ~330 commits/s (SirixDB, lean) 618,000
commits is roughly **31 minutes of ingest alone**, per configuration, before any read workload runs.
PostgreSQL at ~1,120/s needs ~37 minutes for its 2.4 M revisions. A full equal-bytes matrix across
both configurations is a multi-hour job, which is why it is written down here rather than squeezed
into the end of a session.

**Partially addressed (2026-08-01):** a ~2 GB corpus has now been measured, though as a *bulk*
workload rather than this document's versioned one — see
[`COMPARISON_POSTGRES_BULK.md`](COMPARISON_POSTGRES_BULK.md). It confirms this section's central
worry from the other direction: at 2 GB with sub-TOAST-threshold records, PostgreSQL's `jsonb`
does not compress at all and SirixDB's storage is **1.95× smaller** — the exact inverse of §2's
small-document result, and a difference the 16 MiB corpus could never have surfaced. The
equal-bytes *versioned* matrix specified below is still unrun.

Until it is run, **every read number in this document should be read as "cache-resident CPU, 16 MiB
corpus"** — including the ones that look favourable.

### 0.15 W1 measures commit FREQUENCY, not engine throughput

Worth asking of the workload itself: why one commit per changed field? §1 specifies it, and it does
model a real pattern (audit trails, event sourcing, per-event durability). But it is also the single
worst shape for any copy-on-write store — a full CoW path, a new revision root, an indirect-page
rewrite and the dual-beacon protocol, all amortized over ONE changed node — and because commit is
fsync-bound, it makes W1 largely a measurement of the device's fsync latency, where both systems sit
near the same floor. That is why PostgreSQL "wins" W1 by a stable ratio no engine work has moved.

Measured with `-Dpgcmp.w1.changesPerCommit` (2,000 commits, lean):

| changes per commit | ms/commit | **changes/s** | storage |
|---|---|---|---|
| 1 | 3.98 | 251 | 4.31 MiB |
| 8 | 3.87 | **2,072** | 4.31 MiB |

**Eight times the work per commit costs the same wall time and the same bytes.** The per-commit cost
is essentially all fixed overhead; the changed nodes are nearly free, and eight changes land in the
same page so the history does not grow either. Measured in useful work rather than commit calls,
SirixDB does 8× more at identical durability.

This reframes W1 entirely. As written it does not compare ingest engines — it compares two
implementations of "make one field durable", which is dominated by fsync on both sides and by fixed
per-commit overhead on SirixDB's. **A throughput comparison has to sweep batch size**, and report
changes/s rather than commits/s, or it is reporting the benchmark author's choice of commit
granularity as if it were a property of the database.

PostgreSQL will amortize the same way — its commit is fsync-bound too — so the sweep is needed on
both sides before anything is concluded. The point is not that SirixDB looks better at batch 8; it
is that a single fixed batch size cannot support a throughput claim about either system.

### 0.3 Reproduction

Latency micro-benchmarks (§0.2):

```
./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*DurableCommitBenchmark.*' \
    -Pjmh.warmupIterations=5 -Pjmh.iterations=12
```

For a before/after, run the same harness in a worktree at the baseline commit (the benchmark file
copies over cleanly except for the borrowed-cursor probe, whose Builder overload does not exist
there).

The serializer rows of §0.5 are the two `serialize*` probes of that same benchmark; run only those
and you get an answer in ~90 s per side:

```
./gradlew :sirix-benchmarks:jmh -Pjmh.includes='DurableCommitBenchmark.serialize.*' \
    -Pjmh.warmupIterations=5 -Pjmh.iterations=12 -Pjmh.fork=1
```

Take the baseline the same way, back to back — `git checkout <baseline> -- bundles/sirix-core/src`,
rerun, restore. **Do not** compare against a figure recorded in an earlier session; §0.5 shows the
same code drifting 12 % between two of them. If the gradle wrapper reports an error bar larger than
its mean (it did once here, 652 ± 1431 µs on a probe whose real value is ~26 µs), re-run the JMH jar
directly with `-v EXTRA` and read the per-iteration lines before believing it:

```
java --add-modules=jdk.incubator.vector --enable-preview --enable-native-access=ALL-UNNAMED \
    -jar bundles/sirix-benchmarks/build/libs/sirix-benchmarks-*-jmh.jar \
    'DurableCommitBenchmark.serialize' -wi 5 -i 12 -f 1 -v EXTRA
```

Full W1-W6 comparison (§0.4) — SirixDB side, both configurations:

```
./gradlew :sirix-benchmarks:postgresComparison -Ppgcmp.args='5000 1000 lean'
./gradlew :sirix-benchmarks:postgresComparison -Ppgcmp.args='5000 1000 full'
```

PostgreSQL side, against a server with `shared_buffers=1GB`, `synchronous_commit=on`, `fsync=on`:

```
createdb bench
psql -d bench -f docs/bench/postgres-comparison-schema.sql
psql -d bench -c "INSERT INTO doc (id, doc) VALUES (1, \$json\$$(cat docs/bench/postgres-comparison-doc.json)\$json\$)"
psql -d bench -c '\timing on' -c 'CALL bench_w1(5000)' \
    -c 'SELECT bench_w2(1000)' -c 'SELECT bench_w3()' -c 'SELECT * FROM bench_w4()' \
    -c 'SELECT count(*) FROM bench_w6(2500, 2501)'
psql -d bench -c 'CHECKPOINT' \
    -c "SELECT pg_total_relation_size('doc') + pg_total_relation_size('doc_history')"
```

`docs/bench/postgres-comparison-doc.json` holds the exact document bytes both systems ingest;
`PostgresComparisonBench.buildDocument()` regenerates the same string, so the two stay in step.

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
