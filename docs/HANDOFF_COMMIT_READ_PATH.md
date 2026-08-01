# Handoff — commit protocol & read-path work

Working note for whoever picks this up (including a future session with no context). Delete once
the branch is merged and the open items below are either done or filed as issues.

Branch: **`claude/sirix-json-duckdb-inspiration-qj3d7v`** in `sirixdb/sirix`, and a branch of the
same name in `sirixdb/sirix-enterprise`. Everything described here is pushed. **Treat the remote
as the source of truth** — see "Environment gotchas".

---

## 1. What shipped

Each commit was verified against the full `sirix-core` suite (~9,670 tests) before pushing.

| Commit | What |
|---|---|
| `9fac721` | Beacon-embedded revision-record tail log — durable commit goes from 3 device round-trips to 2 |
| `d0cbc75` | Caches stopped re-reading pages they already held; write-frontier handoff; reader channel linger; lazy NamePage; `JsonSerializer.Builder(rtx, …)` client-lent cursor |
| `1abf9a8`, `f879447` | `DurableCommitBenchmark` (JMH) + measured before/after in `COMPARISON_POSTGRES.md` §0.2 |
| `e238106` | Writer channel pooling; idempotent `close()`; `RevisionRecordDurability` moved to `io.sirix.io` |
| `807cd82` | O(1) cache-ownership probe replacing a full-cache scan per orphaned page |
| `b341426` | Per-commit flush buffer recycled instead of abandoned |
| `440b577` | Commit marker created in one syscall, without a check-then-act race |
| `20fa67b`, `2219c66` | Full W1–W6 PostgreSQL re-run (§0.4), harness vendored in-tree, both PG framings reported |
| `e958063`, `795996f` | W2 cost breakdown as permanent benchmark instrumentation |
| `569cadf` | Serializer: hoisted an invariant out of the end-element loop — −19/20% on full-document serialization |

`sirix-enterprise` `669ddf0`: the same tail-log change for the io_uring backend (4 fsyncs per
commit → 3), plus a fix to `FFMIOUringWriter.truncateTo`, whose signature no longer matched
core's `Writer` interface. **That break predates this work** — verified by building without the
change — and blocked compiling the module at all.

### The two ideas worth carrying forward

**Lazy revision records.** The 32-byte per-commit record no longer gets its own fsync. It rides a
checksummed 16-entry ring in the trailing 768 bytes of *both* uber-beacon slots, staged before the
write-ahead barrier that already hardens the data file, so the record is durable exactly when its
beacon is. Three invariants keep this from weakening recovery: an **eviction guard** forces the
revisions file before a ring slot is reused, so no committed record ever depends on the ring alone;
the reader treats the ring as a **salvage source, never a second opinion** (consulted only after
the file's own record fails its checksum, then healed back in place); and the ring image passes
**writer-to-writer** through `RevisionRecordDurability` rather than being re-derived per commit.
Spec in `docs/DISK_FORMAT.md`; coverage in `LazyRevisionRecordRecoveryTest`.

**The caches were re-reading what they already held.** `PageCache.get(ref, fn)` and
`RevisionRootPageCache.get(key, fn)` went through `asMap().compute()`, which invokes the mapping
function *on a hit*. Every caller is a pure load-if-absent, so a cached page was re-read,
re-decompressed and re-deserialized on every lookup, then written back over the entry. This is the
single largest read-path find in the branch.

---

## 2. Measured

JMH, `AverageTime`, µs/op, 5 warmup + 12 measurement iterations, one fork. **Trust these over the
W1–W6 numbers** — see the caveat in §4.

| Probe | Baseline (`aab434b`) | Now |
|---|---|---|
| Open read txn + point read + close | 10.314 ± 0.302 | **2.757 ± 0.133** |
| Point read on a held cursor | 0.057 | 0.051 (control — unchanged, as expected) |
| Serialize full doc, own transaction | 49.674 ± 2.861 | **37.362 ± 1.139** |
| Serialize full doc, borrowed cursor | n/a (new path) | **17.583 ± 0.474** |

The held-cursor control staying flat is what makes the 3.7× on transaction-open credible: nothing
touched traversal, so that gap is setup work that genuinely stopped happening.

**No commit-side claim is available on this hardware.** `durableCommit` measured 7,506 ± 7,848 µs
before and 5,789 ± 1,936 µs after — the baseline's error bar exceeds its own mean, because
virtualized storage wanders by more than the effect. The 3→2 round-trip reduction is a structural
property of the protocol (inspectable, and covered by the crash tests), not something this box can
time. **Re-measure on real NVMe before quoting a number.**

---

## 3. Open items

**Highest value first.**

1. **Serializer, continued.** Serialization is ~87 % of a full-document read; `569cadf` took 19 % off
   and it is still dominant. Four hypotheses are already *eliminated* by committed instrumentation —
   don't re-test them: serializer construction (1.6 µs), value materialization (~17 µs for all 131
   nodes), escaping, and sink choice (the byte sink's raw-UTF8 fast path buys only ~10 %). Start by
   profiling `JsonSerializer.emitNode` and the rest of `AbstractSerializer.serializeRevision`.
2. **io_uring tail log is compile-verified only.** `FFIIOUring.isAvailable()` is false in this
   container, so all six `IOUringIntegrationTest` cases print "Skipping" and pass vacuously. Needs a
   run on a real io_uring host before it is trusted.
3. **Phase-1 publish on the commit thread** — the one item from the original plan never started. It
   moves page publication across a thread boundary inside the two-phase commit, interacting with
   `AfterCommitState`, the pipelined-async epoch that reads a pending revision's pages before phase 2
   hardens them, and the TIL teardown after. Deliberately left for a session with room to trace those
   paths: the failure mode is pages published from the wrong thread or freed under a reader, which
   does not fail a test run — it corrupts data later.
4. **W5 regression.** Storage went 9.9 → 16.56 MiB (lean). This is preallocation and it is real
   bytes: `du` reports allocated ≈ apparent, because the zero-fill+fsync that keeps in-place writes
   journal-free is exactly what makes those blocks allocated. At this workload's size an 8 MiB tail
   dominates, so the gap to PostgreSQL widened from ~2.1× to ~3.9×. It amortizes for larger
   resources, but consider whether the adaptive chunk should scale down for small ones.

---

## 4. Reading the PostgreSQL comparison correctly

`docs/COMPARISON_POSTGRES.md` §0.4 has the current numbers. Two traps:

- **W2 splits on framing, and the answer flips.** PostgreSQL server-side (plpgsql loop, no client
  boundary) is 27.6 µs/read; client-driven (one statement per read over a unix socket) is ~275 µs.
  SirixDB is embedded at 101 µs — so **~2.7× faster than what an application actually experiences,
  3.7× slower than PostgreSQL's engine work in isolation**. Both are in the doc. Quoting only the
  server-side figure (as an earlier draft did) understates SirixDB for anyone running PostgreSQL
  over a network. W1/W3/W5 have no such ambiguity; PostgreSQL wins those under either framing.
- **The W1–W6 harness is noisy on this class of box** — W2 swung 151→288 µs for identical code.
  Use it for ratios and orders of magnitude, not for detecting a 20 % change. Use JMH for that.

Reproduction commands are in §0.3. Both harness halves are now in-tree
(`PostgresComparisonBench` + `docs/bench/`) because the original driver lived in `/tmp` and did not
survive its machine.

---

## 5. Environment gotchas

- **The container rolled the local git repo back twice mid-session**, to pre-merge history, silently.
  The second time it nearly produced a commit that reverted work already pushed (caught by a rejected
  push). If local history looks wrong: `git fetch` and compare against the remote before committing,
  and stage files explicitly rather than `git add -A`. Scratch files under `/workspace` survive the
  rollback; the session scratchpad does not.
- **PostgreSQL** is installed but not running by default. Start:
  `su postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/16/main -o '-c config_file=/etc/postgresql/16/main/postgresql.conf' -l /tmp/pg.log start"`.
  `shared_buffers` was raised to 1 GB to match the doc's methodology. Measured fdatasync floor on
  this box: 1,675 ops/s (597 µs/op).
- **`sirix-core` 1.0.0-beta7 was hand-installed into this container's `~/.m2`** so `sirix-enterprise`
  builds against current core rather than the released artifact. It shadows the real beta7 locally.
  Composite build (`--include-build`) does not work: it fails on an unrelated plugin incompatibility
  in `sirix-rest-api`.
- **Publishing core to mavenLocal via gradle fails** (signing + javadoc errors). What works:
  `./gradlew :sirix-core:generatePomFileForMavenPublication :sirix-core:jar`, then copy the jar and
  `build/publications/maven/pom-default.xml` into `~/.m2/repository/io/sirix/sirix-core/1.0.0-beta7/`.
- **Javadoc does not build** on `sirix-core` (pre-existing errors, unrelated to this branch).
