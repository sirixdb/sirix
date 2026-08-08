# Designing a meaningful SirixDB performance comparison

`COMPARISON_POSTGRES.md` is, as it stands, not a database comparison. Four independent defects were
found in it (§0.9, §0.10, §0.14, §0.15), each of which alone invalidates conclusions drawn from it:
a cold JVM, a RAM-resident corpus, an arbitrary commit granularity, and a hand-rolled timing loop.
This document specifies what to build instead. It is a design, not a result — nothing here has been
run.

Everything below is a consequence of a mistake actually made, not a general best-practices list.

---

## 1. Rules the current benchmark broke

**R1 — Measure in the regime you want to claim.** State, per number, whether it is cache-resident or
storage-bound, and make the corpus match. A corpus smaller than RAM cannot support any statement
about I/O; §0.12's tmpfs run measured no difference precisely because there was no I/O to remove.

**R2 — Size by bytes, not by rows or revisions.** SirixDB stores ~3.9× more per revision than the
PostgreSQL pattern. Matching revision counts silently hands PostgreSQL a 3.9× smaller working set,
so a cache-pressure comparison at "equal revisions" is measuring the wrong thing. Equal-bytes and
equal-revisions are different experiments; pick one and label it.

**R3 — Never let the harness choose the operating point.** W1 fixed one change per commit and
therefore reported commit granularity as if it were engine throughput: 8 changes per commit cost the
same 3.9 ms as 1 (§0.15). Any axis the workload fixes arbitrarily — batch size, document size, read
locality, revision distribution — must be SWEPT, and the result reported as a curve.

**R4 — Use JMH for warm steady state; use a bespoke single-run harness for cold.** They measure
opposite things and neither substitutes. A hand-rolled loop got warm-up wrong by 30× on W3 (§0.10);
JMH would get cold-start wrong by design, since eliminating first-run effects is its purpose.

**R5 — Report the unit the user experiences.** Both `changes/s` and `commits/s`; both server-side and
client-driven for PostgreSQL (§0.9: 23.5 µs vs ~266 µs for the same read — a 10× difference that
decides who "wins"). Quoting one framing without the other is a choice, and it should be a stated
one.

**R6 — A control per claim.** The one measurement in this whole effort that held up under scrutiny
was the held-cursor read staying flat at ~50 ns while transaction-open fell 10.3 → 2.8 µs. It held up
*because* the control made the alternative explanation (a faster box) untenable.

---

## 2. What to measure

### 2.1 Regimes — every workload is run in all three

| Regime | Corpus vs machine | How | Answers |
|---|---|---|---|
| **Cache-resident** | ≪ RAM | current 16 MiB corpus | pure CPU cost per operation |
| **Buffer-pressured** | > engine caches, < RAM | ~2 GB (≈618 k SirixDB commits, ~31 min ingest) | cache-miss paths, still no device I/O |
| **Storage-bound** | > RAM | > ~20 GB here, or cgroup memory limit, or drop caches per run | what the device actually costs |

A cgroup memory limit is far cheaper than a 20 GB corpus and gives the same effect — prefer it.

### 2.2 Axes to sweep, not fix

- **changes per commit**: 1, 10, 100, 1 000 — report changes/s (R3)
- **document size**: ~2 KB, ~100 KB, ~10 MB — the current 2.4 KB maximises fixed-overhead share
- **history depth**: 10², 10³, 10⁴, 10⁵ revisions — where reconstruction cost actually bites
- **read locality**: newest revision / random / oldest — §0.12 showed random costs ~3× fixed, and
  which one a deployment does is a real difference between deployments

### 2.3 Workloads

Keep W1–W6, with W1 swept per R3. Add what a bitemporal store is actually *for*, because a
comparison restricted to where PostgreSQL is optimised is not neutral — it is a choice of terrain:

- **time-travel scan**: aggregate a field across a revision RANGE (not all revisions)
- **as-of join**: reconstruct N documents as of one timestamp — the shape a report actually runs
- **audit diff**: what changed between two arbitrary revisions (SirixDB's W6 already wins this, and
  PostgreSQL has no native equivalent — say so as a capability difference, not a timing)
- **write amplification**: bytes written per logical change, which is where CoW versioning is
  structurally different and where neither system's number is currently known
- **cold-start latency**: first read after restart, per R4 — a system with a 4× slower warm read can
  still win here if it reads fewer bytes

### 2.4 Report per number

Regime · corpus bytes on each side · JVM and GC · warm-up evidence (per-iteration timings, not just
a median) · error bars · the control that rules out the obvious alternative explanation.

---

## 3. Sequencing

1. **Fix the harness before generating more numbers.** JMH port for warm (`VersionedDocWorkloadBenchmark`
   exists; W1 needs the batch sweep), one bespoke cold harness. Without this every number is suspect,
   which is how four defects survived this long.
2. **Cache-resident sweep** — cheap, and it is the regime where this session's serializer/cursor/cache
   work is genuinely visible.
3. **Buffer-pressured (~2 GB)** under a cgroup limit. *Partly done — see
   [`COMPARISON_POSTGRES_BULK.md`](COMPARISON_POSTGRES_BULK.md), which runs a 2.12 GB **bulk**
   corpus warm and cold. It uses `drop_caches` rather than a cgroup limit, because the container
   it ran in exposes no cgroup controllers. The versioned workload at this scale is still unrun.*
4. **Storage-bound**, same corpus, tighter limit.
5. Only then rewrite `COMPARISON_POSTGRES.md`'s conclusions.

## 4. What survives from the current document

The **optimisations**, not the comparison. Every gain in §0.5/§0.6 was measured by JMH against its
own immediately-preceding commit, on one box in one session, with non-overlapping intervals — those
do not depend on the PostgreSQL comparison being sound, and they are real: transaction-open
10.3 → 2.8 µs, owning-transaction full-document serialization 42.3 → 13.5 µs, and the `NamesCache`
fix worth -41 % on its own.

What does not survive is any statement of the form "SirixDB is N× faster/slower than PostgreSQL at
X". Those need §2's matrix first.
