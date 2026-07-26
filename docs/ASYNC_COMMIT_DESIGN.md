# Asynchronous Durable Commits (`KEEP_OPEN_ASYNC_COMMIT`) — Design

Status: **phase 1 implemented** (pipelined hardening); phase 2 (fully
backgrounded serialization) is future work.

## Motivation — the missing middle ground

| Mode | Revisions | Writer blocks on | Crash loses |
|---|---|---|---|
| `KEEP_OPEN` (sync auto-commit) | one per threshold, durable+queryable | full commit protocol per epoch | ≤ 1 epoch |
| `KEEP_OPEN_ASYNC_FLUSH` | none until final `commit()` | ~nothing (leaf I/O backgrounded) | everything since last real commit |
| **`KEEP_OPEN_ASYNC_COMMIT`** | **one per threshold, durable+queryable** | **serialization only — never the flush barriers** | ≤ 1 in-flight epoch + working set |

Micro-benchmark context (100k inserts, threshold 8k, this repo's bench
environment): sync auto-commit 556 ms, async flush 141 ms, single commit
106 ms. The sync-commit overhead is dominated by per-epoch durability
barriers (index-catalogue fsync, buffered-tail flush, data force, two DSYNC
beacon writes) — exactly the part this mode moves off the writer thread.

## Design: split the commit at the durability barrier

`NodeStorageEngineWriter.commit()` decomposes into two phases:

**Phase 1 — write pages (synchronous, writer thread).**
Create the commit marker file, set user/message/timestamp,
`parallelSerializationOfKeyValuePages()`, `uberPage.commit(this)` (walks the
TIL-backed reference tree, writes every modified page through the buffered
data channel and assigns every reference its disk key). After phase 1 the
revision's page trie is fully addressed in memory and fully written into OS
buffers — nothing references TIL-only state anymore. This phase mutates TIL
pages during serialization and therefore MUST run on the writer thread (the
async-flush path solves the same race with deep copies; doing that for the
whole trie is the phase-2 roadmap item below).

**Phase 2 — harden (background thread).**
`serializeIndexDefinitions(revision)` (must be durable before the beacon —
existing crash invariant), `writeUberPageReference(...)` (flushes the
buffered tail, forces the data file, writes both uber beacons write-through;
its return is the commit acknowledge), clear the TIL and local caches,
delete the commit marker, **then** publish
`session.setLastCommittedUberPage(...)` and close the superseded page
transaction (releasing its writer channels).

The public synchronous `commit()` is unchanged: phase 1 + phase 2 inline.

`asyncCommit()` = acquire the single commit permit (depth-1 pipeline), run
phase 1 inline, submit phase 2 to a background thread, return. The node
transaction immediately re-instantiates onto a **new page transaction based
on the pending uber page** and keeps inserting while phase 2's barriers run.

## Why this is safe

- **No concurrent file appends.** Only phase 1 writes data pages, and a new
  epoch's phase 1 cannot start before acquiring the permit that the previous
  epoch's phase 2 releases. Inserts (pure in-memory) are the only thing that
  overlaps hardening.
- **Reads of the pending revision are ordinary reads.** Phase 1 assigned
  disk keys everywhere, so the successor page transaction reads revision N
  through the normal buffer-manager/disk path (OS page cache serves
  not-yet-forced bytes). No frozen-TIL indirection, no forwarding links —
  the stale-reference machinery of the async *flush* path is not involved.
- **Durable-before-visible.** Readers resolve "latest" through
  `lastCommittedUberPage`, which phase 2 publishes only after the beacon
  write returns. A revision can never be opened and then lost to a crash.
- **Crash semantics unchanged.** A crash during/after phase 1 but before the
  beacon leaves the commit marker file present and no beacon for revision N:
  the existing truncate-to-last-committed recovery discards the partial
  epoch, exactly as for a crashed synchronous commit.
- **Commit-marker gating.** While an async commit is in flight the marker
  file legitimately exists; the successor page transaction's
  truncate-on-open check is skipped for pipelined creation (the check exists
  for *crash* recovery; in-process the in-flight state is known).
- **Failure poisoning.** A phase-2 failure latches the transaction
  terminally (same pattern as the async-flush path): the next operation
  throws, close/rollback drain the pipeline first. The already-reinstantiated
  epoch N+1 is based on a revision that never became durable, so poisoning —
  not retrying — is the only sound response.
- **Lineage.** Epoch N+1's page transaction is created from the pending
  (phase-1-complete, canonical in memory) uber page of revision N, not from
  `lastCommittedUberPage`. The depth-1 pipeline makes the pending chain at
  most one deep.

## Constraints

- `FILE_CHANNEL` storage backend only. (The plain async pre-flush,
  `KEEP_OPEN_ASYNC_FLUSH`, additionally supports `MEMORY_MAPPED` — both
  backends append through `FileChannelWriter`, and a write transaction's
  internal reads use `FileChannelReader` on both; async *durable commits*
  stay `FILE_CHANNEL`-only until the mid-transaction revision publication
  is validated against concurrently remapping memory-mapped readers.)
- Count-based auto-commit only (no timed trigger).
- One in-flight hardening per transaction (permit); an explicit `commit()`
  or `close()` first drains the pipeline.

## What phase 2 (future) adds

Backgrounding phase 1 as well — deep-copying the frozen generation's pages
(leaf *and* structural) so serialization runs off-thread, the way
`asyncFlush` already does for leaves. That removes the last synchronous cost
(CPU serialization) from the writer thread at the price of copying the trie
spine, and requires the successor transaction to read revision N through the
frozen generation until keys are assigned (the TIL generation/forwarding
machinery is the intended vehicle). The interface (`asyncCommit()`) is
unchanged by that evolution, which is why the mode ships now with pipelined
hardening only.

## Interaction with ledger mode (`TAMPER_EVIDENCE_PLAN.md`)

Each async commit is a full revision with a commit record, so the chain
gains one link per epoch, in pipeline order (depth-1 ⇒ order preserved).
Chain-hash computation and (later) signing belong to phase 2 — off the
writer thread by construction.

## Store/import guidance

Bulk imports that want one semantically meaningful revision should keep
using `KEEP_OPEN_ASYNC_FLUSH` (see the shredding discussion in
`ARCHITECTURE.md`); `KEEP_OPEN_ASYNC_COMMIT` is for ingest that needs
periodic *queryable* checkpoints without paying synchronous barriers.
