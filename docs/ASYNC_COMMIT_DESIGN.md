# Asynchronous Durable Commits (`KEEP_OPEN_ASYNC_COMMIT`) — Design

Status: **phase 1 implemented** (pipelined hardening); phase 2 (fully
backgrounded serialization) is future work.

## Motivation — the missing middle ground

| Mode | Revisions | Writer blocks on | Crash loses |
|---|---|---|---|
| `KEEP_OPEN` (sync auto-commit) | one per eligible threshold, durable+queryable | full commit protocol per epoch | ≤ 1 epoch |
| `KEEP_OPEN_ASYNC_FLUSH` | none until final `commit()` | ~nothing (leaf I/O backgrounded) | everything since last real commit |
| **`KEEP_OPEN_ASYNC_COMMIT`** | **one per eligible threshold, durable+queryable** | **serialization; prior hardening when the depth-1 pipeline is full** | ≤ 1 in-flight epoch + working set |

XML publication scopes defer revision-producing thresholds; their contract is owned by
[`XmlNodeTrx.beginAtomicOperation()`](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlNodeTrx.java).
Query application semantics are documented in [Query Updates](../README.md#query-updates).

Micro-benchmark context (100k inserts, threshold 8k, this repo's bench
environment): sync auto-commit 556 ms, async flush 141 ms, single commit
106 ms. The sync-commit overhead is dominated by per-epoch durability
barriers in the [commit protocol](DISK_FORMAT.md#1-files) — exactly the part
this mode moves off the writer thread.

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
`serializeIndexDefinitions(revision)` (writes only when required by the
[catalogue persistence rules](ARCHITECTURE.md#index-catalogues); any write must be durable
before the beacon), `writeUberPageReference(...)` (flushes the
buffered tail, forces the data file, and publishes both uber beacons through
the writer's durability protocol;
its return is the commit acknowledge), clear the TIL and local caches,
delete the commit marker, **then** publish
`session.setLastCommittedUberPage(...)` and close the superseded page
transaction (releasing its writer channels).

The public synchronous `commit()` is unchanged: phase 1 + phase 2 inline.

`asyncCommit()` = acquire the single commit permit (depth-1 pipeline), run
phase 1 inline, submit phase 2 to a background thread, return. The node
transaction immediately re-instantiates onto a **new page transaction based
on the pending uber page** and keeps inserting while phase 2's barriers run.
After successful hardening, `awaitPendingAsyncCommit()` reconciles the successor's
catalogue persistence baseline with the predecessor through
[`Indexes.acknowledgePersisted()`](../bundles/sirix-core/src/main/java/io/sirix/index/Indexes.java),
preserving changes the successor made while hardening was in flight.

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
  `lastCommittedUberPage`, which phase 2 publishes only after
  `writeUberPageReference(...)` returns with durable beacons. A revision can never
  be opened and then lost to a crash.
- **Crash semantics unchanged.** A crash during/after phase 1 but before the
  beacon leaves the commit marker file present and no beacon for revision N:
  the existing truncate-to-last-committed recovery discards the partial
  epoch, exactly as for a crashed synchronous commit.
- **Commit-marker gating.** While an async commit is in flight the marker
  file legitimately exists; the successor page transaction's
  recovery check is skipped only for pipelined creation with a known pending
  base. Ordinary writer admission follows [marker recovery](operations.md#marker-recovery),
  including after an in-process failure.
- **Failure poisoning.** A phase-2 failure latches the transaction
  terminally, including failures retiring the pending root or closing the
  predecessor. Repeated `awaitPendingAsyncCommit()` calls retain the original
  cause; commit and rollback drain the pipeline and reject the poisoned
  successor. A failure after beacon publication can leave revision N durable;
  recovery selects the verified beacon rather than assuming N was lost.
- **Lineage.** Epoch N+1's page transaction is created from the pending
  (phase-1-complete, canonical in memory) uber page of revision N, not from
  `lastCommittedUberPage`. The depth-1 pipeline makes the pending chain at
  most one deep.

## Failure and teardown ownership

Background hardening and both inline fallbacks (successor setup failure or
task-submission failure) release their acquired async permit exactly once.
Inline hardening preserves the setup/submission error and suppresses subsequent
hardening or cleanup errors. All routes close the detached predecessor and
retire its pending root even when hardening fails; a hardening failure remains
terminal after the permit is released.

Public transaction close retains its dirty-transaction protection. Resource-session
close can explicitly abort a dirty successor after fencing the failed predecessor;
see [`ResourceSession.close()`](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java).
The [database hook contract](hooks.md) owns callback timing, error handling and lock order.
Pre-hooks run synchronously before publication; post-hooks and listeners use a dedicated database
dispatcher after durable publication. All hardening paths, including inline setup/submission
fallbacks, schedule notifications at that publication boundary. Dispatcher read-only views resolve
durable revision roots; only writer readers with intent logs can use unpublished pending roots.
Neither successor mutation nor writer close waits for post delivery.

External close rejects enclosing writer execution and callback reentry before admission monitors.
Database/handle/session retirement and destructive history changes establish bounded notification
quiescence. On timeout queued work is discarded, while active callback readers and dependent
storage/file ownership remain reachable for retry. The final overlapping retirement scope resumes deferred
notifications from other open sessions; final database close stops the dispatcher. Callback errors
are logged independently and cannot veto published data or skip later notification attempts.

Node close acquires execution with a bound, preserves physical worker termination fences, then
removes its registry ownership and releases P exactly once after scheduler termination. Session
close snapshots outside node cleanup and retains every unfinished node owner's page writers and
ordinary reader dependencies. Independent owners still receive close; the first failure is
preserved and later failures are suppressed. A close-attempt count excludes new writers during
unlocked cleanup, while admitted reads remain available to active callbacks. Marker-driven recovery
at the next exclusive admission remains the sole aborted-offset reuse barrier.

`NodeTransactionAbortLifecycleTest` covers paired setup/submission and hardening
faults, retained causes, dirty successors and paused workers in XML and JSON.

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

Resource retirement rejects enclosing execution on any same-resource writer before notification quiescence. Retirement pauses callback admission without exclusively locking other lifecycle owners.
The former resource-shared unbounded completion fences and outer-unlock notification queues are
superseded by [bounded database dispatch](hooks.md); the earlier postmortems record historical designs.

Raw page truncation acquires its bound node execution owner and fences hardening before revalidating
the captured writer; all page truncations fence submitted async append work before changing history.
Closed predecessors and interrupted maintenance reject before destructive storage work. Temporary
pre-hook admission refusal leaves scheduled commits enabled; user-hook failures retain veto semantics.

The dispatcher retains at most its configured capacity across active, executor-queued and deferred
jobs. Each job carries its resource and published revision. Destructive page admission refuses
before mutation when deferred work would lose that original history; overlapping non-destructive
retirement stays shared. Overflow drops new work without waiting for delivery or vetoing durability.
Delivery is best effort; hooks/listeners expose their revision, but resume/replay remains a follow-up.
See [the hook contract](hooks.md) for capacity, ordering and abandonment semantics.
