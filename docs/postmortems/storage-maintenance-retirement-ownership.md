# Bug: notification retirement confused with lifecycle ownership

- Fix / issue: lifecycle audit V05/V06; review-truncation-storage-fence, review-retirement-stops-scheduled-commits and review-retirement-owner-lock-order.
- Symptom: raw page truncation could race durability or append workers; ordinary lifecycle operations could time out through an inverted retirement lock; a temporary admission refusal could permanently stop timed commits.
- Trigger: pause a hardener or append worker before raw truncation; overlap execution/history ownership with another retirement; overlap a scheduled pre-hook with another handle's retirement.
- Root cause: callback quiescence was treated as exclusive lifecycle ownership. Storage completion belongs to the node execution/commit owner and page append permit, while notification quiescence only pauses admission and drains existing callbacks.
- Why tests missed it: node-wrapper tests covered hardening but bypassed raw page entry and flush work; callback metadata tests did not exercise ordinary execution/history owners; timed tests covered rollback after user failures rather than temporary internal admission refusal.
- Regression test: `rawPageTruncationFencesBoundHardening`, `truncationFencesAsyncAppend`, `timedCommitResumesAfterTemporaryAdmissionRefusal`, `executionOwnerCanQuiesceWhileCloseWaitsForExecution`, `historyOwnerCanTruncateWhileDerivedCleanupWaits` and retained-owner retry. [Evidence](../../data/sirix-1-0-lifecycle-fences/storage-maintenance-retirement-verification.json): 1,461 selected cases and 69 auxiliary cases pass apart from configured skips; 42 starting-HEAD cases fail. The strengthened real-handle timer cases pass twice and fail twice on starting HEAD. Scoped PIT remains with the outer executor.
- Lesson: [Separate quiescence from exclusive ownership](../LESSONS.md#separate-quiescence-from-exclusive-ownership).

## Behaviour and invariants

Every destructive page truncation must acquire its bound node's execution ownership, fence pending hardening, revalidate the captured page writer, and fence submitted append work before changing beacons, files or snapshots. A predecessor closed by hardening is rejected without mutation; a failed worker preserves its failure and history. Raw unbound page writers fence their own append work. Node wrappers retain their revision validation and index-abort contract.

Notification retirement is a shared admission pause, not a mutex between lifecycle owners. All overlapping scopes drain existing callbacks within the configured bound; admission resumes only after the last successful scope leaves. Timeout/interruption abandons queued notifications and retains active views for retry. Callback lifecycle rejection precedes every lock/wait. Existing mutation gates precede retirement, and callback waits remain outside handle/database/session admission monitors. Execution ownership protects node operations, history ownership protects derived maintenance, and teardown may join a derived worker without owning a lock the worker needs to request quiescence.

Temporary pre-hook admission refusal prevents that scheduled publication but leaves future ticks enabled. Explicit commits still report refusal; actual hook exceptions and Errors keep their original veto behaviour.

## Derived regression cases

- Raw successor/predecessor and node truncation during paused hardening: no early history change; success truncates only after hardening, closed predecessor rejects, terminal hardening failure preserves durable revision.
- Raw and node truncation during paused async append, including failure and interruption: no snapshot/file reuse while worker owns pages.
- JSON/XML timed commit during another retirement: observe refused admission, release retirement, then observe a later durable scheduled revision.
- Overlapping retirement while an ordinary caller owns execution/history: the owner can request quiescence and finish, allowing close/cleanup to continue without a retirement timeout.
- Preserve callback metadata reads, bounded callback failure, interrupted retirement, retained-owner retry, mutation-gate ordering, derived history recovery, warm opens and successor overlap.

The first focused run also exposed direct local-database teardown closing common storage while
user aliases still owned hardening. Local close must drain its registered user sessions before
common storage, preserving the first failure and retaining storage on an unfinished user close.
The raw entry preflight must reject ordinary enclosing execution, while a validated node maintenance
scope may reach the page boundary. Once physical workers have terminated, another closer may finish
idempotent cleanup; the first closer's delayed return must neither release a later writer's permit
nor lose its original failure. The existing concurrent-close test retains those ownership assertions
and removes its dependency on exclusive notification serialization.

Local teardown closes resource admission after callback quiescence, snapshots registered users under
the admission monitor, then releases that monitor before derived-worker joins and user cleanup.
Existing metadata reads remain available. The closing admission state remains set after a failed
owner drain so no new generation can enter before retry; physical closed state is published only
after cleanup. This is the local database's missing admission/ownership phase boundary, analogous
to the existing public-handle close boundary, rather than another callback guard or deadline.
