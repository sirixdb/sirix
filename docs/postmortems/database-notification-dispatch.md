# Bug: database handle close waited on its own callback

- Fix / issue: review-database-handle-callback-cycle; [hook contract](../hooks.md).
- Symptom: external handle A close waited for B's callback, while B's callback waited for A's closing flag; neither could finish.
- Trigger: two handles share a resource, a callback pauses, A closes externally, and the callback calls A.close(). Pre-hooks, post-hooks and revision listeners could reach the wrapper before the existing resource guard.
- Root cause: synchronous post-delivery completion was coupled to writer/session teardown, and reentrancy checks lived below database-handle admission. A resource fence could not define a safe database-wide callback contract. Adding wrapper guards alone would retain unbounded listener waits.
- Why tests missed it: earlier checks entered ResourceSession.close directly, bypassed DatabaseHandle's closing wait, and treated synchronous callback completion as required. They also opened revision views after pending-root cleanup, missing unpublished-root aliasing during eager dispatch.
- Regression test: DatabaseNotificationLifecycleTest exercises JSON/XML pre/post/listener reentry across handle, local database, session, writer, page writer, facade, manager and shutdown boundaries; the exact external-close cycle; bounded blocked delivery with independent reader retention, queue abandonment and retry; durability/successor overlap, ordered immutable views, inline fallback, callback Error, nested independent databases, and durable-root reads while predecessor cleanup is paused. SharedCallbackLifetimeTest retains pre-hook lifetime and distant-page truncation coverage. These new asynchronous behavior assertions reject the old delivery/fence contract; baseline execution is not claimed.
- Lesson: enforce callback ownership at the database boundary, separate synchronous veto from asynchronous notification, and never retire active reader dependencies merely to meet a close deadline. See [Lessons](../LESSONS.md).

The dispatcher owns views until callback exit. Close has a configurable deadline; expiry drops unopened queued jobs, logs the loss and reports timeout while retaining live resources for retry. No callback cancellation is treated as termination. Ordinary row mutation and count-only paths are unchanged; lock/unlock now inherits ReentrantLock without delivery queues. Focused verification and executable-shape evidence are recorded in the notification-dispatch verification artifact; historical benchmark/PIT counts do not validate this change.

Adversarial verification also found stale rollback base selection after raw page truncation: the
node cursor could still name a removed revision. Public rollback and session abort now bound their
replacement root to the surviving durable revision, preserving ordinary revert/bootstrap behavior.
A node that cannot finish retains its bound page writers; only independent writers receive separate
close attempts. Concurrent session retirement may time out and retry while another closer still
owns retirement; independent node close still supports the existing cooperative permit-release path.

Fresh evidence: 1,207 selected cases, zero failures/errors and five configured skips; 69 auxiliary
cases, zero failures/errors and two skips. The six hot caller instruction/exception-table comparisons
match the independently compiled starting HEAD. See
[verification](../../data/sirix-1-0-lifecycle-fences/notification-dispatch-verification.json) and
[compiled comparison](../../data/sirix-1-0-lifecycle-fences/notification-dispatch-shape.json).
