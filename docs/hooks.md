# Database hook and notification contract

Pre-commit hooks run synchronously inside the writer transaction before publication. They may do ordinary independent read-only work. An exception or Error vetoes that commit with its original identity; the transaction remains available for retry or rollback. A hook may not commit, roll back, close, truncate or otherwise end its database, handles, sessions or writer transactions. Reentrant lifecycle calls reject immediately with IllegalStateException before any admission monitor, execution lock or closing wait.

Post-commit hooks and revision listeners run asynchronously on one dedicated notification dispatcher per database after durable publication. Submission never waits for delivery. Notifications are ordered per resource (the database dispatcher also serializes resources), with at-least-once delivery attempts while notification admission remains open. A post-hook receives a fresh dispatcher-owned read-only cursor pinned to exactly the committed revision and initially at the document root; it must not retain the cursor. Revision listeners receive the published revision number and run within an equivalent revision-pinned read scope. Independent read-only cursors may be opened and closed normally; callbacks may not borrow a mutable writer cursor. Callback exceptions are reported by the dispatcher and cannot veto durable data. Applications should make notification handling idempotent.

A dispatcher-thread callback may not close/commit/rollback/truncate the same database, any of its handles/sessions/writer transactions or database-wide manager/shutdown operations. Rejection occurs before locks and waits, including a second close while an external close is already underway. Different databases remain independent. Ordinary enclosing writer execution retains the existing pre-lock retirement rejection.

Database/handle/session close stops notification admission, drains admitted callbacks up to the configured bound, then discards queued notifications with a warning if the bound expires. The default bound is five seconds, configurable with `sirix.notifications.closeTimeoutMillis` or the database dispatcher's timeout configuration. No close path waits unboundedly on callback delivery. If a running pre-hook or listener still owns readers at the deadline, close reports a timeout and retains those readers, storage, registration and file ownership for retry. It does not close state beneath a running callback. The notification thread is a daemon, and dropped jobs have not opened views. A later close can finish after the callback returns. During successful handle/session retirement, publications from other open sessions are deferred until retirement releases ownership; final database close discards them. Notifications from a closing or closed source session are dropped. Concurrent pre-hook admission during retirement rejects before publication and may be retried. A successful final database close stops the dispatcher. Notification admission stays stopped after timeout until successful retirement/retry.

Writer close does not wait for post notifications: their independent read views belong to the session, and session/database retirement owns their drain. Acquiring a writer for close or destructive truncation is bounded as well, so an in-transaction hook cannot pin an external closer forever. Non-callback async storage workers retain their existing positive termination fence. Destructive truncation establishes bounded notification quiescence before invalidating history; timeout preserves the pinned history and exposes failure.

Notification retirement is a shared admission pause rather than an exclusive lifecycle lock.
Each overlapping scope drains callbacks within the configured bound. Admission resumes only when
the final scope leaves, so a lifecycle owner never waits for another lifecycle owner's retirement
scope. Mutation gates precede retirement. Callback waits remain outside handle/database/session
admission monitors; post callbacks take no writer execution or history lock. Node execution owns
storage maintenance, then hardening and append permits fence physical workers before destructive
page work. History locks protect derived history independently; teardown can join a derived worker
without holding an exclusive retirement lock it needs. Callback lifecycle calls reject before any
lock or wait. Temporary admission refusal skips a scheduled commit tick while preserving subsequent
ticks; user hook exceptions and Errors retain their synchronous veto behaviour. Normal per-row
mutation and count-only paths gain no callback bookkeeping. Transaction lock/unlock inherits the
standard ReentrantLock implementation.

Specification/test mapping: database-wide dispatcher tests cover pre/post/revision callbacks against handle/local database/session/writer/page/manager/shutdown lifecycle entry points in JSON/XML, including the external-handle-close cycle; immutable revision/order checks and a held durability barrier prove successor overlap and delivery after durability; a blocked listener plus queued followers proves bounded timeout, queue abandonment, live reader retention and retry. Pre-hook tests preserve veto identity, synchronous timing and same-database rejection. Existing independent-owner cleanup and worker-fence regressions remain applicable. Historical callback-fence evidence describes the superseded synchronous delivery contract.

Resource lifecycle lock order also applies to bootstrap and retained-resource removal: acquire
the mutation gate first, then bounded notification quiescence, before the handle admission or
local database monitor. Nested bootstrap/session cleanup reuses that established retirement scope.
Eager derived initialization runs after session registration has released those monitors; derived
creation/rebuild establishes the same outer retirement boundary. Callback metadata reads and
ordinary existing-session opens remain permitted while another thread waits for quiescence.
Timeout/interruption before quiescence must leave resource files and retained owners untouched,
preserve the failure/interrupt status, and permit retry after the active callback exits.

Session-open completion carries only the newly opened derived-source reference past admission;
warm opens retain their existing fast return and never enter notification retirement. Resource
removal rejects registered user sessions before waiting, then revalidates under the database monitor.

Mutation-gate entry itself rejects same-database callback mutation before retaining or locking a
gate, using the canonical database identity. This covers collection/store wrappers that acquire an
outer resource or database gate before reaching engine lifecycle methods. Ordinary metadata and
read operations do not enter mutation gates. Guarded store cleanup and database removal already
hold their mutation gate before handle/database/session retirement; ungated teardown and internal
derived bootstrap acquire no later gate while holding retirement. A shared database hold must never
be upgraded to an exclusive hold.

Raw page truncation uses the same bound-node execution/hardening owner as node truncation,
then revalidates its captured page writer and fences submitted async flush work. A predecessor
closed during hardening cannot truncate storage. An unbound page writer fences its own flush
worker. Worker failure leaves beacons, files and snapshot state untouched by truncation.

Direct local-database teardown closes registered user sessions before shared storage; an unfinished
user close retains the shared backend for retry. Ordinary enclosing writer execution still rejects
raw maintenance before waiting. Only the validated node-maintenance scope can enter its page boundary
while owning execution. After physical termination, a concurrent closer may complete idempotent
cleanup; delayed earlier closers cannot release a subsequent writer's permit.

Local database resource admission closes only after callbacks have quiesced. User-session drains,
derived-worker joins and shared-storage cleanup run outside its admission monitor. Metadata reads
remain permitted until final closure; failure retains the admission boundary and unfinished owners
for a later close retry.
