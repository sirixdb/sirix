# Bug: deferred delivery lost publication history and retained unbounded work

- Fix / issue: review-deferred-notification-history and review-bounded-notification-admission.
- Symptom: a deferred revision-2 callback could read replacement revision 2 after truncation/reissue, or fail before invocation; stalled delivery or retirement could retain unlimited jobs.
- Trigger: hold handle A's close in another resource's writer cleanup; handle B commits, truncates and reissues a deferred revision. Alternatively, keep delivery paused while committing beyond queue capacity.
- Root cause: quiescence counted executor work but intentionally ignored deferred work, and deferred jobs had no explicit resource/revision identity for destructive admission. Neither backlog shared a retention budget.
- Why tests missed it: pinned active views and overlapping non-destructive retirement were tested separately; neither deferred publication followed by history reuse nor combined backlog saturation was exercised.
- Regression test: `deferredPublicationPreventsHistoryReuse` (24 cases), `saturatedNotificationsDoNotBlockDurableCommits` (8 cases), and three queue/admission unit cases. The 32 engine cases fail on independently compiled starting-HEAD classes. The final aggregate has 1,496 selected cases and 69 auxiliary cases passing apart from configured skips; the notification suite was retried after correcting an exactly-once test assumption. [Evidence](../../data/sirix-1-0-lifecycle-fences/deferred-notification-verification.json). Scoped PIT remains with the outer executor.
- Lesson: deferred publications remain history owners until delivery or explicit abandonment; queue capacity must cover every retained backlog.

## Behaviour and derived checks

An admitted notification carries its resource and committed revision. Destructive page maintenance drains executable work within the existing bound, but refuses before mutation if deferred work would lose its original revision. Refusal never waits for another retirement owner and does not discard the conflicting publication. Retained target revisions and unrelated resources do not prevent maintenance. Node, raw bound, raw unbound and derived-history entry points share this check. Physical-worker fences, immediate callback rejection and ordinary overlapping retirement remain unchanged.

One positive per-database capacity bounds active, executor-queued and retirement-deferred notifications together. Admission never waits for consumer progress or runs callbacks on the committing thread. Overflow drops the new job without vetoing the durable commit; admitted work remains ordered. Dropped/abandoned jobs have opened no views, and all retained references/accounting are released when dropped or delivered. Hooks expose the committed revision through their read view, and listeners receive that revision number. No resume/replay API, durable replay or unconditional delivery guarantee is provided.

The regression oracle is the committed input value and revision: original revision 2 contains 1, and a later reissued revision 2 contains 2. Maintenance must refuse while the original notification is deferred; after original delivery, retry and reissue may succeed. Saturation must leave commits complete and the latest stored value readable while the consumer is still latched, retain no more than the configured capacity across both queues, and deliver only the admitted prefix in order.

Revision listeners retain their existing repeated-publication behaviour during writer cleanup.
Tests compare every admitted deferred job with its original input value and require ordered
admitted delivery; they do not invent exactly-once revision publication.
