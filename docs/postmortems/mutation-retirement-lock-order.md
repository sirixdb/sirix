# Bug: retirement inverted existing mutation-gate ownership

- Fix / issue: review-mutation-retirement-lock-order.
- Symptom: ordinary concurrent creation or guarded cleanup timed out on notification retirement even with no callbacks running.
- Trigger: caller A holds a resource/database mutation gate; B acquires retirement and waits for that gate; A calls creation/removal/cleanup and waits for B's retirement.
- Root cause: moving callback drain ahead of admission monitors also moved it ahead of a pre-existing outer mutation gate. JSON/XML collection/store callers already acquire their gate before entering the engine. The new opposite edge made the global retirement lock part of a cycle.
- Why tests missed it: the metadata-read matrix paused callbacks but never held an outer mutation gate. Callback rejection was checked at engine lifecycle methods, below collection/store gate acquisition.
- Regression test: DatabaseNotificationLifecycleTest.heldMutationGateCanEnterLifecycleBeforeQueuedCreator covers JSON/XML, held resource/database gates, create/remove and session/handle/local/database cleanup. Gate queue observation establishes the interleaving; lifecycle completion and committed empty-resource state or precise closed-handle rejection form the oracle. callbackRejectsBeforeWaitingOnAnOuterMutationGate covers pre/post/listener mutation entry for canonical and equivalent database paths while another thread holds the gate; permitted metadata reads must still succeed.
- Lesson: extend existing lock orders rather than defining a new order from only the innermost callers. Put callback mutation rejection at the first shared acquisition boundary. See [Lessons](../LESSONS.md).

The order is mutation gate, notification retirement, then admission monitors. The internal derived
bootstrap and ungated teardown paths acquire no later mutation gate. Guarded database removal and
store cleanup already follow gate-before-retirement order and need no new gate or lifecycle state.
The 24 metadata-read regressions, bounded timeout/interruption, retained owners, warm opens and R13
successor overlap remain required. No row-loop, mutation-cursor or storage-reader logic changes.

Fresh verification: 1,415 selected cases passed with zero failures/errors and five configured skips;
69 auxiliary cases had zero failures/errors and two skips. All 278 notification/lifecycle cases,
49 query gate/store cases and documented work budgets passed. Independently compiled starting-HEAD
classes reproduced retirement timeouts in all 22 held-gate cases; all 22 pass on the fix. The five
previously compared hot class files remain byte-identical. See
[exact suites, source hashes and limits](../../data/sirix-1-0-lifecycle-fences/mutation-retirement-order-verification.json).
