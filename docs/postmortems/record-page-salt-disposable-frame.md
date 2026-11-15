# Record-page salt changed a refused disposable frame

- Fix / issue: record-page salt implementation for the 1.0 format freeze.
- Symptom: a disposable identity encoding that was refused for insufficient frame capacity left a newly generated salt in the caller's logical header.
- Trigger: salted defaults with a full frame whose encoded page exceeds its available capacity. `AsyncSnapshotEncodedCacheTest.directIdentityCapacityFailurePreservesCacheAndFrame` failed at header byte 24 before the fix.
- Root cause: the shared record-page serializer initialized the source header before the outer snapshot writer decided whether to accept the encoding. Successful serialization is earlier than the publication boundary: capacity and unresolved-reference checks can still refuse it.
- Why tests missed it: the initial salt tests covered successful ordinary writes, copies and reloads. They did not exercise a caller refusing an already encoded disposable result. The existing exact-header regression caught the violation.
- Regression test: retain the existing salted capacity-refusal test; `RecordPageSaltTest.disposableEncodingSaltsOutputWithoutMutatingTheSourceHeader` additionally checks that output is salted, remains readable and leaves the source salt untouched. Run both through `:sirix-core:test --tests io.sirix.page.RecordPageSaltTest --tests io.sirix.access.trx.page.AsyncSnapshotEncodedCacheTest`.
- Lesson: speculative serializers must honor the caller's publication boundary. Emit disposable salt directly into the output; retain it in the header only for reusable ordinary pages. See [Lessons](../LESSONS.md).
