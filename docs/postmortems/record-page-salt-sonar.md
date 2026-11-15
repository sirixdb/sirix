# Bug: salt serialization rejected by the Sonar quality gate

- Fix / issue: PR #1354, CI run 43727185579, Sonar rules S2184 and S5164.
- Symptom: analysis completes, but both Sonar checks fail the new-code reliability gate.
- Trigger: analyze the disposable header length expression and the new thread-local salt source.
- Root cause: the disposable serializer subtracts two integer header offsets before widening to the sink's long byte count. The fixed offsets cannot overflow, but the expression violates Sonar's arithmetic rule. Sonar also treats the intentional thread-lifetime salt cache as request-scoped state requiring removal.
- Why tests missed it: runtime tests establish wire bytes and allocation behavior; neither a redundant widening conversion nor a static-analysis lifetime assumption changes those results.
- Regression checks: the real Sonar analysis is the failing-before consumer. `PageKindTest.disposableEncodingSaltsOutputWithoutMutatingTheSourceHeader` independently checks all bytes outside the eight-byte salt against ordinary serialization, and `RecordPageSaltAllocationTest` checks reuse without per-page allocation. Keep these executable contracts; do not add source-text assertions for annotations or casts.
- Lesson: make sink arithmetic explicitly long and document intentional bounded thread-lifetime caches at the analyzer boundary. See [Lessons](../LESSONS.md).

## Behaviour, invariants and error cases

Header-slice byte counts must use the sink's long arithmetic. Both ordinary and disposable writes in `PageKind.KEYVALUELEAFPAGE.serializeKeyValueLeafPage` must emit the same fixed header and bitmap, differing only in whether salt initialization updates the logical frame. Widen an operand in both sibling header-slice subtractions.

Every thread must reuse its private, bounded salt generator across both writer paths through `PageLayout.saltForWrite`. Its cipher and fixed buffers retain no page, resource, request or user data. Keep the cache for the worker lifetime, with a field-scoped S5164 suppression explaining that ownership; do not add per-page removal, shared synchronization or cleanup machinery. Thread termination releases the cache. Disabled salts and existing nonzero salts continue to bypass the generator. Seed rejection and cipher failures retain their existing behavior.

The acceptance checks are the existing wire-equivalence, arbitrary-salt eager/lazy/probe reader, copy/reload/reset, NIST-vector/refill and allocation tests. They reject truncated or shifted headers, source-frame mutation during disposable writes, changed keystreams and per-page allocation. No production behavior change is intended, so an artificial failing-before runtime test would not represent either analyzer finding.

## Caller and failure-path trace

- `serializePage` delegates to `serializeKeyValueLeafPage` with no disposable table. After the first 18 header bytes, its widened subtraction still writes 142 bytes through the bitmap. Salt initialization still updates the logical header only here.
- `serializeDisposablePage` owns a confined writer table and delegates to the same helper. Its widened subtraction still writes the six bytes at offsets 18–23, followed by the eight salt bytes and 128 bitmap bytes. `NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage` can refuse the encoded result for unresolved references or insufficient frame capacity without having changed the logical salt; successful publication still copies the completed encoding into the frame/cache.
- Both paths select salt through `PageLayout.saltForWrite`: disabled returns zero, nonzero existing salt is preserved, and a fresh salted page uses `RecordPageSaltSource.current().nextSalt()`. The suppression changes none of the cache lookup, initialization, refill, error or thread ownership behavior. Cached encoded-page returns remain untouched. No alias, parameter, branch or helper becomes unreachable.

## PIT disposition

The downloaded CI artifact contains 3,571 mutations: 395 killed, 348 surviving and 2,828 uncovered, with no timeout. Mapping its XML locations against the diff from `21cef670d56a6ef25bef835d5fef16bc8b0c8f6d` to `cf4a471c4f4c5f68156dc6b0b318ecc1369b1c0c` confirms all 17 changed-line mutants were killed, including all seven in `RecordPageSaltSource`. The 3,176 undetected mutants concern unchanged statements, not this PR's salt implementation. Do not weaken the reporter, change its scope or expand this fix into legacy coverage work. Evidence is in `build/ci-current/pr-mutation` and `build/ci-current/mutation-provenance.json`.

## Verification results

- All 28 selected tests passed without skips: `PageKindTest`, `PageLayoutTest`, `RecordPageSaltSourceTest`, `RecordPageSaltAllocationTest` and `AsyncSnapshotEncodedCacheTest`. Their normal Gradle task dependencies, including isolated startup and storage-epoch suites, also completed. Command: `./gradlew :sirix-core:test --tests io.sirix.page.PageKindTest --tests io.sirix.page.PageLayoutTest --tests io.sirix.page.RecordPageSaltSourceTest --tests io.sirix.page.RecordPageSaltAllocationTest --tests io.sirix.access.trx.page.AsyncSnapshotEncodedCacheTest`. Evidence: `build/ci-current/focused-tests.log` and `build/ci-current/focused-results.json`. The initial combined command failed only on comment wrapping; the subsequent `:sirix-core:spotlessJavaCheck` passed, as did `git diff --check`.
- Local changed-source PIT rebuilt the repaired sources and completed in 3m10s with the same 395 killed, 348 surviving and 2,828 uncovered mutants. All 17 changed-line mutants were killed again, with no timeouts. The Gradle PIT task succeeded; the unchanged `--blocking` reporter returned 1 for the unchanged-code coverage debt. Evidence: `build/ci-current/local-mutation/sirix-core/report/mutations.xml`, `build/ci-current/local-mutation/summary.md` and `build/ci-current/local-mutation-provenance.json`.
- Sonar's downloaded gate response confirms that reliability was the only failing condition; coverage was 86.1%, duplication 0%, and security/maintainability passed. Both reliability issues are addressed at their reported sites, including the sibling header slice. Remote analysis must be rerun by the outer CI executor; this phase did not upload another analysis or change the quality-gate policy.
