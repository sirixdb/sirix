# Bug: predictable record-page salts

- Fix / issue: review R1, record-page salt implementation for the 1.0 format freeze.
- Symptom: surviving page salts allowed reconstruction of an erased salt, leaving the retained parent checksum usable to test candidate erased contents.
- Trigger: serialize four fresh pages on one thread with the identity pipeline, erase the second, invert the surviving salts' mixer and recover the generator increment from the third and fourth outputs.
- Root cause: uniqueness and non-blocking generation were treated as sufficient for erasure preparation. ThreadLocalRandom has an invertible output function over an additive state; exposed neighboring outputs reveal the missing output. Both reusable and disposable writers shared this source.
- Invariant and scope: neighboring persisted salts must not reveal a removed salt. The shared saltForWrite boundary covers ordinary/disposable writes, monolithic/chunked encoding, new/reset pages and existing-salt reuse. Deterministic mode remains zero, disposable source headers remain intact, and every reader remains unchanged.
- Fix: replace the source at that boundary with a per-thread AES-256-CTR keystream seeded with non-blocking OS randomness. A reused 2 KiB buffer amortizes cipher calls across 256 salts. The cipher counter continues across refills. Missing crypto fails closed; no global security configuration changes or reader operations are introduced.
- Why tests missed it: uniqueness, byte differences and correct decoding do not establish unpredictability. No test attempted state recovery.
- Regression test: RecordPageSaltTest.neighboringSaltsCannotReconstructTheRemovedSalt executes the attack against both write helpers, with ThreadLocalRandom and SplittableRandom positive controls; the former implementation necessarily satisfies the attack relation. RecordPageSaltSourceTest checks NIST SP 800-38A F.5.5 and an independent AES-ECB/BigInteger counter oracle across eight batch refills. Existing deterministic, copy/reload, reset, disposable refusal and allocation checks retain their assertions and bounds.
- Error cases: unavailable non-blocking OS randomness or AES support prevents salted serialization; deterministic and already salted pages do not initialize a source. No insecure fallback is provided.
- Lesson: verify the randomness threat model, cryptographic primitive and batching behavior separately. See [Lessons](../LESSONS.md).

## Focused verification

The review fix ran `:sirix-core:test` restricted to `RecordPageSaltTest`,
`RecordPageSaltSourceTest` and `RecordPageSaltAllocationTest`: 13 tests passed,
none skipped. The existing 4,096-byte allocation bound over 100,000 fresh salts
was unchanged and passed. The same invocation ran PIT with `DEFAULTS` and
`INLINE_CONSTS`, targeting only `RecordPageSaltSource` and the three salt helpers
in `PageLayout`: 30 of 31 mutants killed, no uncovered mutants or timeouts.
Local evidence: `build/review-salt/verification.log` and
`build/review-salt/pit-report/mutations.xml`.

The sole survivor is `RecordPageSaltSource.nextSalt`, line 45,
`RemoveConditionalMutator_EQUAL_ELSE`, replacing the `written != output.length`
condition with false. This is equivalent for conforming AES-CTR/NoPadding
providers: the fixed, distinct 2,048-byte input/output arrays and full-block input
produce exactly 2,048 output bytes on every update. The guard fails closed for a
provider violating that contract; the mutant cannot alter valid generation.
The NIST vector and independent block-counter oracle exercise the actual output
across eight complete batches. No budget was widened.
