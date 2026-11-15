# Bug: unsupported format fields could be silently accepted or misparsed

- Fix / issue: page-envelope and strict-reader pre-freeze change (1.0 format readers).
- Symptom: nonzero unknown reference hash forms were interpreted as eight-byte hashes; out-of-range revision-root references and superblock spare bytes were accepted; metadata tails were ignored. Missing optional settings or unknown settings fields caused unrelated JSON token errors.
- Trigger: a checksum-valid superblock with byte 13, 14 or 15 set; a reference hash-form byte in 2..255; revision-root bitmap slot 13; a metadata page with one extra byte; resource settings with an omitted intermediate option or an unknown field.
- Root cause: presence tests conflated an extensible discriminant with a boolean; bitmap capacity was mistaken for a limit; checksum validation was treated as sufficient format validation; parsers lacked an end-of-image invariant. Optional JSON names were read positionally without consuming unknown values or naming their rejection.
- Why tests missed it: writer/reader round trips only exercise currently emitted values and well-ordered settings. A matching writer cannot emit an unknown future discriminant or reserved-byte violation.
- Regression test: `StrictReferenceFormatTest`, `SuperblockSpareBytesTest`, `PageEnvelopeContractTest`, `ResourceSettingsExtensionsTest`. The unchanged engine at `7cd676bcd` accepts the invalid hash forms, reference slots and spare bytes, so those exception assertions fail before the fix. Independently encoded extension vectors and deterministic random truncations exercise the envelope boundary; metadata tests append bytes to valid images.
- Lesson: validate the semantic domain of every persisted discriminant independently of checksums, and test future-file inputs independently of the current writer. Optional settings require name dispatch and an explicit skip/refuse rule. See [Lessons](../LESSONS.md).

Run the boundary regressions with `./gradlew :sirix-core:test --tests io.sirix.page.PageEnvelopeContractTest --tests io.sirix.page.StrictReferenceFormatTest --tests io.sirix.io.SuperblockSpareBytesTest --tests io.sirix.access.ResourceSettingsExtensionsTest`.

PIT also exposed a test gap: checking only the word “flags” missed mutations that reported the wrong numeric flag or allowed mask. The mixed-flag regression now requires `flags 0x84 (allowed mask 0x3)` and verifies that refusal stops before the body.

## Allocation gate follow-up

The full-suite allocation check exposed a preexisting per-page array clone in
`BinaryEncodingVersion.fromByte`: `Enum.values()` allocates a new array on every call.
A focused JIT profile eliminated it, while the full suite retained 24 bytes per call.
An interpreted baseline/control probe confirmed the same allocation on the unchanged base.
The decoder now checks the sole supported version directly; the accepted byte domain is unchanged.
`PageEnvelopeAllocationTest` retains its original bound, and
`BinaryEncodingVersionStrictTest` checks all byte IDs and public-array isolation.
This was missed because focused performance tests relied on escape analysis to erase the clone.
