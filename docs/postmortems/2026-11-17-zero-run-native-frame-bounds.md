# Bug: native zero-run decoding crossed encoded-frame boundaries

- Fix / issue: S1 HOT leaf codec implementation; native ZeroRunByteCodec decoder.
- Symptom: a truncated frame could succeed by reading bytes in the reusable input
  buffer beyond the declared encoded length; trailing frame bytes were accepted.
- Trigger: frame `ff 06 01 07 08 82` with a shortened declared input length;
  regression `segmentDecoderRejectsTruncationWithoutReadingReusableScratchTail`.
- Root cause: the native-output overload bounded token reads but read varints and
  literal bytes against the backing array, not the encoded frame. It also omitted
  the final consumption check. A reused scratch array made missing input appear
  available. The heap-output overload already enforced the correct invariant.
- Why tests missed it: native tests round-tripped valid frames; adversarial frame
  boundaries were tested only on the heap-output overload.
- Regression test: `:sirix-core:test --tests io.sirix.page.ZeroRunByteCodecTest`.
  Before the fix the new regression failed with “Expected IllegalStateException
  to be thrown, but nothing was thrown.” The test uses a larger backing array to
  distinguish the logical frame boundary from the array boundary.
- Lesson: [Logical codec bounds](../LESSONS.md#logical-codec-bounds).
