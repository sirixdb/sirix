# Bug: a compressed overflow payload could publish bytes from an earlier payload

- Fix / issue: this change on `fm/sirix-fp-s1-leaf-codec`; review finding R1 on
  [PR 1454](https://github.com/sirixdb/sirix/pull/1454). Specified by
  [FMT-216](../../spec/FMT-disk-format.md#fmt-216--projection-overflow-envelope-and-codecs).
- Symptom: the projection-typed overflow page image
  `09 00 03 40 00 00 00 03 00 00 00 02 fe 40 3f` deserialized **successfully** and returned a
  64-byte payload. None of those 64 bytes came from the record: the frame holds no literal byte at
  all. They came from `PageKind.OVERFLOW_DECODE_SCRATCH`, the reusable per-thread array that had
  just held another page's stored bytes. Expected: refusal with `IllegalStateException`. A reader
  could therefore serve one resource's bytes as another record's value, and a page whose stored
  length was truncated in transit decoded to plausible-looking data instead of failing.
  Independently, the compressed branch sized `new byte[decodedLength]` and a native landing of
  `decodedLength + 64` from an `i32` that nothing corroborated, so a fifteen-byte record could ask
  for a ~2 GiB reservation.
- Trigger: any committed or transported `OverflowPage` whose compressed payload frame is corrupt,
  reaching either overflow route — the pre-existing plain route or the projection route this branch
  added. The frame above is minimal: codec `2`, stored length `3`, declared decoded length `64`, and
  the single byte-run opcode `0x3f`, a literal run of 64 bytes.
- Root cause: the invariant a payload decoder owes its caller is that it reads **only** bytes inside
  the frame it was handed, consumes that frame exactly, and produces exactly the size the frame
  declares. This branch established that invariant in `ZeroRunByteCodec.decode` — literal spans
  bounded against `inEnd`, a frame-bounded varint, long runs of at least two, and a trailing-bytes
  check — and did not carry it to the two sibling decoders the same boundary dispatches to.
  `ByteRunCodec.decode` bounded every token against the **output** and nothing against the input, so
  a literal token longer than the frame read the scratch tail. `SirixLZ77Codec.decode` is worse by
  construction, and deliberately so: of its three paths only `decodeCoreSafe` checks input bounds,
  because the Java fast path treats the input array's tail slack as licence to overshoot, and the
  native path "opens with `(void)in_end`" under `Linker.Option.critical(true)`. Both are sound for a
  frame this encoder wrote and unsound for bytes just read from disk. The boundary in
  `PageKind.deserializeCompressedOverflowPayload` handed those disk bytes to the trusted entry point
  and only compared sizes *after* decoding, by which time the stale bytes were already in the result.
- Why tests missed it: the fixtures covered codec `0`, the one codec that had been hardened. The
  golden frame in `ProjectionOverflowEncodingTest` is the zero-run image; the reviewer's attack frame
  is that same image with the codec byte changed from `00` to `02`. Truncation was tested by
  shortening the whole page image, which shrinks `source.remaining()` and trips the envelope's
  stored-length check before any codec runs — it never produced a frame that is *internally*
  inconsistent while the envelope is intact. Nothing decoded a malformed frame after a valid one on
  the same thread, which is what makes the leaked bytes recognisable rather than zeros. The
  specification had already recorded all of this as an open implementation gap in FMT-216's
  Gap/note, so the suite agreed with the code instead of with the requirement.
- Regression test: `ProjectionOverflowEncodingTest.projectionOverflowRefusesFramesThatReadBeyondTheirStoredLength`
  feeds the exact frame plus the other defects FMT-216 enumerates through the public page reader, and
  decodes a valid 64-byte payload before each one so the scratch holds recognisable bytes;
  `projectionOverflowRefusesOuterLengthDisagreementBeforeReserving` covers length disagreement and
  the impossible declared size; `specifiedCodecFixturesProduceTheirSpecifiedPayloads` pins the three
  accept-fixtures FMT-216 supplies, with the payloads that text states;
  `everyElectedCodecRoundTripsOnBothOverflowRoutes` proves writer output still round-trips on both
  routes; `ByteRunCodecTest.boundedByTheFrameNotTheBuffer` and `exactFrameConsumptionIsRequired`
  pin the invariant at the codec, which has three further call sites. Command:
  `./gradlew --max-workers=2 :sirix-core:test --tests 'io.sirix.page.ProjectionOverflowEncodingTest'
  --tests 'io.sirix.page.ByteRunCodecTest'`. Verified by reverting `ByteRunCodec.java` alone and
  re-running: 3 failed, the first reporting `frame 090003400000000300000002fe403f must be refused
  ==> Expected java.lang.IllegalStateException to be thrown, but nothing was thrown` — the frame
  deserialized successfully and returned a page. With the fix restored, 158 tests across the codec,
  page-format, golden-fixture and HOT leaf classes pass with 0 failures. Note that the same frame
  handed straight to `ByteRunCodec.decode` in a unit test raised `IndexOutOfBoundsException`, because
  a 64-byte unit buffer cannot absorb the overshoot; only the production path, with its 64 KiB
  scratch, turned the defect into a silent success. A codec-level test alone would have mislabelled
  this as a bounds error on an undersized buffer.
- Exact frame consumption applies to the run codecs only: their opcodes all consume input and
  produce output, so a leftover byte means a malformed frame, and `ByteRunCodec` now refuses one as
  `ZeroRunByteCodec` already did. LZ77 frames are different, and requiring it there rejected valid
  pages — `StorageEpochFsstInlineDecisionTest` failed with "frame consumed 1327 of 1328 bytes" on
  committed data. `emitLiteralOnlyTokenArr` always appends a trailing literal-only token, and when
  the final match already completes the output, the decoder breaks at `outPos == uncompressed`
  without reading that token's byte. One unread byte inside the frame is therefore well-formed
  output. Containment does not depend on the cursor: the bounds-checked core bounds every read above
  by the frame, so an unread byte cannot reach the payload, and bytes past the stored length are the
  envelope's business, which already rejects them.
- Trade-off recorded: codec `3` now decodes overflow payloads through `decodeChecked`, the
  bounds-checked core, instead of the native decoder. That was preferred over validating the token
  stream in a separate pass so the LZ77 grammar keeps exactly one implementation — a validator that
  drifts from its decoder either rejects valid pages or reopens this hole — and it keeps the read a
  single allocation-free pass. It costs the native decoder's advantage on this one boundary, which is
  per large-blob read rather than per record. `decode` is unchanged for every other caller, and the
  input tail slack the native path needs is still provided. Revisit with a measurement if overflow
  payload decode shows up in a profile.
- Lesson: when a boundary dispatches untrusted bytes to several decoders, the bound belongs to each
  decoder, and a decoder whose hot path elides bounds checks needs a separate entry point for
  untrusted input rather than a caller that hopes. Recorded as a fragment in
  [`docs/lessons.d/hot-leaf-codec.md`](../lessons.d/hot-leaf-codec.md); [`../LESSONS.md`](../LESSONS.md)
  is the permanent destination (see the [fragment convention](../../changelog.d/README.md)).
