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
  pin the invariant at the codec, which has three further call sites.
  `projectionOverflowRefusesADeclaredSizeNoTokenStreamCouldProduce` drives the gibibyte no-token
  frame through all three codecs, `aLongRunStillDeclaresFarMoreThanItsStoredBytes` pins the valid
  counterpart a tightened bound would break, and
  `HOTLeafRefusalAllocationTest.refusingAnUnproducibleOverflowPayloadAllocatesIndependentlyOfItsDeclaredSize`
  is the allocation evidence: refusing a frame declaring a mebibyte costs no more than refusing one
  declaring sixty-four bytes, measured with `ThreadMXBean.getThreadAllocatedBytes` rather than a
  clock. `validLz77OverflowReadsTakeTheNativeDecoder` asserts the product's own dispatch counters to
  show a valid codec-3 overflow read on the native decoder with no Java fallback, and
  `theNativeDecoderItselfRefusesMalformedLz77Frames` uses the same counters to show that a
  length-extension byte past the frame, a literal span past the frame and a match before the first
  decoded byte are each refused BY the native decoder rather than by a slower one substituted for
  it — the evidence that the in-pass C bounds fire.
  `SirixLZ77NativeContractTest.checkedDecodeMatchesTheSameContractAtEverySlack` holds the guarded
  entry to the canary contract — identical bytes, never a store past the destination — across the
  slack matrix that drives both the native loop and the Java pass.
  Those counters were unobservable in the `test` lane until this change: the same
  `sirix.lz77Codec.diag.counters` property was configured twice inside one block of
  `bundles/sirix-core/build.gradle`, deliberately `true` with a comment that a skipping guard "is
  not guarding", then back to `false` seventy-seven lines later as part of a bulk debug-flag list.
  The later assignment won, so this guard and the two older ones
  (`CompressedBlockDecodeTakesTheNativePathTest.aLandingSizedWithTheOutputSlackReachesTheCDecoder`
  and `SirixLZ77NativeDecoderBenchTest.dispatcherRoutesToNative`) all skipped their dispatch
  assertion. Removing the duplicate makes all four assert and pass; an explicit `=false` on the
  command line still wins, as the surviving line documents. Command:
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
- Safety without the trade-off: the bounds live inside the fast decoder, not beside it.
  `sirix_lz77.c`'s decode body now takes a compile-time `checked` parameter and is force-inlined
  twice, so `sirix_lz77_decode` keeps exactly the instruction stream it had and
  `sirix_lz77_decode_guarded` is the same vectorised loop with the input bounds the C entry point
  could not check: the token byte, every length-extension byte, the literal span, the two distance
  bytes, and a cap on the running extension sums so a long `0xFF` chain cannot wrap a 32-bit length.
  The output bounds were already there — the hot loop's `outPos <= safeLimit` invariant, the
  per-token overflow tests and the `dist == 0 || dist > outPos` check. A violation returns a negative
  code that the Java binding turns into `IllegalStateException`. This is the shape LZ4 uses for
  `LZ4_decompress_safe` versus `LZ4_decompress_fast`: one `LZ4_decompress_generic` body with
  compile-time flags and thin wrappers, so the grammar still has exactly one implementation — a
  separate validator that drifts from its decoder either rejects valid pages or reopens the hole.
  A valid codec-3 overflow payload therefore decodes at native speed, which is what the reads-first
  criterion requires, and `SirixLZ77Codec.decodeCoreSafe` gives identical refusals in its own single
  pass wherever the bundled library is absent. The guarded symbol is resolved separately from the
  fast one, so a library built before it existed still serves trusted frames instead of silently
  losing a refusal.
- An agreeing length is not a producible length. The first fix compared the page's declared decoded
  length with the frame's own and allocated once they matched. `09 00 03 00 00 00 40 06 00 00 00 02
  fe 80 80 80 80 04` matches on both: a byte-run marker and a five-byte size varint spending
  `4 << 28` to declare a gibibyte, followed by no token byte at all. One gibibyte of heap plus a
  native landing were reserved before the decoder reached its first "input exhausted". The boundary
  now also requires the declared size to be within what the bytes after the frame header could
  produce: every token consumes at least one body byte, so the ceiling is the body-byte count times
  the most a single token can emit. For the run codecs that is a 128-byte short run or a long run
  whose count is a varint no longer than the bytes present, which is why the ceiling climbs
  exponentially and stops constraining at five body bytes — it has to, because a twelve-byte
  zero-run frame legitimately declares a gibibyte of zeros and no ratio cap may reject committed
  data. For LZ77 each extension byte adds at most 255 against a three-byte token overhead, so
  output never exceeds 255 per consumed byte. The ceiling is a necessary condition only; exact
  production stays with the single-pass decoders, so no second grammar implementation appears.
- Lesson: when a boundary dispatches untrusted bytes to several decoders, the bound belongs to each
  decoder, and a decoder whose hot path elides bounds checks needs a separate entry point for
  untrusted input rather than a caller that hopes. Recorded as a fragment in
  [`docs/lessons.d/hot-leaf-codec.md`](../lessons.d/hot-leaf-codec.md); [`../LESSONS.md`](../LESSONS.md)
  is the permanent destination (see the [fragment convention](../../changelog.d/README.md)).
