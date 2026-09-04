/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.page.SirixLZ77Codec;
import io.sirix.page.SirixLZ77NativeDecoder;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The C decoder is ACTUALLY ENGAGED when a compressed dictionary value block is read back.
 *
 * <p>
 * {@link SirixLZ77Codec#decode} chooses between a C decoder and a Java one by inspecting its
 * arguments, and when the preconditions fail it silently runs the Java decoder — measured at <b>3.0
 * GB/s against 16.9</b>. Nothing fails, nothing warns, and the answer is identical, so a
 * correctness test cannot see the difference: only a counter can.
 * </p>
 *
 * <p>
 * <b>This test exists because that fallback did fire.</b> The landing area was first sized with
 * {@code NATIVE_INPUT_TAIL_SLACK} (16) where the dispatch tests the output against
 * {@code NATIVE_OUTPUT_TAIL_SLACK} (64) — 48 bytes short, so the detour was inert while looking
 * correct in review and in every round-trip assertion. The two constants are public, adjacent and
 * differ by 4x; picking the wrong one is a mistake worth a permanent guard rather than a comment.
 * </p>
 *
 * <p>
 * It is the general form of the rule the campaign keeps rediscovering: <b>a witness must assert
 * that the mechanism under test is engaged</b>, because a correct-but-slower fallback is exactly
 * what a fixture will take without telling you.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class CompressedBlockDecodeTakesTheNativePathTest {

  /** Comfortably over the codec's own {@code NATIVE_DECODE_MIN_BYTES} and a realistic block size. */
  private static final int BLOCK_BYTES = 32 * 1024;

  @Test
  void aLandingSizedWithTheOutputSlackReachesTheCDecoder() {
    Assumptions.assumeTrue(SirixLZ77NativeDecoder.isAvailable(),
        "no native decoder on this platform; the dispatch this test guards cannot be exercised");

    final byte[] raw = compressiblePayload();
    final byte[] frame = new byte[SirixLZ77Codec.maxEncodedSize(raw.length) + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
    final int frameLength = SirixLZ77Codec.encode(MemorySegment.ofArray(raw), 0L, raw.length, frame, 0);

    try (Arena arena = Arena.ofConfined()) {
      final long before = SirixLZ77Codec.getNativeCallCount();
      final long javaBefore = SirixLZ77Codec.getJavaCallCount();
      Assumptions.assumeTrue(before >= 0 && javaBefore >= 0);

      // Sized as the production landing is: decoded length PLUS THE OUTPUT SLACK.
      final MemorySegment landing = arena.allocate(raw.length + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK);
      final int produced = SirixLZ77Codec.decode(frame, 0, frameLength, landing, 0L);
      assertEquals(raw.length, produced, "the frame must decode to what was encoded");

      final long nativeCalls = SirixLZ77Codec.getNativeCallCount() - before;
      final long javaCalls = SirixLZ77Codec.getJavaCallCount() - javaBefore;
      Assumptions.assumeTrue(nativeCalls + javaCalls == 1,
          "-Dsirix.lz77Codec.diag.counters is off, so the dispatch is unobservable here");
      assertEquals(1L, nativeCalls,
          "the decode must reach the C decoder; taking the Java path costs 5.6x and is silent");
      assertEquals(0L, javaCalls);

      // THE MUTATION, made permanent: the same call with the INPUT slack — 48 bytes short — must
      // fall back, which is what makes the assertion above a guard rather than a coincidence.
      final long beforeShort = SirixLZ77Codec.getJavaCallCount();
      final MemorySegment tooShort = arena.allocate(raw.length + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK);
      assertEquals(raw.length, SirixLZ77Codec.decode(frame, 0, frameLength, tooShort, 0L),
          "the fallback must still be CORRECT — it is only slower");
      assertEquals(1L, SirixLZ77Codec.getJavaCallCount() - beforeShort,
          "a landing sized with the INPUT slack must decline to the Java decoder; if it does not, the two "
              + "constants have converged and this guard is no longer aimed at anything");
    }
  }

  @Test
  void theTwoSlackConstantsStillDiffer() {
    assertTrue(SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK > SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK,
        "the guard above assumes the output slack is the larger of the two; if that changes, every landing "
            + "sized against either constant needs re-checking");
  }

  /** Repetitive enough to compress, varied enough not to become one run. */
  private static byte[] compressiblePayload() {
    final StringBuilder text = new StringBuilder(BLOCK_BYTES + 64);
    int i = 0;
    while (text.length() < BLOCK_BYTES) {
      text.append("https://example.invalid/a/path/that/repeats/").append(i++).append('\n');
    }
    final byte[] utf8 = text.toString().getBytes(StandardCharsets.UTF_8);
    final byte[] exact = new byte[BLOCK_BYTES];
    System.arraycopy(utf8, 0, exact, 0, BLOCK_BYTES);
    return exact;
  }
}
