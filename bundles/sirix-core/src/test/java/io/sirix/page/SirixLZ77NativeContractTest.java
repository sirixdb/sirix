package io.sirix.page;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the tail-slack contract between {@link SirixLZ77Codec} and the native decoder.
 *
 * <p>
 * {@code sirix_lz77.c} decodes with wide stores and no input bounds checks inside its hot loop, on
 * the documented promise that the caller leaves {@link SirixLZ77Codec#NATIVE_OUTPUT_TAIL_SLACK}
 * bytes past the decoded data and {@link SirixLZ77Codec#NATIVE_INPUT_TAIL_SLACK} past the frame.
 * The call is made under {@code Linker.Option.critical(true)}, which hands C a pointer straight
 * into the Java heap — so a violation is not a wrong answer, it is a corrupted process that crashes
 * later somewhere unrelated. That is exactly how it presented: an intermittent JVM SIGSEGV in an
 * unrelated test.
 *
 * <p>
 * Correctness assertions alone would not have caught it, because an overrun writes PAST the output
 * and leaves the output itself perfectly correct. So every case here places canary bytes beyond the
 * end of the destination segment and checks them afterwards: the failure mode is detected where it
 * happens rather than wherever the JVM later trips over it.
 *
 * <p>
 * What is asserted is precisely the contract, no more: the decoder MAY scribble anywhere inside the
 * buffer it was handed — its wildcopy writes up to 16 bytes for a 1-byte payload, which is what the
 * tail-slack requirement is for — and it may NEVER write one byte beyond it.
 *
 * <p>
 * Runs against whichever decoder is active. With the native library absent the Java decoder takes
 * every case and the test still passes — it pins the contract, not the implementation.
 */
final class SirixLZ77NativeContractTest {

  private static final byte CANARY = (byte) 0xA5;

  /** Slack values around the contract's boundary, including well under and exactly at it. */
  private static final int[] OUTPUT_SLACKS = {0, 1, 15, 16, 17, 63, SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK, 65, 256};

  /** Input tail slack to leave past the frame, including none at all. */
  private static final int[] INPUT_SLACKS = {0, 1, 15, SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK, 128};

  @Test
  void decodesCorrectlyAndWritesNothingPastTheOutputAtEverySlack() {
    final Random rng = new Random(0x51ACL);
    for (final byte[] payload : payloads(rng)) {
      for (final int inSlack : INPUT_SLACKS) {
        for (final int outSlack : OUTPUT_SLACKS) {
          roundTrip(payload, inSlack, outSlack, false);
        }
      }
    }
  }

  /**
   * The bounds-checked entry point owes the same contract on every valid frame: the same bytes, and
   * never a store past the destination it was handed. It is a second native entry whose in-pass
   * bounds must not have cost it the wildcopy slack agreement — and the slack matrix below drives
   * both the native guarded loop (slack at or above the contract) and the Java single pass (below
   * it), so the refusals and the decoded bytes are pinned on whichever one runs.
   */
  @Test
  void checkedDecodeMatchesTheSameContractAtEverySlack() {
    final Random rng = new Random(0x51ACL);
    for (final byte[] payload : payloads(rng)) {
      for (final int inSlack : INPUT_SLACKS) {
        for (final int outSlack : OUTPUT_SLACKS) {
          roundTrip(payload, inSlack, outSlack, true);
        }
      }
    }
  }

  /**
   * The shapes that put the decoder's unguarded paths at the very end of a frame: a short trailing
   * literal is the case whose 16-byte chunked copy reads past the compressed data.
   */
  private static java.util.List<byte[]> payloads(final Random rng) {
    final java.util.List<byte[]> out = new java.util.ArrayList<>();
    // Sizes straddling the decoder's 16/64-byte internal boundaries.
    for (final int n : new int[] {1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 255, 1023, 4096, 29_000}) {
      out.add(incompressible(rng, n)); // literal-heavy: long literal runs
      out.add(runs(rng, n)); // match-heavy: short distances (1, 2, 4)
      out.add(mixed(rng, n)); // both, with short trailing literals
    }
    return out;
  }

  private static byte[] incompressible(final Random rng, final int n) {
    final byte[] b = new byte[n];
    rng.nextBytes(b);
    return b;
  }

  private static byte[] runs(final Random rng, final int n) {
    final byte[] b = new byte[n];
    int i = 0;
    while (i < n) {
      final byte v = (byte) rng.nextInt(4);
      final int run = Math.min(n - i, 1 + rng.nextInt(40));
      Arrays.fill(b, i, i + run, v);
      i += run;
    }
    return b;
  }

  /** Compressible body followed by a 1..14 byte literal tail — the unguarded copy's worst case. */
  private static byte[] mixed(final Random rng, final int n) {
    final byte[] b = runs(rng, n);
    final int tail = Math.min(n, 1 + rng.nextInt(14));
    for (int i = n - tail; i < n; i++) {
      b[i] = (byte) rng.nextInt();
    }
    return b;
  }

  /** Bytes past the destination segment that must remain untouched. */
  private static final int GUARD = 256;

  private static void roundTrip(final byte[] payload, final int inputSlack, final int outputSlack,
      final boolean checked) {
    final byte[] encodeBuf = new byte[SirixLZ77Codec.maxEncodedSize(payload.length) + 64];
    final int encodedLen = SirixLZ77Codec.encode(MemorySegment.ofArray(payload), 0L, payload.length, encodeBuf, 0);

    // Compressed frame in an array with exactly `inputSlack` bytes behind it.
    final byte[] input = new byte[encodedLen + inputSlack];
    System.arraycopy(encodeBuf, 0, input, 0, encodedLen);

    // The destination the decoder is GIVEN is exactly payload + outputSlack; the array behind it
    // extends further and is filled with canaries, so a store past the segment is visible.
    final int visible = payload.length + outputSlack;
    final byte[] output = new byte[visible + GUARD];
    Arrays.fill(output, CANARY);
    final MemorySegment dst = MemorySegment.ofArray(output).asSlice(0, visible);

    final int decoded = checked
        ? SirixLZ77Codec.decodeChecked(input, 0, encodedLen, dst, 0L)
        : SirixLZ77Codec.decode(input, 0, encodedLen, dst, 0L);

    final String where =
        "len=" + payload.length + " inputSlack=" + inputSlack + " outputSlack=" + outputSlack + " checked=" + checked;
    assertEquals(payload.length, decoded, "decoded length: " + where);
    assertArrayEquals(payload, Arrays.copyOf(output, payload.length), "decoded bytes: " + where);
    for (int i = visible; i < output.length; i++) {
      assertEquals(CANARY, output[i], "decoder wrote " + (i - visible + 1) + " byte(s) past the destination: " + where);
    }
  }

  /** A non-zero destination offset must be honoured, and must not disturb what precedes it. */
  @Test
  void honoursOutputOffsetWithoutDisturbingWhatPrecedesIt() {
    final Random rng = new Random(0x0FF5E7L);
    for (final int offset : new int[] {1, 7, 8, 16, 64, 129}) {
      final byte[] payload = runs(rng, 3000);
      final byte[] encodeBuf = new byte[SirixLZ77Codec.maxEncodedSize(payload.length) + 64];
      final int encodedLen = SirixLZ77Codec.encode(MemorySegment.ofArray(payload), 0L, payload.length, encodeBuf, 0);
      final byte[] input = new byte[encodedLen + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
      System.arraycopy(encodeBuf, 0, input, 0, encodedLen);

      final int visible = offset + payload.length + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
      final byte[] output = new byte[visible + GUARD];
      Arrays.fill(output, CANARY);
      final MemorySegment dst = MemorySegment.ofArray(output).asSlice(0, visible);

      final int decoded = SirixLZ77Codec.decode(input, 0, encodedLen, dst, offset);

      assertEquals(payload.length, decoded, "decoded length at offset " + offset);
      assertArrayEquals(payload, Arrays.copyOfRange(output, offset, offset + payload.length),
          "decoded bytes at offset " + offset);
      for (int i = 0; i < offset; i++) {
        assertEquals(CANARY, output[i], "decoder wrote before the output offset " + offset);
      }
      for (int i = visible; i < output.length; i++) {
        assertEquals(CANARY, output[i], "decoder wrote past the destination at offset " + offset);
      }
    }
  }

  /**
   * A length extension that sums to exactly 2^32 must be refused, by whichever decoder runs.
   *
   * <p>
   * FMT-216 sums successive extension bytes "until a byte below 255, included in that sum". The frame
   * built below declares 64 decoded bytes and then spends 16,843,008 bytes of {@code ff} followed by
   * {@code f1}, so the literal length reaches {@code 15 + 255 * 16_843_008 + 241}, which is 2^32 to
   * the byte. A decoder that adds first and bounds afterwards wraps that to zero, skips the literal
   * and goes on to produce 64 bytes from a malformed frame — the behaviour this rejects.
   *
   * <p>
   * Both decoders are driven through the untrusted entry point: with the frame's array carrying the
   * documented input tail slack the native loop takes it, and with the array ending at the frame the
   * native dispatch declines and the Java single pass takes it. The dispatch counters say which ran,
   * so neither can pass by not being exercised.
   */
  @Test
  void aLengthExtensionSummingToFourGibibytesIsRefusedByWhicheverDecoderRuns() {
    final byte[] frame = wrappingLiteralExtensionFrame();
    for (final boolean withSlack : new boolean[] {true, false}) {
      if (withSlack && !SirixLZ77NativeDecoder.isGuardedAvailable()) {
        continue;
      }
      final byte[] input = Arrays.copyOf(frame, frame.length + (withSlack
          ? SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK
          : 0));
      final int visible = 64 + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
      final byte[] output = new byte[visible + GUARD];
      Arrays.fill(output, CANARY);
      final MemorySegment dst = MemorySegment.ofArray(output).asSlice(0, visible);
      final long nativeBefore = SirixLZ77Codec.getNativeCallCount();
      final long javaBefore = SirixLZ77Codec.getJavaCallCount();

      assertThrows(IllegalStateException.class, () -> SirixLZ77Codec.decodeChecked(input, 0, frame.length, dst, 0L),
          "a literal length summing to 2^32 must be refused (inputSlack=" + withSlack + ")");

      final long natives = SirixLZ77Codec.getNativeCallCount() - nativeBefore;
      final long javas = SirixLZ77Codec.getJavaCallCount() - javaBefore;
      if (natives + javas == 1) {
        assertEquals(withSlack
            ? 1L
            : 0L, natives, "the native decoder must be the one refusing when the frame carries input slack");
        assertEquals(withSlack
            ? 0L
            : 1L, javas, "the Java single pass must be the one refusing when it does not");
      }
      for (int i = visible; i < output.length; i++) {
        assertEquals(CANARY, output[i], "a refused frame wrote past the destination (inputSlack=" + withSlack + ")");
      }
    }
  }

  /**
   * {@code fd 40 | 10 2a 01 00 | f0 [ff x 16_843_008] f1 | 01 00 | f0 28 [2a x 55]} — the frame whose
   * literal-length extension sums to 2^32. Generated rather than stored: it is sixteen mebibytes.
   */
  private static byte[] wrappingLiteralExtensionFrame() {
    final int fills = 16_843_008;
    assertEquals(1L << 32, 15L + 255L * fills + 0xF1, "the extension must sum to exactly 2^32");
    final byte[] frame = new byte[fills + 67];
    int at = 0;
    frame[at++] = SirixLZ77Codec.FRAME_MARKER;
    frame[at++] = 0x40;
    frame[at++] = 0x10;
    frame[at++] = 0x2A;
    frame[at++] = 0x01;
    frame[at++] = 0x00;
    frame[at++] = (byte) 0xF0;
    Arrays.fill(frame, at, at + fills, (byte) 0xFF);
    at += fills;
    frame[at++] = (byte) 0xF1;
    frame[at++] = 0x01;
    frame[at++] = 0x00;
    frame[at++] = (byte) 0xF0;
    frame[at++] = 0x28;
    Arrays.fill(frame, at, at + 55, (byte) 0x2A);
    at += 55;
    assertEquals(frame.length, at, "frame layout");
    return frame;
  }
}
