package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The SIMD dict-id kernels against ground truth.
 *
 * <p>These replaced a scalar loop that had been correct for a long time, on a path where being
 * wrong is a wrong ANSWER rather than a slowdown: a string equality predicate is answered purely by
 * counting occurrences of one dict id, so an off-by-one lane or a mis-masked tail silently changes
 * the count a query returns. The oracle here is therefore the value array the payload was packed
 * from, not another decoder — comparing two decoders would agree if both mis-read the same tail.
 *
 * <p>What the randomisation is aimed at: bit widths that do and do not divide a byte, value counts
 * either side of the vector width (so the scalar tail runs, and runs alone), a non-zero start index
 * so the kernel's absolute-vs-relative indexing has to be right, and a payload cut to the minimum
 * length so the final 64-bit window reads past the end and exercises the short-read path.
 */
final class StringRegionSimdTest {

  /** Pack {@code values} LSB-first at {@code bitWidth} bits each, as the region writer does. */
  private static byte[] pack(final int[] values, final int bitWidth, final int dictIdsOffset) {
    final long totalBits = (long) values.length * bitWidth;
    // Exactly the bytes the values need, so the last window read runs off the end on purpose.
    final byte[] payload = new byte[dictIdsOffset + (int) ((totalBits + 7) >>> 3)];
    for (int i = 0; i < values.length; i++) {
      long bitPos = (long) i * bitWidth;
      long v = values[i] & maskOf(bitWidth);
      int remaining = bitWidth;
      while (remaining > 0) {
        final int byteOff = dictIdsOffset + (int) (bitPos >>> 3);
        final int shift = (int) (bitPos & 7L);
        final int bitsThisByte = Math.min(8 - shift, remaining);
        final int chunk = (int) (v & ((1L << bitsThisByte) - 1L));
        payload[byteOff] |= (byte) (chunk << shift);
        v >>>= bitsThisByte;
        remaining -= bitsThisByte;
        bitPos += bitsThisByte;
      }
    }
    return payload;
  }

  private static long maskOf(final int bitWidth) {
    return bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
  }

  @Test
  @DisplayName("the SIMD dict-id count matches the values it was packed from")
  void countMatchesGroundTruth() {
    final Random rnd = new Random(20260804L);
    for (int trial = 0; trial < 4000; trial++) {
      final int bitWidth = 1 + rnd.nextInt(32);
      final int count = rnd.nextInt(300);
      final int dictIdsOffset = rnd.nextInt(17);
      final long mask = maskOf(bitWidth);
      // A small id space, so the target id actually occurs often enough to be interesting.
      final int idSpace = (int) Math.min(mask, 7L) + 1;
      final int[] values = new int[count];
      for (int i = 0; i < count; i++) {
        values[i] = rnd.nextInt(idSpace);
      }
      final byte[] payload = pack(values, bitWidth, dictIdsOffset);

      final int start = count == 0 ? 0 : rnd.nextInt(count);
      final int n = count == 0 ? 0 : rnd.nextInt(count - start + 1);
      final int dictId = rnd.nextInt(idSpace);

      long expected = 0;
      for (int i = start; i < start + n; i++) {
        if (values[i] == dictId) {
          expected++;
        }
      }

      final long actual =
          StringRegionSimd.countDictId(payload, dictIdsOffset, bitWidth, start, n, dictId);
      assertTrue(actual >= 0L, "kernel declined a width it claims to support: " + bitWidth);
      assertEquals(expected, actual,
                   "bitWidth=" + bitWidth + " offset=" + dictIdsOffset + " start=" + start
                       + " n=" + n + " dictId=" + dictId);
    }
  }

  @Test
  @DisplayName("the masked count agrees with the liveness bitmap it was given")
  void maskedCountMatchesGroundTruth() {
    final Random rnd = new Random(987654321L);
    for (int trial = 0; trial < 4000; trial++) {
      final int bitWidth = 1 + rnd.nextInt(32);
      final int count = 1 + rnd.nextInt(300);
      final int dictIdsOffset = rnd.nextInt(17);
      final long mask = maskOf(bitWidth);
      final int idSpace = (int) Math.min(mask, 7L) + 1;
      final int[] values = new int[count];
      for (int i = 0; i < count; i++) {
        values[i] = rnd.nextInt(idSpace);
      }
      final byte[] payload = pack(values, bitWidth, dictIdsOffset);

      final int start = rnd.nextInt(count);
      final int n = rnd.nextInt(count - start + 1);
      final int dictId = rnd.nextInt(idSpace);

      // Liveness indexed RELATIVE to start — the convention the merge relies on.
      final long[] liveBits = new long[(Math.max(n, 1) + 63) >>> 6];
      final boolean[] live = new boolean[n];
      for (int i = 0; i < n; i++) {
        // Skew towards live: the interesting merges shadow a minority of slots.
        live[i] = rnd.nextInt(4) != 0;
        if (live[i]) {
          liveBits[i >>> 6] |= 1L << (i & 63);
        }
      }

      long expected = 0;
      for (int i = 0; i < n; i++) {
        if (live[i] && values[start + i] == dictId) {
          expected++;
        }
      }

      final long actual = StringRegionSimd.countDictIdMasked(payload, dictIdsOffset, bitWidth,
                                                             start, n, dictId, liveBits);
      assertTrue(actual >= 0L, "kernel declined a width it claims to support: " + bitWidth);
      assertEquals(expected, actual,
                   "bitWidth=" + bitWidth + " offset=" + dictIdsOffset + " start=" + start
                       + " n=" + n + " dictId=" + dictId);
    }
  }

  /**
   * An all-ones bitmap has to give exactly the unmasked answer. The merge picks between the two
   * kernels on the live count, so a divergence here would show up only on pages that happen to be
   * fully live — which is most of them, and therefore the hardest case to notice.
   */
  @Test
  @DisplayName("a fully live mask reproduces the unmasked count")
  void fullyLiveMaskMatchesUnmasked() {
    final Random rnd = new Random(13579L);
    for (int trial = 0; trial < 1500; trial++) {
      final int bitWidth = 1 + rnd.nextInt(32);
      final int count = 1 + rnd.nextInt(200);
      final long mask = maskOf(bitWidth);
      final int idSpace = (int) Math.min(mask, 7L) + 1;
      final int[] values = new int[count];
      for (int i = 0; i < count; i++) {
        values[i] = rnd.nextInt(idSpace);
      }
      final byte[] payload = pack(values, bitWidth, 0);
      final int dictId = rnd.nextInt(idSpace);

      final long[] liveBits = new long[(count + 63) >>> 6];
      for (int i = 0; i < count; i++) {
        liveBits[i >>> 6] |= 1L << (i & 63);
      }

      assertEquals(StringRegionSimd.countDictId(payload, 0, bitWidth, 0, count, dictId),
                   StringRegionSimd.countDictIdMasked(payload, 0, bitWidth, 0, count, dictId, liveBits),
                   "bitWidth=" + bitWidth + " count=" + count);
    }
  }
}
