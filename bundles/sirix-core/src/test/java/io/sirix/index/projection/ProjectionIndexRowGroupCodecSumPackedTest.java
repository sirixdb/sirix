package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ProjectionIndexRowGroupCodec#sumPacked(byte[], int, int, int)} against the unpack-then-add
 * reference for every width, every arm (int-refill / windowed / byte-wise tail), tight and padded
 * sources, counts that are not word multiples, and a non-zero start offset. The fused sum is the
 * dense-block arm of the segment fold — a wrong tail there is a wrong SUM at 100M rows.
 */
final class ProjectionIndexRowGroupCodecSumPackedTest {

  private static byte[] pack(final long[] values, final int width, final int pos, final int slack) {
    final int bytes = (int) (((long) values.length * width + 7) >>> 3);
    final byte[] src = new byte[pos + bytes + slack];
    long bit = (long) pos << 3;
    for (final long v : values) {
      for (int b = 0; b < width; b++) {
        if (((v >>> b) & 1L) != 0L) {
          src[(int) (bit >>> 3)] |= (byte) (1 << (int) (bit & 7));
        }
        bit++;
      }
    }
    return src;
  }

  private static long reference(final byte[] src, final int pos, final int count, final int width) {
    final long[] out = new long[count];
    ProjectionIndexRowGroupCodec.unpackInto(src, pos, count, width, 0L, out, 0);
    long sum = 0L;
    for (final long v : out) {
      sum += v;
    }
    return sum;
  }

  /** The widths the writer emits: 1..56 packed and 64 raw ({@link ProjectionIndexRowGroupCodec#clampPackWidth}). */
  private static int[] writerWidths() {
    final int[] widths = new int[57];
    for (int w = 1; w <= 56; w++) {
      widths[w - 1] = w;
    }
    widths[56] = 64;
    return widths;
  }

  @Test
  void everyWidthEveryTailMatchesTheReference() {
    final Random rnd = new Random(0x5EED_5EEDL);
    final int[] counts = {1, 2, 7, 63, 64, 65, 127, 128, 1000, 1024, 1025};
    for (final int width : writerWidths()) {
      final long mask = width == 64 ? -1L : (1L << width) - 1L;
      for (final int count : counts) {
        final long[] values = new long[count];
        long expected = 0L;
        for (int i = 0; i < count; i++) {
          values[i] = rnd.nextLong() & mask;
          expected += values[i];
        }
        for (final int pos : new int[] {0, 3, 8}) {
          for (final int slack : new int[] {0, 1, 7, 8, 64}) {
            final byte[] src = pack(values, width, pos, slack);
            final long ref = reference(src, pos, count, width);
            assertEquals(expected, ref, "reference width=" + width + " count=" + count);
            assertEquals(expected, ProjectionIndexRowGroupCodec.sumPacked(src, pos, count, width),
                "width=" + width + " count=" + count + " pos=" + pos + " slack=" + slack);
          }
        }
      }
    }
  }

  @Test
  void allOnesAndAllZerosAtEveryWidth() {
    for (final int width : writerWidths()) {
      final long mask = width == 64 ? -1L : (1L << width) - 1L;
      final int count = 1024;
      final long[] ones = new long[count];
      Arrays.fill(ones, mask);
      final byte[] src = pack(ones, width, 0, 0);
      // Wrap-around is defined (two's complement); the reference adds the same values in the same ring.
      assertEquals(reference(src, 0, count, width), ProjectionIndexRowGroupCodec.sumPacked(src, 0, count, width),
          "ones width=" + width);
      assertEquals(0L, ProjectionIndexRowGroupCodec.sumPacked(new byte[(count * width + 7) / 8], 0, count, width),
          "zeros width=" + width);
    }
  }

  @Test
  void widthZeroAndEmptyCountAreZero() {
    final byte[] src = {(byte) 0xFF, (byte) 0xFF};
    assertEquals(0L, ProjectionIndexRowGroupCodec.sumPacked(src, 0, 100, 0));
    assertEquals(0L, ProjectionIndexRowGroupCodec.sumPacked(src, 0, 0, 5));
  }
}
