package io.sirix.index.projection;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PackedIntegerWordDecodeTest {

  @Test
  void everyIntegerWidthAndTailMatchesIndependentBitEncoding() {
    final Random random = new Random(0x8d3c04);
    for (int width = 0; width <= Integer.SIZE; width++) {
      final long mask = (1L << width) - 1;
      for (final int count : new int[] {0, 1, 7, 8, 9, 15, 16, 63, 64, 65, 1023, 1024, 4097}) {
        final int[] expected = new int[count];
        for (int row = 0; row < count; row++) {
          expected[row] = (int) (random.nextInt() & mask);
        }
        for (final int offset : new int[] {0, 1, 7}) {
          for (final int tail : new int[] {0, 8}) {
            final byte[] encoded = encodeBits(expected, width, offset, tail);
            final ProjectionIndexRowGroupCodec.Cursor cursor = new ProjectionIndexRowGroupCodec.Cursor(encoded, offset);
            final int[] actual = new int[count + 3];
            Arrays.fill(actual, 0x59FEC17);
            ProjectionIndexRowGroupCodec.unpackIntsInto(cursor, count, width, actual);
            assertArrayEquals(expected, Arrays.copyOf(actual, count), "width=" + width + ", count=" + count);
            assertEquals(offset + ((count * width + 7) >>> 3), cursor.position());
            for (int row = count; row < actual.length; row++) {
              assertEquals(0x59FEC17, actual[row], "output beyond count must stay untouched");
            }
          }
        }
      }
    }
  }

  @Test
  void truncatedNarrowStreamsFailAtTheirActualBoundary() {
    for (int width = 1; width <= Byte.SIZE; width++) {
      for (final int count : new int[] {1, 7, 8, 9, 63, 64, 65, 1024}) {
        final byte[] valid = encodeBits(new int[count], width, 3, 0);
        final byte[] truncated = Arrays.copyOf(valid, valid.length - 1);
        final int bits = width;
        assertThrows(IndexOutOfBoundsException.class,
            () -> ProjectionIndexRowGroupCodec.unpackIntsInto(new ProjectionIndexRowGroupCodec.Cursor(truncated, 3),
                count, bits, new int[count]));
      }
    }
  }

  private static byte[] encodeBits(final int[] values, final int width, final int offset, final int tail) {
    final int size = (values.length * width + 7) >>> 3;
    final byte[] bytes = new byte[offset + size + tail];
    Arrays.fill(bytes, 0, offset, (byte) 0xA5);
    Arrays.fill(bytes, offset + size, bytes.length, (byte) 0xA5);
    for (int row = 0; row < values.length; row++) {
      for (int bit = 0; bit < width; bit++) {
        if (((values[row] >>> bit) & 1) != 0) {
          final int position = row * width + bit;
          bytes[offset + (position >>> 3)] |= (byte) (1 << (position & 7));
        }
      }
    }
    return bytes;
  }
}
