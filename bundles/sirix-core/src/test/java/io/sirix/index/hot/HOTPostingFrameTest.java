package io.sirix.index.hot;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HOTPostingFrameTest {
  @Test
  void goldenPostingsPinWidthsAndEndianness() {
    assertEncoding("0101000000", 0);
    assertEncoding("0101ffff00", 65535);
    assertEncoding("0102010001ff", 256, 511);
    assertEncoding("01020100020100", 256, 512);
    assertEncoding("0102000002ffff", 0, 65535);
  }

  @Test
  void zeroBaseSingletonWorksThroughAllOrdinalReaders() {
    final MemorySegment zero = MemorySegment.ofArray(encode(new int[] {0}));
    final MemorySegment raw = MemorySegment.ofArray(raw(new int[] {0, 1}));
    assertEquals(0, HOTPostingFrame.ordinalAt(zero, 0, 0));
    assertEquals(0, HOTPostingFrame.indexOf(zero, 0, 0));
    assertEquals(-2, HOTPostingFrame.indexOf(zero, 0, 1));
    final List<Integer> result = new ArrayList<>();
    HOTPostingFrame.intersect(zero, 0, raw, 0, result::add);
    assertEquals(List.of(0), result);
  }

  @Test
  void rejectsNonCanonicalOrTruncatedPostings() {
    for (final String hex : List.of("01", "0100000000", "0141000002", "0101000001", "010100000000", "0102000000",
        "010200000301", "0102000001", "010200000100", "01030000010101", "01030000010201", "0102ffff0101",
        "010200000200ff", "010201000200ff")) {
      final byte[] bytes = HexFormat.of().parseHex(hex);
      assertThrows(IllegalStateException.class,
          () -> HOTPostingFrame.validate(MemorySegment.ofArray(bytes), 0, bytes.length), hex);
    }
  }

  @Test
  void ordinalSearchAndSortedIntersectionMatchRawOracle() {
    final Random random = new Random(0x485054);
    for (int iteration = 0; iteration < 128; iteration++) {
      final int[] a = keys(random);
      final int[] b = keys(random);
      final MemorySegment rawA = MemorySegment.ofArray(raw(a));
      final MemorySegment rawB = MemorySegment.ofArray(raw(b));
      final MemorySegment encodedA = MemorySegment.ofArray(encode(a));
      final MemorySegment encodedB = MemorySegment.ofArray(encode(b));
      for (int ordinal = 0; ordinal < a.length; ordinal++) {
        assertEquals(a[ordinal], HOTPostingFrame.ordinalAt(rawA, 0, ordinal));
        assertEquals(a[ordinal], HOTPostingFrame.ordinalAt(encodedA, 0, ordinal));
        assertEquals(ordinal, HOTPostingFrame.indexOf(encodedA, 0, a[ordinal]));
        final int absentOrPresent = a[ordinal] ^ 1;
        assertEquals(Arrays.binarySearch(a, absentOrPresent), HOTPostingFrame.indexOf(encodedA, 0, absentOrPresent));
      }
      final List<Integer> expected = new ArrayList<>();
      for (final int key : a) {
        if (Arrays.binarySearch(b, key) >= 0) {
          expected.add(key);
        }
      }
      for (final MemorySegment left : List.of(rawA, encodedA)) {
        for (final MemorySegment right : List.of(rawB, encodedB)) {
          final List<Integer> actual = new ArrayList<>();
          HOTPostingFrame.intersect(left, 0, right, 0, actual::add);
          assertEquals(expected, actual);
        }
      }
    }
  }

  @Test
  void materializesTheOriginalPostingInsideAnExistingDestination() {
    final int[] keys = {0, 255, 256, 65499, 65535};
    final byte[] encoded = encode(keys);
    final byte[] expected = raw(keys);
    final byte sentinel = 23;
    final byte[] destination = new byte[expected.length + 9];
    Arrays.fill(destination, sentinel);
    assertEquals(expected.length,
        HOTPostingFrame.copyOrdinary(MemorySegment.ofArray(encoded), 0, MemorySegment.ofArray(destination), 5));
    assertArrayEquals(expected, Arrays.copyOfRange(destination, 5, 5 + expected.length));
    for (int i = 0; i < destination.length; i++) {
      if (i < 5 || i >= 5 + expected.length) {
        assertEquals(sentinel, destination[i]);
      }
    }
  }

  @Test
  void invalidOrNonChunkLongsDoNotBecomeFrames() {
    for (final int[] keys : new int[][] {{}, {1, 1}, {5, 4}, {-1}, {65536}, new int[65]}) {
      final byte[] bytes = raw(keys);
      assertEquals(0, HOTPostingFrame.encodeIfSmaller(MemorySegment.ofArray(bytes), 0, bytes.length,
          MemorySegment.ofArray(new byte[bytes.length]), 0));
    }
  }

  private static void assertEncoding(final String bytes, final int... keys) {
    assertArrayEquals(HexFormat.of().parseHex(bytes), encode(keys));
  }

  private static byte[] encode(final int[] keys) {
    final byte[] bytes = raw(keys);
    final MemorySegment destination = MemorySegment.ofArray(new byte[bytes.length + 3]);
    final int written = HOTPostingFrame.encodeIfSmaller(MemorySegment.ofArray(bytes), 0, bytes.length, destination, 3);
    HOTPostingFrame.validate(destination, 3, written);
    return destination.asSlice(3, written).toArray(ValueLayout.JAVA_BYTE);
  }

  private static byte[] raw(final int[] keys) {
    final ByteBuffer raw = ByteBuffer.allocate(2 + keys.length * Long.BYTES);
    raw.put((byte) 0).put((byte) keys.length);
    for (final int key : keys) {
      raw.putLong(key);
    }
    return raw.array();
  }

  private static int[] keys(final Random random) {
    final int count = 1 + random.nextInt(64);
    final int bound = random.nextBoolean()
        ? 90
        : 65536;
    final TreeSet<Integer> set = new TreeSet<>();
    while (set.size() < count) {
      set.add(random.nextInt(bound));
    }
    return set.stream().mapToInt(Integer::intValue).toArray();
  }
}
