/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ValueDictionaryComparisonTest {

  @Test
  void byteRangesPreserveUtf16OrderIncludingSharedPartialCharacters() {
    final int[] alphabet = {0, 65, 127, 128, 0x7FF, 0x800, 0xD7FF, 0xE000, 0xFFFF,
        0x10000, 0x10001, 0x1F642, 0x10FFFF};
    final SplittableRandom random = new SplittableRandom(0xC011A710);
    for (int iteration = 0; iteration < 2_000; iteration++) {
      final String prefix = value(random, alphabet);
      final String left = prefix + value(random, alphabet);
      final String right = iteration % 7 == 0 ? left : prefix + value(random, alphabet);
      final byte[] a = left.getBytes(StandardCharsets.UTF_8);
      final byte[] b = right.getBytes(StandardCharsets.UTF_8);
      final byte[] paddedA = new byte[a.length + 13];
      final byte[] paddedB = new byte[b.length + 17];
      System.arraycopy(a, 0, paddedA, 3, a.length);
      System.arraycopy(b, 0, paddedB, 7, b.length);
      final ValueDictionaryEntryNode first = new ValueDictionaryEntryNode(1, a);
      final ValueDictionaryEntryNode second = new ValueDictionaryEntryNode(2, b);
      final int expected = Integer.signum(left.compareTo(right));
      assertEquals(expected, Integer.signum(ValueDictionaryEntryNode.compareUtf16Range(paddedA, 3, a.length,
          paddedB, 7, b.length)));
      assertEquals(expected, Integer.signum(first.compareValueUtf16(second)));
      assertEquals(expected, Integer.signum(first.compareToRange(paddedB, 7, b.length)));
    }
  }

  @Test
  void rejectsRangesOutsideTheirInputs() {
    final byte[] a = {65, 66};
    final byte[] b = {65};
    assertThrows(IndexOutOfBoundsException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(a, 1, a.length, b, 0, b.length));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(a, 0, a.length, b, -1, b.length));
  }

  @Test
  void malformedDifferingSuffixStillFailsClosed() {
    final byte[] left = {65, (byte) 0xC2, (byte) 0x80};
    final byte[] right = {65, (byte) 0xC2, 65};
    assertThrows(IllegalStateException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(left, 0, left.length, right, 0, right.length));
    assertEquals(0, ValueDictionaryEntryNode.compareUtf16Range(right, 0, right.length,
        Arrays.copyOf(right, right.length), 0, right.length));
  }

  private static String value(final SplittableRandom random, final int[] alphabet) {
    final StringBuilder value = new StringBuilder();
    final int length = random.nextInt(24);
    for (int i = 0; i < length; i++) {
      value.appendCodePoint(alphabet[random.nextInt(alphabet.length)]);
    }
    return value.toString();
  }
}
