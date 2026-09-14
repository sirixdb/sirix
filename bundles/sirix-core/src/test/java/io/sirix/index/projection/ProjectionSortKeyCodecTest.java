/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortKeyCodecTest {

  @Test
  void signedNumbersAndMissingValuesHaveTupleOrder() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendMissing();
    final byte[] missing = writer.copyKey();
    final long[] values = {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE};
    byte[] previous = missing;
    for (final long value : values) {
      writer.reset();
      writer.appendLong(value);
      final byte[] key = writer.copyKey();
      assertTrue(Arrays.compareUnsigned(previous, key) < 0);
      previous = key;
    }
    writer.reset();
    writer.appendBoolean(false);
    final byte[] falseKey = writer.copyKey();
    writer.reset();
    writer.appendBoolean(true);
    assertTrue(Arrays.compareUnsigned(falseKey, writer.copyKey()) < 0);
  }

  @Test
  void escapedStringsKeepBinaryOrderAndRemainPrefixFree() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    final String[] values = {"", "\u0000", "\u0000\u0000", "\u0000a", "a", "a\u0000", "aa", "é", "😀"};
    byte[] previous = null;
    for (final String value : values) {
      writer.reset();
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      writer.appendUtf8(utf8, 0, utf8.length);
      final byte[] key = writer.copyKey();
      if (previous != null) {
        assertTrue(Arrays.compareUnsigned(previous, key) < 0, value);
      }
      previous = key;
    }
    writer.reset();
    writer.appendMissing();
    assertTrue(Arrays.compareUnsigned(writer.copyKey(), previous) < 0);
  }

  @Test
  void compositePrefixSelectsOnlyMatchingRows() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, "commit");
    appendString(writer, "create");
    final byte[] prefix = writer.copyKey();
    final byte[] upper = ProjectionSortKeyCodec.prefixUpperExclusive(prefix);
    final String[][] rows = {{"commit", "create"}, {"commit", "created"},
        {"commits", "create"}, {"commit", "delete"}, {"commit", "create"}};
    for (int i = 0; i < rows.length; i++) {
      writer.reset();
      appendString(writer, rows[i][0]);
      appendString(writer, rows[i][1]);
      appendString(writer, "app.bsky.feed.post");
      writer.appendRecordKey(i);
      final byte[] key = writer.copyKey();
      final boolean inRange = Arrays.compareUnsigned(key, prefix) >= 0
          && Arrays.compareUnsigned(key, upper) < 0;
      assertEquals(i == 0 || i == 4, inRange, "row " + i);
    }
    assertArrayEquals(new byte[] {1, 3}, ProjectionSortKeyCodec.prefixUpperExclusive(new byte[] {1, 2, (byte) 0xFF}));
    assertNull(ProjectionSortKeyCodec.prefixUpperExclusive(new byte[] {(byte) 0xFF}));
    assertNull(ProjectionSortKeyCodec.prefixUpperExclusive(new byte[0]));
  }

  @Test
  void recordSuffixAndBufferReuseDoNotAliasFinishedKeys() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    final byte[] large = new byte[1024];
    Arrays.fill(large, (byte) 'x');
    writer.appendUtf8(large, 0, large.length);
    writer.appendRecordKey(1);
    final byte[] first = writer.copyKey();
    assertEquals(1035, writer.length());
    writer.reset();
    writer.appendUtf8(large, 0, large.length);
    writer.appendRecordKey(2);
    assertTrue(Arrays.compareUnsigned(first, writer.copyKey()) < 0);
    assertThrows(IllegalArgumentException.class, () -> writer.appendRecordKey(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> writer.appendUtf8(large, 0, large.length + 1));
  }

  private static void appendString(final ProjectionSortKeyCodec.Writer writer, final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(bytes, 0, bytes.length);
  }
}
