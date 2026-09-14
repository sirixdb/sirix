/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ProjectionSortedLeafTest {

  @Test
  void prefixCompressedLeafRoundTripsAndSeeksWithoutRowObjects() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    final byte[][] keys = new byte[4][];
    final byte[][] payloads = new byte[4][];
    for (int i = 0; i < keys.length; i++) {
      writer.reset();
      appendString(writer, "commit");
      appendString(writer, "create");
      appendString(writer, "app.bsky.feed.post");
      appendString(writer, "user-" + i);
      writer.appendLong(i * 10L);
      writer.appendRecordKey(i + 1);
      keys[i] = writer.copyKey();
      payloads[i] = new byte[] {(byte) (i + 10)};
    }
    final ProjectionSortedLeaf encoded = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
    assertNotNull(encoded);
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.open(encoded.encodedBytes());
    assertEquals(4, leaf.rowCount());
    assertEquals(3, leaf.lowerBound(keys[3]));
    assertEquals(0, leaf.lowerBound(new byte[0]));
    assertEquals(4, leaf.lowerBound(new byte[] {(byte) 0xFF}));
    for (int i = 0; i < keys.length; i++) {
      assertArrayEquals(keys[i], leaf.copyKey(i));
      assertArrayEquals(payloads[i], leaf.copyPayload(i));
      assertEquals(0, leaf.compareRowKey(i, keys[i]));
      assertEquals(i, leaf.lowerBound(keys[i]));
    }
    assertEquals(1, leaf.lowerBound(Arrays.copyOf(keys[0], keys[0].length + 1)));
    assertEquals(0, leaf.compareRowKey(0, keys[0]));
  }

  @Test
  void capacityAndMalformedRowsFailClosed() {
    final byte[][] keys = new byte[ProjectionSortedLeaf.MAX_ROWS + 1][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new byte[] {(byte) (i >>> 8), (byte) i};
    }
    assertNull(ProjectionSortedLeaf.encode(keys, null, keys.length));
    final byte[][] duplicate = {new byte[] {1}, new byte[] {1}};
    assertThrows(IllegalArgumentException.class, () -> ProjectionSortedLeaf.encode(duplicate, null, 2));
    final ProjectionSortedLeaf single = ProjectionSortedLeaf.encode(new byte[][] {new byte[] {1, 2}}, null, 1);
    assertNotNull(single);
    final byte[] truncated = Arrays.copyOf(single.encodedBytes(), single.encodedBytes().length - 1);
    assertThrows(IllegalArgumentException.class, () -> ProjectionSortedLeaf.open(truncated));
    final byte[] badOffset = single.encodedBytes().clone();
    badOffset[HEADER_FIRST_OFFSET]++;
    assertThrows(IllegalArgumentException.class, () -> ProjectionSortedLeaf.open(badOffset));
  }

  @Test
  void prefixBoundarySearchSkipsOnlyMatchingRows() {
    final byte[][] keys = {{1, 1, 10, 0}, {1, 1, 10, 1}, {1, 1, 20}, {1, 2, 10}};
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, null, keys.length);
    assertNotNull(leaf);
    assertEquals(1, leaf.commonPrefixLength());
    assertEquals(4, leaf.firstNonPrefixRowAfter(0, new byte[] {1}, 1));
    assertEquals(3, leaf.firstNonPrefixRowAfter(0, new byte[] {1, 1}, 2));
    assertEquals(2, leaf.firstNonPrefixRowAfter(0, new byte[] {1, 1, 10}, 3));
    assertEquals(2, leaf.firstNonPrefixRowAfter(1, new byte[] {1, 1, 10}, 3));
    assertEquals(4, leaf.firstNonPrefixRowAfter(3, new byte[] {1, 2}, 2));
    assertThrows(IllegalArgumentException.class,
        () -> leaf.firstNonPrefixRowAfter(0, new byte[] {1, 2}, 2));
  }

  @Test
  void localInsertAndDeleteReencodeOnlyOneLeafAcrossPrefixChanges() {
    final byte[][] keys = {{1, 1, 10}, {1, 1, 20}};
    final byte[][] payloads = {{10}, {20}};
    final ProjectionSortedLeaf original = ProjectionSortedLeaf.encode(keys, payloads, 2);
    assertNotNull(original);
    assertEquals(2, original.commonPrefixLength());

    final byte[] newFirst = {0, 9, 9};
    final ProjectionSortedLeaf shortened = original.withInserted(newFirst, new byte[] {9});
    assertNotNull(shortened);
    assertEquals(0, shortened.commonPrefixLength());
    assertArrayEquals(newFirst, ProjectionSortedLeaf.open(shortened.encodedBytes()).copyKey(0));
    assertArrayEquals(keys[0], shortened.copyKey(1));
    assertArrayEquals(payloads[1], shortened.copyPayload(2));
    assertArrayEquals(keys[0], original.copyKey(0));

    final ProjectionSortedLeaf restored = shortened.withRemoved(newFirst);
    assertNotNull(restored);
    assertEquals(2, restored.commonPrefixLength());
    assertArrayEquals(original.encodedBytes(), ProjectionSortedLeaf.open(restored.encodedBytes()).encodedBytes());

    final byte[] middle = {1, 1, 15};
    final ProjectionSortedLeaf insertedMiddle = restored.withInserted(middle, new byte[] {15});
    assertNotNull(insertedMiddle);
    assertEquals(1, insertedMiddle.lowerBound(middle));
    assertArrayEquals(new byte[] {15}, insertedMiddle.copyPayload(1));
    assertThrows(IllegalArgumentException.class, () -> insertedMiddle.withInserted(middle, new byte[0]));
    assertThrows(IllegalStateException.class, () -> insertedMiddle.withRemoved(new byte[] {1, 1, 16}));
    assertNull(ProjectionSortedLeaf.encode(new byte[][] {{42}}, null, 1).withRemoved(new byte[] {42}));
  }

  private static final int HEADER_FIRST_OFFSET = 9 + 2;

  private static void appendString(final ProjectionSortKeyCodec.Writer writer, final String value) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
  }
}
