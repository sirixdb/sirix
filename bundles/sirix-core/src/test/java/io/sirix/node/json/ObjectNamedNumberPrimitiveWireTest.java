/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node.json;

import io.sirix.page.NodeFieldLayout;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Wire-compatibility coverage for the allocation-free fused integral-number writers. */
final class ObjectNamedNumberPrimitiveWireTest {

  private static final int CAPACITY = 256;
  private static final long NODE_KEY = 71L;
  private static final long PARENT_KEY = 43L;
  private static final long RIGHT_SIBLING_KEY = 72L;
  private static final long LEFT_SIBLING_KEY = 69L;
  private static final int NAME_KEY = -1_234_567;
  private static final long PATH_NODE_KEY = 101L;
  private static final int PREVIOUS_REVISION = -1;
  private static final int LAST_MODIFIED_REVISION = 17;
  private static final long HASH = 0x1020_3040_5060_7080L;

  @Test
  void primitiveIntWriterIsByteIdenticalToBoxedWriterAtBoundaries() {
    final int[] values = {Integer.MIN_VALUE, -65, -1, 0, 63, 64, Integer.MAX_VALUE};

    for (final int value : values) {
      assertArrayEquals(writeBoxed(Integer.valueOf(value)), writeInt(value),
          "primitive int wire differs for " + value);
    }
  }

  @Test
  void primitiveLongWriterIsByteIdenticalToBoxedWriterAtBoundaries() {
    final long[] values = {
        Long.MIN_VALUE,
        (long) Integer.MIN_VALUE - 1L,
        -65L,
        -1L,
        0L,
        63L,
        64L,
        (long) Integer.MAX_VALUE + 1L,
        Long.MAX_VALUE
    };

    for (final long value : values) {
      assertArrayEquals(writeBoxed(Long.valueOf(value)), writeLong(value),
          "primitive long wire differs for " + value);
    }
  }

  @Test
  void primitiveWriterReuseDoesNotLeakPriorValueOrLength() {
    final byte[] storage = new byte[CAPACITY];
    final MemorySegment segment = MemorySegment.ofArray(storage);
    final int[] offsets = new int[NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT];

    final int firstLength = ObjectNamedNumberNode.writeNewIntRecord(segment, 0L, offsets,
        NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, HASH, 7);
    final byte[] firstWire = Arrays.copyOf(storage, firstLength);

    Arrays.fill(storage, (byte) 0xA5);
    ObjectNamedNumberNode.writeNewLongRecord(segment, 0L, offsets,
        NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, HASH, Long.MIN_VALUE);
    Arrays.fill(storage, (byte) 0xCC);

    final int secondLength = ObjectNamedNumberNode.writeNewIntRecord(segment, 0L, offsets,
        NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, HASH, 7);

    assertEquals(firstLength, secondLength);
    assertArrayEquals(firstWire, Arrays.copyOf(storage, secondLength));
  }

  private static byte[] writeBoxed(final Number value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = ObjectNamedNumberNode.writeNewRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT], NODE_KEY, PARENT_KEY,
        RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY, PREVIOUS_REVISION,
        LAST_MODIFIED_REVISION, HASH, value);
    return Arrays.copyOf(storage, length);
  }

  private static byte[] writeInt(final int value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = ObjectNamedNumberNode.writeNewIntRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT], NODE_KEY, PARENT_KEY,
        RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY, PREVIOUS_REVISION,
        LAST_MODIFIED_REVISION, HASH, value);
    return Arrays.copyOf(storage, length);
  }

  private static byte[] writeLong(final long value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = ObjectNamedNumberNode.writeNewLongRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT], NODE_KEY, PARENT_KEY,
        RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY, PREVIOUS_REVISION,
        LAST_MODIFIED_REVISION, HASH, value);
    return Arrays.copyOf(storage, length);
  }
}
