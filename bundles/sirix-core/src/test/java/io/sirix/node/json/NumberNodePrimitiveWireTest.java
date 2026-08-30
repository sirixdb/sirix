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

/** Wire-compatibility coverage for the allocation-free plain integral-number writers. */
final class NumberNodePrimitiveWireTest {

  private static final int CAPACITY = 256;
  private static final long NODE_KEY = 71L;
  private static final long PARENT_KEY = 43L;
  private static final long RIGHT_SIBLING_KEY = 72L;
  private static final long LEFT_SIBLING_KEY = 69L;
  private static final int PREVIOUS_REVISION = -1;
  private static final int LAST_MODIFIED_REVISION = 17;

  @Test
  void primitiveIntWriterIsByteIdenticalToBoxedWriterAtBoundaries() {
    final int[] values = {Integer.MIN_VALUE, -65, -1, 0, 63, 64, Integer.MAX_VALUE};

    for (final int value : values) {
      assertArrayEquals(writeBoxed(Integer.valueOf(value)), writeInt(value), "primitive int wire differs for " + value);
    }
  }

  @Test
  void primitiveLongWriterIsByteIdenticalToBoxedWriterAtBoundaries() {
    final long[] values = {Long.MIN_VALUE, (long) Integer.MIN_VALUE - 1L, -65L, -1L, 0L, 63L, 64L,
        (long) Integer.MAX_VALUE + 1L, Long.MAX_VALUE};

    for (final long value : values) {
      assertArrayEquals(writeBoxed(Long.valueOf(value)), writeLong(value), "primitive long wire differs for " + value);
    }
  }

  @Test
  void primitiveWriterReuseDoesNotLeakPriorTypeValueOrLength() {
    final byte[] storage = new byte[CAPACITY];
    final MemorySegment segment = MemorySegment.ofArray(storage);
    final int[] offsets = new int[NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT];

    final int firstLength = NumberNode.writeNewIntRecord(segment, 0L, offsets, NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY,
        LEFT_SIBLING_KEY, PREVIOUS_REVISION, LAST_MODIFIED_REVISION, 7);
    final byte[] firstWire = Arrays.copyOf(storage, firstLength);

    Arrays.fill(storage, (byte) 0xA5);
    NumberNode.writeNewLongRecord(segment, 0L, offsets, NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, Long.MIN_VALUE);
    Arrays.fill(storage, (byte) 0xCC);

    final int secondLength = NumberNode.writeNewIntRecord(segment, 0L, offsets, NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY,
        LEFT_SIBLING_KEY, PREVIOUS_REVISION, LAST_MODIFIED_REVISION, 7);

    assertEquals(firstLength, secondLength);
    assertArrayEquals(firstWire, Arrays.copyOf(storage, secondLength));
  }

  private static byte[] writeBoxed(final Number value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = NumberNode.writeNewRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT], NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, value);
    return Arrays.copyOf(storage, length);
  }

  private static byte[] writeInt(final int value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = NumberNode.writeNewIntRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT], NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, value);
    return Arrays.copyOf(storage, length);
  }

  private static byte[] writeLong(final long value) {
    final byte[] storage = new byte[CAPACITY];
    final int length = NumberNode.writeNewLongRecord(MemorySegment.ofArray(storage), 0L,
        new int[NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT], NODE_KEY, PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY,
        PREVIOUS_REVISION, LAST_MODIFIED_REVISION, value);
    return Arrays.copyOf(storage, length);
  }
}
