/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PathKeySerializer}.
 * 
 * <p>
 * Verifies order-preserving serialization of long keys.
 * </p>
 */
class PathKeySerializerTest {

  private final PathKeySerializer serializer = PathKeySerializer.INSTANCE;

  @Test
  void testSerializeDeserializePositive() {
    byte[] buf = new byte[8];
    int len = serializer.serialize(42L, buf, 0);
    assertEquals(8, len);

    long result = serializer.deserialize(buf, 0, 8);
    assertEquals(42L, result);
  }

  @Test
  void testSerializeDeserializeZero() {
    byte[] buf = new byte[8];
    serializer.serialize(0L, buf, 0);

    long result = serializer.deserialize(buf, 0, 8);
    assertEquals(0L, result);
  }

  @Test
  void testSerializeDeserializeNegative() {
    byte[] buf = new byte[8];
    serializer.serialize(-1L, buf, 0);

    long result = serializer.deserialize(buf, 0, 8);
    assertEquals(-1L, result);
  }

  @Test
  void testSerializeDeserializeMinMax() {
    byte[] buf = new byte[8];

    // Long.MIN_VALUE
    serializer.serialize(Long.MIN_VALUE, buf, 0);
    assertEquals(Long.MIN_VALUE, serializer.deserialize(buf, 0, 8));

    // Long.MAX_VALUE
    serializer.serialize(Long.MAX_VALUE, buf, 0);
    assertEquals(Long.MAX_VALUE, serializer.deserialize(buf, 0, 8));
  }

  @Test
  void testOrderPreservation_NegativeBeforeZero() {
    byte[] neg = new byte[8];
    byte[] zero = new byte[8];

    serializer.serialize(-1L, neg, 0);
    serializer.serialize(0L, zero, 0);

    // Unsigned byte comparison should put negative before zero
    int cmp = serializer.compare(neg, 0, 8, zero, 0, 8);
    assertTrue(cmp < 0, "-1 should serialize before 0");
  }

  @Test
  void testOrderPreservation_ZeroBeforePositive() {
    byte[] zero = new byte[8];
    byte[] pos = new byte[8];

    serializer.serialize(0L, zero, 0);
    serializer.serialize(1L, pos, 0);

    int cmp = serializer.compare(zero, 0, 8, pos, 0, 8);
    assertTrue(cmp < 0, "0 should serialize before 1");
  }

  @Test
  void testOrderPreservation_SortedSequence() {
    long[] values = {Long.MIN_VALUE, -1000L, -1L, 0L, 1L, 1000L, Long.MAX_VALUE};
    byte[][] serialized = new byte[values.length][8];

    for (int i = 0; i < values.length; i++) {
      serializer.serialize(values[i], serialized[i], 0);
    }

    // Verify order is preserved
    for (int i = 0; i < values.length - 1; i++) {
      int cmp = serializer.compare(serialized[i], 0, 8, serialized[i + 1], 0, 8);
      assertTrue(cmp < 0, values[i] + " should serialize before " + values[i + 1]);
    }
  }

  @Test
  void testSerializeWithOffset() {
    byte[] buf = new byte[16];
    serializer.serialize(12345L, buf, 4);

    long result = serializer.deserialize(buf, 4, 8);
    assertEquals(12345L, result);
  }

  @Test
  void testRoundTripMultipleValues() {
    byte[] buf = new byte[8];
    long[] testValues =
        {0L, 1L, -1L, 100L, -100L, Long.MAX_VALUE, Long.MIN_VALUE, 0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L};

    for (long value : testValues) {
      serializer.serialize(value, buf, 0);
      long result = serializer.deserialize(buf, 0, 8);
      assertEquals(value, result, "Round-trip failed for " + value);
    }
  }
}

