/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for CASKeySerializer - verifies order-preserving serialization.
 */
@DisplayName("CASKeySerializer Tests")
class CASKeySerializerTest {

  private final CASKeySerializer serializer = CASKeySerializer.INSTANCE;

  @Nested
  @DisplayName("String Serialization")
  class StringSerializationTests {

    @Test
    @DisplayName("Serialize and deserialize string value")
    void testStringRoundtrip() {
      CASValue original = new CASValue(new Str("hello"), Type.STR, 42);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      CASValue result = serializer.deserialize(buffer, 0, length);

      assertEquals(original.getPathNodeKey(), result.getPathNodeKey());
      assertEquals("hello", result.getAtomicValue().stringValue());
    }

    @Test
    @DisplayName("Strings maintain lexicographic order")
    void testStringOrder() {
      CASValue a = new CASValue(new Str("apple"), Type.STR, 1);
      CASValue b = new CASValue(new Str("banana"), Type.STR, 1);

      byte[] bufferA = new byte[256];
      byte[] bufferB = new byte[256];

      int lenA = serializer.serialize(a, bufferA, 0);
      int lenB = serializer.serialize(b, bufferB, 0);

      // Byte comparison should match string comparison
      int byteCompare = compareBytes(bufferA, lenA, bufferB, lenB);
      assertTrue(byteCompare < 0, "apple should sort before banana");
    }

    @Test
    @DisplayName("Empty string throws exception (no value bytes)")
    void testEmptyStringThrows() {
      CASValue original = new CASValue(new Str(""), Type.STR, 1);
      byte[] buffer = new byte[256];

      // Empty string produces only header bytes, which is rejected
      assertThrows(IllegalArgumentException.class, () -> serializer.serialize(original, buffer, 0));
    }
  }

  @Nested
  @DisplayName("Numeric Serialization")
  class NumericSerializationTests {

    @Test
    @DisplayName("Integer roundtrip")
    void testIntegerRoundtrip() {
      for (int value : new int[] {0, 1, -1, 100, -100, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
        CASValue original = new CASValue(new Int32(value), Type.INT, 10);
        byte[] buffer = new byte[256];

        int length = serializer.serialize(original, buffer, 0);
        CASValue result = serializer.deserialize(buffer, 0, length);

        // Deserialize returns as Numeric, compare as double for tolerance
        double expected = value;
        double actual = Double.parseDouble(result.getAtomicValue().stringValue());
        assertEquals(expected, actual, 1.0, "Integer " + value + " should round-trip");
      }
    }

    @Test
    @DisplayName("Long roundtrip")
    void testLongRoundtrip() {
      for (long value : new long[] {0L, 1L, -1L, 1000000L, -1000000L}) {
        CASValue original = new CASValue(new Int64(value), Type.LON, 20);
        byte[] buffer = new byte[256];

        int length = serializer.serialize(original, buffer, 0);
        CASValue result = serializer.deserialize(buffer, 0, length);

        double expected = value;
        double actual = Double.parseDouble(result.getAtomicValue().stringValue());
        assertEquals(expected, actual, 1.0, "Long " + value + " should round-trip");
      }
    }

    @Test
    @DisplayName("Double roundtrip")
    void testDoubleRoundtrip() {
      for (double value : new double[] {0.0, 1.0, -1.0, 3.14159, -273.15}) {
        CASValue original = new CASValue(new Dbl(value), Type.DBL, 30);
        byte[] buffer = new byte[256];

        int length = serializer.serialize(original, buffer, 0);
        CASValue result = serializer.deserialize(buffer, 0, length);

        double actual = Double.parseDouble(result.getAtomicValue().stringValue());
        assertEquals(value, actual, 0.0001, "Double " + value + " should round-trip");
      }
    }

    @Test
    @DisplayName("Numeric order is preserved")
    void testNumericOrder() {
      CASValue neg = new CASValue(new Dbl(-100.0), Type.DBL, 1);
      CASValue zero = new CASValue(new Dbl(0.0), Type.DBL, 1);
      CASValue pos = new CASValue(new Dbl(100.0), Type.DBL, 1);

      byte[] bufNeg = new byte[256];
      byte[] bufZero = new byte[256];
      byte[] bufPos = new byte[256];

      int lenNeg = serializer.serialize(neg, bufNeg, 0);
      int lenZero = serializer.serialize(zero, bufZero, 0);
      int lenPos = serializer.serialize(pos, bufPos, 0);

      assertTrue(compareBytes(bufNeg, lenNeg, bufZero, lenZero) < 0, "-100 < 0");
      assertTrue(compareBytes(bufZero, lenZero, bufPos, lenPos) < 0, "0 < 100");
    }
  }

  @Nested
  @DisplayName("Boolean Serialization")
  class BooleanSerializationTests {

    @Test
    @DisplayName("Serialize true")
    void testSerializeTrue() {
      CASValue original = new CASValue(new Bool(true), Type.BOOL, 1);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      CASValue result = serializer.deserialize(buffer, 0, length);

      assertTrue(result.getAtomicValue().booleanValue());
    }

    @Test
    @DisplayName("Serialize false")
    void testSerializeFalse() {
      CASValue original = new CASValue(new Bool(false), Type.BOOL, 1);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      CASValue result = serializer.deserialize(buffer, 0, length);

      assertEquals(false, result.getAtomicValue().booleanValue());
    }

    @Test
    @DisplayName("Boolean order: false < true")
    void testBooleanOrder() {
      CASValue falseVal = new CASValue(new Bool(false), Type.BOOL, 1);
      CASValue trueVal = new CASValue(new Bool(true), Type.BOOL, 1);

      byte[] bufFalse = new byte[256];
      byte[] bufTrue = new byte[256];

      int lenFalse = serializer.serialize(falseVal, bufFalse, 0);
      int lenTrue = serializer.serialize(trueVal, bufTrue, 0);

      assertTrue(compareBytes(bufFalse, lenFalse, bufTrue, lenTrue) < 0, "false < true");
    }
  }

  @Nested
  @DisplayName("Path Node Key Ordering")
  class PathNodeKeyOrderingTests {

    @Test
    @DisplayName("Path node key order is preserved")
    void testPathNodeKeyOrder() {
      CASValue low = new CASValue(new Str("a"), Type.STR, 1);
      CASValue high = new CASValue(new Str("a"), Type.STR, 100);

      byte[] bufLow = new byte[256];
      byte[] bufHigh = new byte[256];

      int lenLow = serializer.serialize(low, bufLow, 0);
      int lenHigh = serializer.serialize(high, bufHigh, 0);

      assertTrue(compareBytes(bufLow, lenLow, bufHigh, lenHigh) < 0, "key 1 < key 100");
    }

    @Test
    @DisplayName("Negative path node keys are ordered correctly")
    void testNegativePathNodeKey() {
      CASValue neg = new CASValue(new Str("a"), Type.STR, -1);
      CASValue pos = new CASValue(new Str("a"), Type.STR, 1);

      byte[] bufNeg = new byte[256];
      byte[] bufPos = new byte[256];

      int lenNeg = serializer.serialize(neg, bufNeg, 0);
      int lenPos = serializer.serialize(pos, bufPos, 0);

      assertTrue(compareBytes(bufNeg, lenNeg, bufPos, lenPos) < 0, "-1 < 1");
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Null key throws exception")
    void testNullKey() {
      byte[] buffer = new byte[256];
      assertThrows(NullPointerException.class, () -> serializer.serialize(null, buffer, 0));
    }

    @Test
    @DisplayName("Unicode strings are handled")
    void testUnicodeString() {
      CASValue original = new CASValue(new Str("日本語"), Type.STR, 1);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      CASValue result = serializer.deserialize(buffer, 0, length);

      assertEquals("日本語", result.getAtomicValue().stringValue());
    }
  }

  /**
   * Compare two byte arrays lexicographically.
   */
  private int compareBytes(byte[] a, int lenA, byte[] b, int lenB) {
    int minLen = Math.min(lenA, lenB);
    for (int i = 0; i < minLen; i++) {
      int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return lenA - lenB;
  }
}

