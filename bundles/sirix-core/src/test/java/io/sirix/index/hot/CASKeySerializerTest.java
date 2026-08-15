/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Time;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    @DisplayName("Empty string round-trips and sorts below every non-empty value")
    void testEmptyStringIsAValue() {
      // The empty string is a legitimate indexed value, so its key is legitimately just the 10-byte
      // header. Rejecting a zero-length value region instead made indexing a `""` throw out of the
      // WRITER, and made a `>= ""` range bound throw out of the reader before it returned a cursor.
      final CASValue empty = new CASValue(new Str(""), Type.STR, 1);
      final byte[] buffer = new byte[256];
      final int len = serializer.serialize(empty, buffer, 0);
      assertEquals(10, len, "an empty value region is the 10-byte header alone");
      assertEquals(empty, serializer.deserialize(buffer, 0, len));

      // Order is preserved: shorter-is-less puts "" below every non-empty string of the same type.
      final byte[] other = new byte[256];
      final int otherLen = serializer.serialize(new CASValue(new Str("a"), Type.STR, 1), other, 0);
      assertTrue(compareBytes(buffer, len, other, otherLen) < 0, "empty string sorts first");
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
  @DisplayName("Large Integer Serialization (lossless 64-bit)")
  class LargeIntegerSerializationTests {

    @Test
    @DisplayName("Integers above 2^53 round-trip without precision loss")
    void testLargeIntegerRoundtrip() {
      long[] values = {0L, 1L, -1L, 100L, -100L, (1L << 53), // 9007199254740992 - largest integer exactly representable
                                                             // as double
          (1L << 53) + 1, // 9007199254740993 - NOT representable as double
          (1L << 62), -(1L << 62), Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1};
      for (long value : values) {
        CASValue original = new CASValue(new Int64(value), Type.LON, 7);
        byte[] buffer = new byte[256];

        int length = serializer.serialize(original, buffer, 0);
        CASValue result = serializer.deserialize(buffer, 0, length);

        assertEquals(Long.toString(value), result.getAtomicValue().stringValue(),
            "long " + value + " must round-trip exactly");
      }
    }

    @Test
    @DisplayName("Distinct integers near 2^53 encode to distinct keys (no collision)")
    void testLargeIntegerNoCollision() {
      // Both values collapse to the same double (2^53), so the old double-based
      // encoding produced identical key bytes - a silent CAS index collision.
      long a = (1L << 53); // 9007199254740992
      long b = (1L << 53) + 1; // 9007199254740993

      byte[] bufA = new byte[256];
      byte[] bufB = new byte[256];

      int lenA = serializer.serialize(new CASValue(new Int64(a), Type.LON, 7), bufA, 0);
      int lenB = serializer.serialize(new CASValue(new Int64(b), Type.LON, 7), bufB, 0);

      assertTrue(compareBytes(bufA, lenA, bufB, lenB) != 0,
          "consecutive integers above 2^53 must encode to distinct keys");
    }

    @Test
    @DisplayName("Large integer order is preserved by byte comparison")
    void testLargeIntegerOrder() {
      long[] ascending = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -(1L << 53), -1L, 0L, 1L, (1L << 53), (1L << 53) + 1,
          Long.MAX_VALUE - 1, Long.MAX_VALUE};
      for (int i = 1; i < ascending.length; i++) {
        byte[] lo = new byte[256];
        byte[] hi = new byte[256];
        int lenLo = serializer.serialize(new CASValue(new Int64(ascending[i - 1]), Type.LON, 1), lo, 0);
        int lenHi = serializer.serialize(new CASValue(new Int64(ascending[i]), Type.LON, 1), hi, 0);
        assertTrue(compareBytes(lo, lenLo, hi, lenHi) < 0, ascending[i - 1] + " must sort before " + ascending[i]);
      }
    }

    @Test
    @DisplayName("xs:integer typed values round-trip exactly (CAS index value type)")
    void testXsIntegerTypeRoundtrip() {
      // CAS indexes type integer content as xs:integer (Type.INR); verify that path.
      long value = (1L << 53) + 12345;
      CASValue original = new CASValue(new Int64(value), Type.INR, 99);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      CASValue result = serializer.deserialize(buffer, 0, length);

      assertEquals(Long.toString(value), result.getAtomicValue().stringValue());
      assertEquals(99, result.getPathNodeKey());
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
   * The instant family — {@code xs:dateTime}, {@code xs:date}, {@code xs:time}.
   *
   * <p>
   * These exist because a binary "canonicalize to UTC then write the calendar components" key was
   * briefly used for them and was WRONG in a way no existing test could see: the suite only ever
   * built {@code Type.DATI} indexes with same-instant spellings, which is the one case that encoding
   * got right. Each test below FAILS against that encoding.
   */
  @Nested
  @DisplayName("Instant family")
  class InstantTests {

    /**
     * Two values that {@code compareTo} separates must never share a key: a CAS key IS the index
     * entry's identity, so a collision merges two values' posting lists and corrupts equality lookups
     * and deletes, not merely ranges.
     */
    @Test
    void distinctInstantsNeverShareAKey() {
      // Both pairs are 19h / 5h apart; the old encoder collapsed each onto ONE key by discarding
      // the time-of-day an xs:date cannot carry.
      assertDistinct(new Date("2020-01-01+02:00"), new Date("2019-12-31Z"), Type.DATE);
      assertDistinct(new Date("2020-01-01-05:00"), new Date("2020-01-01Z"), Type.DATE);
      // A full day apart: the old encoder dropped the ±1-day rollover an xs:time cannot carry.
      assertDistinct(new Time("01:00:00+02:00"), new Time("23:00:00Z"), Type.TIME);
      assertDistinct(new DateTime("2020-01-01T12:00:00Z"), new DateTime("2020-01-01T12:00:01Z"), Type.DATI);
    }

    /**
     * The content type has to round-trip, or {@code CASFilterRange#inRange} compares a {@code Str} and
     * orders instants as text instead of chronologically.
     */
    @Test
    void instantsRoundTripAsTypedAtomics() {
      assertRoundTrip(new DateTime("2020-06-01T10:00:00Z"), Type.DATI);
      assertRoundTrip(new Date("2020-06-01Z"), Type.DATE);
      assertRoundTrip(new Time("10:00:00Z"), Type.TIME);
      assertRoundTrip(new DateTime("2020-06-01T12:00:00+02:00"), Type.DATI);
    }

    /**
     * The gate that keeps instant ranges off the byte-bounded cursor. Their stored form is lexical, and
     * text order is not chronological order, so a byte-decided range would silently drop in-range
     * records — the original defect. Equally important is the other direction: the families whose
     * encoders DO preserve order must keep the fast path.
     */
    @Test
    void onlyFamiliesWithOrderPreservingEncodersTakeTheByteBoundedPath() {
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DATI));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DATE));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.TIME));

      // xs:duration is only PARTIALLY ordered (a month is not a fixed number of days), so no byte
      // encoding can order it and it must never reach a byte-bounded cursor.
      assertFalse(CASKeySerializer.isByteOrderPreserving(Type.DUR));

      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.STR));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.INR));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DBL));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.BOOL));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DEC));
    }

    /** Two spellings of the SAME instant must share a key, or {@code eq} misses across timezones. */
    @Test
    void sameInstantWrittenTwoWaysSharesAKey() {
      assertSameKey(new DateTime("2020-06-15T12:00:00Z"), new DateTime("2020-06-15T14:00:00+02:00"), Type.DATI);
      assertSameKey(new Time("12:00:00Z"), new Time("14:00:00+02:00"), Type.TIME);
      // No timezone means the implicit one (UTC), so this is the same instant as its Z spelling.
      assertSameKey(new DateTime("2020-06-15T12:00:00"), new DateTime("2020-06-15T12:00:00Z"), Type.DATI);
    }

    private void assertSameKey(final Atomic a, final Atomic b, final Type type) {
      assertArrayEquals(keyOf(a, type), keyOf(b, type),
          a + " and " + b + " denote the same instant, so they must serialize to the same CAS key");
    }

    private void assertDistinct(final Atomic a, final Atomic b, final Type type) {
      assertNotEquals(0, a.compareTo(b), "fixture is wrong: " + a + " and " + b + " are the same value");
      assertFalse(Arrays.equals(keyOf(a, type), keyOf(b, type)), "distinct " + type + " values " + a + " and " + b
          + " serialize to the same CAS key, so they " + "would share one index entry and merge their posting lists");
    }

    /**
     * The decoded value is the stored value's CANONICAL spelling, so it need not be textually equal —
     * {@code 12:00+02:00} comes back as {@code 10:00Z}. What must hold is that it denotes the same
     * instant, which re-encoding proves exactly: identical keys iff identical instants. (Comparing with
     * {@code compareTo} would not prove it — brackit orders two timezoned spellings of one instant
     * apart.)
     */
    private void assertRoundTrip(final Atomic value, final Type type) {
      final byte[] key = keyOf(value, type);
      final CASValue back = CASKeySerializer.INSTANCE.deserialize(key, 0, key.length);
      assertEquals(type, back.getType(), "content type must round-trip for " + value);
      assertArrayEquals(key, keyOf(back.getAtomicValue(), type),
          "decoded " + back.getAtomicValue() + " does not denote the same instant as the stored " + value);
    }

    private byte[] keyOf(final Atomic value, final Type type) {
      final byte[] buf = new byte[512];
      final int len = CASKeySerializer.INSTANCE.serialize(new CASValue(value, type, 7L), buf, 0);
      return Arrays.copyOf(buf, len);
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

