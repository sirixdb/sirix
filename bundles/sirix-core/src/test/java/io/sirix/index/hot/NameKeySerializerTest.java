/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for NameKeySerializer - verifies QNm serialization.
 */
@DisplayName("NameKeySerializer Tests")
class NameKeySerializerTest {

  private final NameKeySerializer serializer = NameKeySerializer.INSTANCE;

  @Nested
  @DisplayName("Basic Serialization")
  class BasicSerializationTests {

    @Test
    @DisplayName("Serialize and deserialize simple QNm")
    void testSimpleQnmRoundtrip() {
      QNm original = new QNm("localName");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals("localName", result.getLocalName());
    }

    @Test
    @DisplayName("Serialize and deserialize QNm with prefix")
    void testPrefixedQnmRoundtrip() {
      QNm original = new QNm(null, "ns", "element");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals("ns", result.getPrefix());
      assertEquals("element", result.getLocalName());
    }

    @Test
    @DisplayName("Serialize QNm without prefix")
    void testNoPrefixQnm() {
      QNm original = new QNm(null, "", "name");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals("", result.getPrefix());
      assertEquals("name", result.getLocalName());
    }
  }

  @Nested
  @DisplayName("Order Preservation")
  class OrderPreservationTests {

    @Test
    @DisplayName("QNm ordering by prefix first")
    void testOrderByPrefix() {
      QNm a = new QNm(null, "aaa", "name");
      QNm b = new QNm(null, "bbb", "name");

      byte[] bufA = new byte[256];
      byte[] bufB = new byte[256];

      int lenA = serializer.serialize(a, bufA, 0);
      int lenB = serializer.serialize(b, bufB, 0);

      assertTrue(compareBytes(bufA, lenA, bufB, lenB) < 0, "aaa:name < bbb:name");
    }

    @Test
    @DisplayName("QNm ordering by local name")
    void testOrderByLocalName() {
      QNm a = new QNm(null, "ns", "alpha");
      QNm b = new QNm(null, "ns", "beta");

      byte[] bufA = new byte[256];
      byte[] bufB = new byte[256];

      int lenA = serializer.serialize(a, bufA, 0);
      int lenB = serializer.serialize(b, bufB, 0);

      assertTrue(compareBytes(bufA, lenA, bufB, lenB) < 0, "ns:alpha < ns:beta");
    }

    @Test
    @DisplayName("Empty prefix sorts before non-empty")
    void testEmptyPrefixOrder() {
      QNm noPrefix = new QNm(null, "", "name");
      QNm withPrefix = new QNm(null, "a", "name");

      byte[] bufNo = new byte[256];
      byte[] bufWith = new byte[256];

      int lenNo = serializer.serialize(noPrefix, bufNo, 0);
      int lenWith = serializer.serialize(withPrefix, bufWith, 0);

      assertTrue(compareBytes(bufNo, lenNo, bufWith, lenWith) < 0, "empty prefix < 'a' prefix");
    }
  }

  @Nested
  @DisplayName("HOT Discriminative Bit Properties")
  class HOTProperties {

    @Test
    @DisplayName("JSON keys serialize to raw local name bytes with no overhead byte")
    void testJsonKeysHaveNoOverheadByte() {
      QNm key = new QNm("type");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(key, buffer, 0);

      // First byte should be 't' (0x74), not 0x00 separator
      assertEquals('t', buffer[0], "First byte should be the start of local name, not a separator");
      assertEquals(4, length, "Length should match 'type' UTF-8 length");
    }

    @Test
    @DisplayName("Different JSON keys differ at byte 0 for discriminative bit computer")
    void testJsonKeysDifferAtByte0() {
      QNm keyA = new QNm("alpha");
      QNm keyB = new QNm("beta");

      byte[] bufA = new byte[256];
      byte[] bufB = new byte[256];

      serializer.serialize(keyA, bufA, 0);
      serializer.serialize(keyB, bufB, 0);

      // First bytes should differ: 'a' (0x61) vs 'b' (0x62)
      assertTrue(bufA[0] != bufB[0], "JSON keys should differ at byte 0 (not have a shared 0x00 prefix)");
      int discBit = DiscriminativeBitComputer.computeDifferingBit(
          Arrays.copyOf(bufA, 5), Arrays.copyOf(bufB, 4));
      assertTrue(discBit < 8, "Discriminative bit should be in byte 0, not pushed past a separator byte");
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Unicode local name")
    void testUnicodeLocalName() {
      QNm original = new QNm(null, "", "日本語");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals("日本語", result.getLocalName());
    }

    @Test
    @DisplayName("Unicode prefix and local name")
    void testUnicodePrefixAndLocal() {
      QNm original = new QNm(null, "前缀", "名称");
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals("前缀", result.getPrefix());
      assertEquals("名称", result.getLocalName());
    }

    @Test
    @DisplayName("Null key throws exception")
    void testNullKey() {
      byte[] buffer = new byte[256];
      assertThrows(NullPointerException.class, () -> serializer.serialize(null, buffer, 0));
    }

    @Test
    @DisplayName("Zero-length deserialization throws")
    void testZeroLengthDeserialization() {
      byte[] buffer = new byte[] {'a', 'b', 'c'};
      assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(buffer, 0, 0));
    }

    @Test
    @DisplayName("Raw bytes without sentinel deserialize as local name only")
    void testRawBytesDeserializeAsLocalNameOnly() {
      byte[] buffer = new byte[] {'a', 'b', 'c'};
      QNm result = serializer.deserialize(buffer, 0, buffer.length);
      assertEquals("abc", result.getLocalName());
      assertEquals("", result.getPrefix());
    }

    @Test
    @DisplayName("Truncated prefixed format throws")
    void testTruncatedPrefixedFormat() {
      byte[] buffer = new byte[] {(byte) 0xFF, 0x05}; // Sentinel + prefixLen=5 but no data
      assertThrows(Exception.class, () -> serializer.deserialize(buffer, 0, buffer.length));
    }

    @Test
    @DisplayName("Long prefix and local name")
    void testLongNames() {
      String longPrefix = "a".repeat(50);
      String longLocal = "b".repeat(100);
      QNm original = new QNm(null, longPrefix, longLocal);
      byte[] buffer = new byte[256];

      int length = serializer.serialize(original, buffer, 0);
      QNm result = serializer.deserialize(buffer, 0, length);

      assertEquals(longPrefix, result.getPrefix());
      assertEquals(longLocal, result.getLocalName());
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

