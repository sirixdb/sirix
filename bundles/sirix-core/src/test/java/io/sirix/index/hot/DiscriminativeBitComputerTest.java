/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DiscriminativeBitComputer}.
 * 
 * <p>Verifies correctness of the discriminative bit computation algorithm
 * as specified in Binna's thesis and the reference implementation.</p>
 */
@DisplayName("DiscriminativeBitComputer Tests")
class DiscriminativeBitComputerTest {

  @Nested
  @DisplayName("computeDifferingBit tests")
  class ComputeDifferingBitTests {

    @Test
    @DisplayName("CC-1: Empty keys return -1")
    void testEmptyKeys() {
      assertEquals(-1, DiscriminativeBitComputer.computeDifferingBit(new byte[0], new byte[0]));
    }

    @Test
    @DisplayName("CC-1: One empty key returns 0")
    void testOneEmptyKey() {
      assertEquals(0, DiscriminativeBitComputer.computeDifferingBit(new byte[0], new byte[]{0x01}));
      assertEquals(0, DiscriminativeBitComputer.computeDifferingBit(new byte[]{0x01}, new byte[0]));
    }

    @Test
    @DisplayName("CC-2: 0x00 vs 0xFF - MSB differs first (bit 0)")
    void testMaximalBitDifference() {
      // Reference: 0x00 XOR 0xFF = 0xFF, clz(0xFF) = 24, 24-24 = 0
      int result = DiscriminativeBitComputer.computeDifferingBit(
          new byte[]{0x00},
          new byte[]{(byte) 0xFF}
      );
      assertEquals(0, result, "First differing bit should be bit 0 (MSB)");
    }

    @Test
    @DisplayName("CC-2: 0x80 vs 0x00 - MSB differs (bit 0)")
    void testMSBDifference() {
      // 0x80 XOR 0x00 = 0x80, clz(0x80) = 24, 24-24 = 0
      int result = DiscriminativeBitComputer.computeDifferingBit(
          new byte[]{(byte) 0x80},
          new byte[]{0x00}
      );
      assertEquals(0, result);
    }

    @Test
    @DisplayName("0x40 vs 0x00 - second bit differs (bit 1)")
    void testSecondBitDifference() {
      // 0x40 XOR 0x00 = 0x40, clz(0x40) = 25, 25-24 = 1
      int result = DiscriminativeBitComputer.computeDifferingBit(
          new byte[]{0x40},
          new byte[]{0x00}
      );
      assertEquals(1, result);
    }

    @Test
    @DisplayName("0x01 vs 0x00 - LSB differs (bit 7)")
    void testLSBDifference() {
      // 0x01 XOR 0x00 = 0x01, clz(0x01) = 31, 31-24 = 7
      int result = DiscriminativeBitComputer.computeDifferingBit(
          new byte[]{0x01},
          new byte[]{0x00}
      );
      assertEquals(7, result);
    }

    @Test
    @DisplayName("CC-3: Keys differing only in length")
    void testKeysDifferingInLength() {
      byte[] key1 = "abc".getBytes();
      byte[] key2 = "abcd".getBytes();
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(24, result, "Should differ at first bit of 4th byte");
    }

    @Test
    @DisplayName("Identical keys return -1")
    void testIdenticalKeys() {
      byte[] key = {0x12, 0x34, 0x56};
      assertEquals(-1, DiscriminativeBitComputer.computeDifferingBit(key, key.clone()));
    }

    @Test
    @DisplayName("Keys differing in second byte")
    void testSecondByteDifference() {
      byte[] key1 = {0x12, 0x00};
      byte[] key2 = {0x12, (byte) 0x80};
      // First byte same, second byte differs at MSB
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(8, result, "Should differ at bit 8 (MSB of second byte)");
    }

    @Test
    @DisplayName("Each bit position correctly identified")
    void testBitPositions() {
      // Test each bit position from 0 (MSB) to 7 (LSB)
      int[][] testCases = {
          {0x00, 0x80, 0},   // MSB differs
          {0x00, 0x40, 1},   // Bit 1 differs
          {0x00, 0x20, 2},   // Bit 2 differs
          {0x00, 0x10, 3},   // Bit 3 differs
          {0x00, 0x08, 4},   // Bit 4 differs
          {0x00, 0x04, 5},   // Bit 5 differs
          {0x00, 0x02, 6},   // Bit 6 differs
          {0x00, 0x01, 7}    // Bit 7 (LSB) differs
      };
      
      for (int[] testCase : testCases) {
        byte b1 = (byte) testCase[0];
        byte b2 = (byte) testCase[1];
        int expectedBitPos = testCase[2];
        int result = DiscriminativeBitComputer.computeDifferingBit(
            new byte[]{b1},
            new byte[]{b2}
        );
        assertEquals(expectedBitPos, result, 
            String.format("0x%02X vs 0x%02X should differ at bit %d", testCase[0], testCase[1], expectedBitPos));
      }
    }

    @Test
    @DisplayName("Long keys with difference in 8th byte")
    void testLongKeysDifferenceIn8thByte() {
      byte[] key1 = {0, 0, 0, 0, 0, 0, 0, 0x00};
      byte[] key2 = {0, 0, 0, 0, 0, 0, 0, 0x01};
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      // 7 bytes * 8 bits + 7 (LSB of 8th byte) = 63
      assertEquals(63, result);
    }
  }

  @Nested
  @DisplayName("isBitSet tests")
  class IsBitSetTests {

    @Test
    @DisplayName("MSB (bit 0) set in 0x80")
    void testMSBSet() {
      byte[] key = {(byte) 0x80};
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 0));
    }

    @Test
    @DisplayName("MSB (bit 0) not set in 0x7F")
    void testMSBNotSet() {
      byte[] key = {0x7F};
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 0));
    }

    @Test
    @DisplayName("LSB (bit 7) set in 0x01")
    void testLSBSet() {
      byte[] key = {0x01};
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 7));
    }

    @Test
    @DisplayName("LSB (bit 7) not set in 0xFE")
    void testLSBNotSet() {
      byte[] key = {(byte) 0xFE};
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 7));
    }

    @Test
    @DisplayName("Bit in second byte")
    void testBitInSecondByte() {
      byte[] key = {0x00, (byte) 0x80};
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 0));  // First byte, bit 0
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 8));   // Second byte, bit 0
    }

    @Test
    @DisplayName("Bit beyond key length returns false")
    void testBitBeyondKeyLength() {
      byte[] key = {0x00};
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 8));
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 100));
    }

    @Test
    @DisplayName("Negative bit index returns false")
    void testNegativeBitIndex() {
      byte[] key = {(byte) 0xFF};
      assertFalse(DiscriminativeBitComputer.isBitSet(key, -1));
    }
  }

  @Nested
  @DisplayName("computeDiscriminativeMask tests")
  class ComputeDiscriminativeMaskTests {

    @Test
    @DisplayName("Single key returns empty mask")
    void testSingleKey() {
      byte[][] keys = {new byte[]{0x12}};
      long mask = DiscriminativeBitComputer.computeDiscriminativeMask(keys, 0, 8);
      assertEquals(0L, mask);
    }

    @Test
    @DisplayName("Two keys differing in MSB")
    void testTwoKeysDifferingInMSB() {
      byte[][] keys = {
          new byte[]{0x00},
          new byte[]{(byte) 0x80}
      };
      long mask = DiscriminativeBitComputer.computeDiscriminativeMask(keys, 0, 8);
      // Bit 0 (MSB) should be set in the mask
      // In 64-bit mask, byte 0 goes to bits 56-63
      assertTrue((mask & (1L << 63)) != 0, "Bit 63 (MSB of first byte) should be set");
    }

    @Test
    @DisplayName("Empty keys array returns 0")
    void testEmptyKeysArray() {
      byte[][] keys = {};
      long mask = DiscriminativeBitComputer.computeDiscriminativeMask(keys, 0, 8);
      assertEquals(0L, mask);
    }
  }

  @Nested
  @DisplayName("extractPartialKey tests")
  class ExtractPartialKeyTests {

    @Test
    @DisplayName("Extract single bit")
    void testExtractSingleBit() {
      byte[] key = {(byte) 0x80};  // 10000000
      long mask = 1L << 63;  // MSB only
      int partialKey = DiscriminativeBitComputer.extractPartialKey(key, mask, 0);
      assertEquals(1, partialKey);
    }

    @Test
    @DisplayName("Extract from zero key")
    void testExtractFromZeroKey() {
      byte[] key = {0x00};
      long mask = 1L << 63;
      int partialKey = DiscriminativeBitComputer.extractPartialKey(key, mask, 0);
      assertEquals(0, partialKey);
    }
  }

  @Nested
  @DisplayName("Utility method tests")
  class UtilityMethodTests {

    @Test
    @DisplayName("getByteIndex")
    void testGetByteIndex() {
      assertEquals(0, DiscriminativeBitComputer.getByteIndex(0));
      assertEquals(0, DiscriminativeBitComputer.getByteIndex(7));
      assertEquals(1, DiscriminativeBitComputer.getByteIndex(8));
      assertEquals(1, DiscriminativeBitComputer.getByteIndex(15));
      assertEquals(2, DiscriminativeBitComputer.getByteIndex(16));
    }

    @Test
    @DisplayName("getBitPositionInByte")
    void testGetBitPositionInByte() {
      assertEquals(0, DiscriminativeBitComputer.getBitPositionInByte(0));
      assertEquals(7, DiscriminativeBitComputer.getBitPositionInByte(7));
      assertEquals(0, DiscriminativeBitComputer.getBitPositionInByte(8));
      assertEquals(1, DiscriminativeBitComputer.getBitPositionInByte(9));
    }

    @Test
    @DisplayName("countDiscriminativeBits")
    void testCountDiscriminativeBits() {
      assertEquals(0, DiscriminativeBitComputer.countDiscriminativeBits(0L));
      assertEquals(1, DiscriminativeBitComputer.countDiscriminativeBits(1L));
      assertEquals(2, DiscriminativeBitComputer.countDiscriminativeBits(3L));
      assertEquals(64, DiscriminativeBitComputer.countDiscriminativeBits(-1L)); // All bits set
    }
  }
}

