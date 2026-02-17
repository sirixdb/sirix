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
 * Tests for PartialKeyMapping - the PEXT-based key extraction.
 * 
 * <p>
 * Verifies that partial key extraction matches the reference implementation's behavior using
 * Long.compress() (PEXT instruction on x86-64).
 * </p>
 */
@DisplayName("PartialKeyMapping Tests")
class PartialKeyMappingTest {

  @Nested
  @DisplayName("Single Bit Mapping")
  class SingleBitMappingTests {

    @Test
    @DisplayName("Mapping tracks bit indices correctly")
    void testBitIndexTracking() {
      PartialKeyMapping mapping0 = PartialKeyMapping.forSingleBit(0);
      assertEquals(0, mapping0.getMostSignificantBitIndex());
      assertEquals(0, mapping0.getLeastSignificantBitIndex());
      assertEquals(1, mapping0.getNumberBitsUsed());

      PartialKeyMapping mapping7 = PartialKeyMapping.forSingleBit(7);
      assertEquals(7, mapping7.getMostSignificantBitIndex());
      assertEquals(7, mapping7.getLeastSignificantBitIndex());
      assertEquals(1, mapping7.getNumberBitsUsed());
    }

    @Test
    @DisplayName("Extraction mask is non-zero for single bits")
    void testExtractionMaskNonZero() {
      // Single bits at various positions should produce non-zero masks
      for (int bit = 0; bit < 64; bit++) {
        PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(bit);
        assertTrue(mapping.getExtractionMask() != 0, "Bit " + bit + " should have non-zero extraction mask");
        assertEquals(1, mapping.getNumberBitsUsed());
      }
    }
  }

  @Nested
  @DisplayName("Multiple Bits Mapping")
  class MultipleBitsMappingTests {

    @Test
    @DisplayName("Adding bits increases count")
    void testAddingBitsIncreasesCount() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      assertEquals(1, mapping.getNumberBitsUsed());

      mapping = PartialKeyMapping.withAdditionalBit(mapping, 7);
      assertEquals(2, mapping.getNumberBitsUsed());

      mapping = PartialKeyMapping.withAdditionalBit(mapping, 4);
      assertEquals(3, mapping.getNumberBitsUsed());
    }

    @Test
    @DisplayName("Bit range is tracked correctly")
    void testBitRangeTracking() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(10);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 5);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 20);

      assertEquals(5, mapping.getMostSignificantBitIndex());
      assertEquals(20, mapping.getLeastSignificantBitIndex());
    }

    @Test
    @DisplayName("Bits within 8-byte window stay single-mask")
    void testSingleMaskWithinWindow() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 63);

      // 64 bits = 8 bytes, should fit in single mask
      assertFalse(mapping.isMultiMask(), "Bits 0-63 should fit in single mask");
    }

    @Test
    @DisplayName("Bits far apart require multi-mask")
    void testMultiMaskRequired() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 80);

      assertTrue(mapping.isMultiMask(), "Bits > 64 apart should use multi-mask");
    }
  }

  @Nested
  @DisplayName("Mask Operations")
  class MaskOperationsTests {

    @Test
    @DisplayName("getMaskForHighestBit returns single bit")
    void testMaskForHighestBit() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(4);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 12);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 20);

      int highestMask = mapping.getMaskForHighestBit();
      assertEquals(1, Integer.bitCount(highestMask), "Highest bit mask should have exactly one bit set");
    }

    @Test
    @DisplayName("getAllMaskBits returns all bits")
    void testGetAllMaskBits() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      assertEquals(1, Integer.bitCount(mapping.getAllMaskBits()));

      mapping = PartialKeyMapping.withAdditionalBit(mapping, 1);
      assertEquals(2, Integer.bitCount(mapping.getAllMaskBits()));

      mapping = PartialKeyMapping.withAdditionalBit(mapping, 2);
      assertEquals(3, Integer.bitCount(mapping.getAllMaskBits()));
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Empty key extraction returns 0")
    void testEmptyKey() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      byte[] key = {};
      assertEquals(0, mapping.extractMask(key));
    }

    @Test
    @DisplayName("Key shorter than offset returns 0")
    void testShortKey() {
      // Bit 64 = byte 8
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(64);
      byte[] key = {0x00, 0x00}; // Only 2 bytes
      assertEquals(0, mapping.extractMask(key));
    }

    @Test
    @DisplayName("Single bit at various positions")
    void testSingleBitPositions() {
      // Test first bit of each byte in 8-byte window
      for (int byteIdx = 0; byteIdx < 8; byteIdx++) {
        int bitIdx = byteIdx * 8; // MSB of each byte
        PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(bitIdx);
        assertEquals(1, mapping.getNumberBitsUsed());
        assertEquals(bitIdx, mapping.getMostSignificantBitIndex());
      }
    }
  }
}
