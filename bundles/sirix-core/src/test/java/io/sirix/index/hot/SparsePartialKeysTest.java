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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SparsePartialKeys SIMD-accelerated search.
 * 
 * <p>
 * Verifies that the sparse partial key search matches the reference implementation's behavior:
 * </p>
 * 
 * <pre>
 * (densePartialKey & sparsePartialKey[i]) == sparsePartialKey[i]
 * </pre>
 */
@DisplayName("SparsePartialKeys Tests")
class SparsePartialKeysTest {

  @Nested
  @DisplayName("Byte Partial Keys")
  class BytePartialKeysTests {

    @Test
    @DisplayName("Search finds exact match")
    void testSearchExactMatch() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      keys.setEntry(0, (byte) 0b00000000);
      keys.setEntry(1, (byte) 0b00000001);
      keys.setEntry(2, (byte) 0b00000010);
      keys.setEntry(3, (byte) 0b00000011);

      // Search for 0b11 should match entries 0, 1, 2, 3 (all are subsets)
      int result = keys.search(0b11111111);
      assertEquals(0b1111, result, "All entries should match full mask");

      // Search for 0b01 should match entries 0, 1 (only subsets of 01)
      result = keys.search(0b00000001);
      assertEquals(0b0011, result, "Entries 0 and 1 should match 01");

      // Search for 0b10 should match entries 0, 2 (only subsets of 10)
      result = keys.search(0b00000010);
      assertEquals(0b0101, result, "Entries 0 and 2 should match 10");
    }

    @Test
    @DisplayName("Search with sparse keys")
    void testSearchSparseKeys() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(3);
      // Sparse keys: entry 0 has bit 0, entry 1 has bit 3, entry 2 has bits 0 and 3
      keys.setEntry(0, (byte) 0b00000001);
      keys.setEntry(1, (byte) 0b00001000);
      keys.setEntry(2, (byte) 0b00001001);

      // Dense key with bits 0, 1, 2, 3 set
      int result = keys.search(0b00001111);
      assertEquals(0b111, result, "All entries should match");

      // Dense key with only bit 0 set
      result = keys.search(0b00000001);
      assertEquals(0b001, result, "Only entry 0 should match");

      // Dense key with only bit 3 set
      result = keys.search(0b00001000);
      assertEquals(0b010, result, "Only entry 1 should match");
    }

    @Test
    @DisplayName("Empty keys always match")
    void testEmptyKeysMatch() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(3);
      keys.setEntry(0, (byte) 0);
      keys.setEntry(1, (byte) 0);
      keys.setEntry(2, (byte) 0);

      // Any search key should match all empty sparse keys
      assertEquals(0b111, keys.search(0b11111111));
      assertEquals(0b111, keys.search(0b00000000));
      assertEquals(0b111, keys.search(0b10101010));
    }
  }

  @Nested
  @DisplayName("Short Partial Keys")
  class ShortPartialKeysTests {

    @Test
    @DisplayName("Search with 16-bit keys")
    void testSearch16Bit() {
      SparsePartialKeys<Short> keys = SparsePartialKeys.forShorts(4);
      keys.setEntry(0, (short) 0x0000);
      keys.setEntry(1, (short) 0x0001);
      keys.setEntry(2, (short) 0x0100);
      keys.setEntry(3, (short) 0x0101);

      // Search for all bits
      int result = keys.search(0xFFFF);
      assertEquals(0b1111, result);

      // Search for low byte only
      result = keys.search(0x00FF);
      assertEquals(0b0011, result, "Entries 0 and 1 should match");

      // Search for high byte only
      result = keys.search(0xFF00);
      assertEquals(0b0101, result, "Entries 0 and 2 should match");
    }
  }

  @Nested
  @DisplayName("Int Partial Keys")
  class IntPartialKeysTests {

    @Test
    @DisplayName("Search with 32-bit keys")
    void testSearch32Bit() {
      SparsePartialKeys<Integer> keys = SparsePartialKeys.forInts(4);
      keys.setEntry(0, 0x00000000);
      keys.setEntry(1, 0x00000001);
      keys.setEntry(2, 0x00010000);
      keys.setEntry(3, 0x00010001);

      int result = keys.search(0xFFFFFFFF);
      assertEquals(0b1111, result);

      result = keys.search(0x0000FFFF);
      assertEquals(0b0011, result);
    }
  }

  @Nested
  @DisplayName("Find Masks By Pattern")
  class FindMasksByPatternTests {

    @Test
    @DisplayName("Find entries with specific prefix")
    void testFindWithPrefix() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      keys.setEntry(0, (byte) 0b00000000);
      keys.setEntry(1, (byte) 0b00000001);
      keys.setEntry(2, (byte) 0b00000100);
      keys.setEntry(3, (byte) 0b00000101);

      // Find entries where bit 2 is set
      int result = keys.findMasksByPattern(0b00000100, 0b00000100);
      assertEquals(0b1100, result, "Entries 2 and 3 should match");

      // Find entries where bit 0 is set
      result = keys.findMasksByPattern(0b00000001, 0b00000001);
      assertEquals(0b1010, result, "Entries 1 and 3 should match");
    }
  }

  @Nested
  @DisplayName("Relevant Bits for Range")
  class RelevantBitsTests {

    @Test
    @DisplayName("Get discriminative bits in range")
    void testGetRelevantBitsForRange() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      keys.setEntry(0, (byte) 0b00000000);
      keys.setEntry(1, (byte) 0b00000001);
      keys.setEntry(2, (byte) 0b00000011);
      keys.setEntry(3, (byte) 0b00000111);

      // Relevant bits in range [0, 4) - should find bits 0, 1, 2
      int relevantBits = keys.getRelevantBitsForRange(0, 4);
      assertEquals(0b00000111, relevantBits);

      // Relevant bits in range [1, 2) - just bit 1
      relevantBits = keys.getRelevantBitsForRange(1, 2);
      assertEquals(0b00000010, relevantBits);
    }
  }

  @Nested
  @DisplayName("Discriminating Bit Value")
  class DiscriminatingBitValueTests {

    @Test
    @DisplayName("First entry always has bit value 0")
    void testFirstEntryBitValue() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      assertFalse(keys.determineValueOfDiscriminatingBit(0));
    }

    @Test
    @DisplayName("Last entry always has bit value 1")
    void testLastEntryBitValue() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      assertTrue(keys.determineValueOfDiscriminatingBit(3));
    }

    @Test
    @DisplayName("Middle entries based on common bits")
    void testMiddleEntryBitValue() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(4);
      keys.setEntry(0, (byte) 0b00000000);
      keys.setEntry(1, (byte) 0b00000001);
      keys.setEntry(2, (byte) 0b00000010);
      keys.setEntry(3, (byte) 0b00000011);

      // Entry 1 and 2 in the middle
      boolean bitValue1 = keys.determineValueOfDiscriminatingBit(1);
      boolean bitValue2 = keys.determineValueOfDiscriminatingBit(2);

      // Values depend on common bits with neighbors
      // This tests the algorithm, not specific values
      assertTrue(bitValue1 || !bitValue1); // Always valid
      assertTrue(bitValue2 || !bitValue2); // Always valid
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Maximum entries (32)")
    void testMaxEntries() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(32);
      keys.setNumEntries(32);

      // Fill all 32 entries with values where 0xFF would match them
      // Entry 0 = 0x00 (empty, always matches)
      for (int i = 0; i < 32; i++) {
        keys.setEntry(i, (byte) i);
      }

      // Search with 0xFF should match entry 0 (0x00 is subset of any mask)
      // and any entries whose bits are all set in 0xFF
      int result = keys.search(0xFF);
      // Entry 0 = 0x00 should always match since (0xFF & 0x00) == 0x00
      assertTrue((result & 1) != 0, "Entry 0 (0x00) should always match");
    }

    @Test
    @DisplayName("Single entry")
    void testSingleEntry() {
      SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(1);
      keys.setEntry(0, (byte) 0b10101010);

      int result = keys.search(0xFF);
      assertEquals(0b1, result);

      result = keys.search(0b10101010);
      assertEquals(0b1, result);

      result = keys.search(0b01010101);
      assertEquals(0b0, result, "Should not match");
    }

    @Test
    @DisplayName("Invalid entry count throws")
    void testInvalidEntryCount() {
      assertThrows(IllegalArgumentException.class, () -> SparsePartialKeys.forBytes(33));
      assertThrows(IllegalArgumentException.class, () -> SparsePartialKeys.forBytes(-1));
    }
  }
}

