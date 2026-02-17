/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HOTIndirectPage - verifies SIMD-accelerated child lookup.
 * 
 * <p>
 * Validates that the integration of SparsePartialKeys provides correct child lookup for SpanNode
 * nodes.
 * </p>
 */
@DisplayName("HOTIndirectPage Tests")
class HOTIndirectPageTest {

  @Nested
  @DisplayName("BiNode Tests")
  class BiNodeTests {

    @Test
    @DisplayName("BiNode finds correct child for bit=0")
    void testBiNodeBitZero() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      // Discriminative bit at position 0 (MSB of byte 0)
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      // Key with MSB=0 should go left
      byte[] keyLeft = {0x00, 0x00, 0x00, 0x00};
      assertEquals(0, biNode.findChildIndex(keyLeft), "Key with MSB=0 should find left child");

      // Key with MSB=1 should go right
      byte[] keyRight = {(byte) 0x80, 0x00, 0x00, 0x00};
      assertEquals(1, biNode.findChildIndex(keyRight), "Key with MSB=1 should find right child");
    }

    @Test
    @DisplayName("BiNode finds correct child for bit in middle")
    void testBiNodeMiddleBit() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      // Discriminative bit at position 4 (middle of byte 0)
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 4, leftRef, rightRef);

      // Key with bit 4=0 (0x07 = 0000_0111)
      byte[] keyLeft = {0x07, 0x00, 0x00, 0x00};
      assertEquals(0, biNode.findChildIndex(keyLeft), "Key with bit 4=0 should find left child");

      // Key with bit 4=1 (0x08 = 0000_1000)
      byte[] keyRight = {0x08, 0x00, 0x00, 0x00};
      assertEquals(1, biNode.findChildIndex(keyRight), "Key with bit 4=1 should find right child");
    }
  }

  @Nested
  @DisplayName("SpanNode Tests with SparsePartialKeys")
  class SpanNodeTests {

    @Test
    @DisplayName("SpanNode uses SIMD search for child lookup")
    void testSpanNodeSIMDSearch() {
      // Create 4 children with different partial keys
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
      }

      // Partial keys: 0b00, 0b01, 0b10, 0b11 (using 2 bits)
      byte[] partialKeys = {0b00, 0b01, 0b10, 0b11};

      // Bit mask extracting bits 6 and 7 of byte 0
      long bitMask = 0b11L; // Bits 0-1 in little-endian representation

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, bitMask, partialKeys, children);

      assertNotNull(spanNode.getChildReference(0));
      assertEquals(4, spanNode.getNumChildren());

      // The SIMD search finds entries where (denseKey & sparseKey) == sparseKey
      // For entry 0 (sparse=0b00), any denseKey will match since (x & 0) == 0
      // For entry 1 (sparse=0b01), denseKey must have bit 0 set
      // For entry 2 (sparse=0b10), denseKey must have bit 1 set
      // For entry 3 (sparse=0b11), denseKey must have both bits set

      // Search with dense key 0b00 - should match entry 0
      int found = spanNode.findChildIndex(new byte[] {0b00, 0, 0, 0, 0, 0, 0, 0});
      assertEquals(0, found, "Dense key 0b00 should match entry 0 (sparse 0b00)");

      // Search with dense key 0b01 - should match entries 0 and 1, return lowest (0)
      found = spanNode.findChildIndex(new byte[] {0b01, 0, 0, 0, 0, 0, 0, 0});
      assertEquals(0, found, "Dense key 0b01 should match entry 0 first");

      // Search with dense key 0b11 - should match all entries, return lowest (0)
      found = spanNode.findChildIndex(new byte[] {0b11, 0, 0, 0, 0, 0, 0, 0});
      assertEquals(0, found, "Dense key 0b11 should match entry 0 first");
    }

    @Test
    @DisplayName("SpanNode copy preserves SparsePartialKeys")
    void testSpanNodeCopy() {
      PageReference[] children = new PageReference[3];
      for (int i = 0; i < 3; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
      }

      byte[] partialKeys = {0b00, 0b01, 0b10};
      long bitMask = 0b11L;

      HOTIndirectPage original = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, bitMask, partialKeys, children);

      // Create copy
      HOTIndirectPage copy = new HOTIndirectPage(original);

      // Both should find the same child
      byte[] searchKey = {0b01, 0, 0, 0, 0, 0, 0, 0};
      assertEquals(original.findChildIndex(searchKey), copy.findChildIndex(searchKey),
          "Copy should find same child as original");
    }

    @Test
    @DisplayName("SpanNode with 16 children uses SIMD")
    void testSpanNodeMax16Children() {
      PageReference[] children = new PageReference[16];
      byte[] partialKeys = new byte[16];
      for (int i = 0; i < 16; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i; // Distinct partial keys
      }

      // 4 bits needed to distinguish 16 entries
      long bitMask = 0b1111L;

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, bitMask, partialKeys, children);

      assertEquals(16, spanNode.getNumChildren());

      // Entry 0 (sparse=0) always matches first
      int found = spanNode.findChildIndex(new byte[] {0b1111, 0, 0, 0, 0, 0, 0, 0});
      assertEquals(0, found, "Should match entry 0 first (subset of all)");
    }
  }

  @Nested
  @DisplayName("MultiNode Tests")
  class MultiNodeTests {

    @Test
    @DisplayName("MultiNode uses direct byte indexing")
    void testMultiNodeDirectIndexing() {
      // Create 32 children
      PageReference[] children = new PageReference[32];
      for (int i = 0; i < 32; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
      }

      // Child index maps byte values to child slots
      byte[] childIndex = new byte[256];
      for (int i = 0; i < 256; i++) {
        childIndex[i] = (byte) (i % 32); // Round-robin mapping
      }

      HOTIndirectPage multiNode = HOTIndirectPage.createMultiNode(1L, 1, (byte) 0, childIndex, children);

      assertEquals(32, multiNode.getNumChildren());

      // Key with byte 0 = 5 should map to child 5
      int found = multiNode.findChildIndex(new byte[] {5, 0, 0, 0});
      assertEquals(5, found, "Byte 5 should map to child 5");

      // Key with byte 0 = 37 should map to child 5 (37 % 32)
      found = multiNode.findChildIndex(new byte[] {37, 0, 0, 0});
      assertEquals(5, found, "Byte 37 should map to child 5 (37 % 32)");
    }
  }

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Short key handled correctly")
    void testShortKey() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      // Discriminative bit at byte 2
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 16, leftRef, rightRef);

      // Key shorter than byte 2 - should go left (default)
      byte[] shortKey = {0x00};
      assertEquals(0, biNode.findChildIndex(shortKey), "Short key should find left child");
    }

    @Test
    @DisplayName("Empty key handled correctly")
    void testEmptyKey() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      // Empty key - should go left (default)
      byte[] emptyKey = {};
      assertEquals(0, biNode.findChildIndex(emptyKey), "Empty key should find left child");
    }

    @Test
    @DisplayName("copyWithUpdatedChild creates correct copy")
    void testCopyWithUpdatedChild() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      PageReference newRightRef = new PageReference();
      newRightRef.setKey(300);

      HOTIndirectPage copy = original.copyWithUpdatedChild(1, newRightRef);

      // Original unchanged
      assertEquals(200, original.getChildReference(1).getKey());
      // Copy has new child
      assertEquals(300, copy.getChildReference(1).getKey());
      // Left child unchanged
      assertEquals(100, copy.getChildReference(0).getKey());
    }
  }
}
