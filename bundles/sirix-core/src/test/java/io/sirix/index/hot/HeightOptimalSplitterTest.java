/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTIndirectPage.NodeType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HeightOptimalSplitter - verifies height-optimal splitting algorithm.
 */
@DisplayName("HeightOptimalSplitter Tests")
class HeightOptimalSplitterTest {

  @Nested
  @DisplayName("Split Point Finding")
  class SplitPointTests {

    @Test
    @DisplayName("Find optimal split point for 2 keys")
    void testSplitPoint2Keys() {
      byte[][] keys = {{0x00, 0x01}, {0x00, 0x02}};
      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertEquals(1, splitPoint, "Split point for 2 keys should be 1");
    }

    @Test
    @DisplayName("Find optimal split point for 4 keys")
    void testSplitPoint4Keys() {
      byte[][] keys = {{0x00}, {0x01}, {0x02}, {0x03}};
      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertEquals(2, splitPoint, "Split point for 4 keys should be 2 (median)");
    }

    @Test
    @DisplayName("Find optimal split point for odd number of keys")
    void testSplitPointOddKeys() {
      byte[][] keys = {{0x00}, {0x01}, {0x02}, {0x03}, {0x04}};
      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertEquals(2, splitPoint, "Split point for 5 keys should be 2");
    }
  }

  @Nested
  @DisplayName("SpanNode Detection")
  class SpanNodeDetectionTests {

    @Test
    @DisplayName("Should create SpanNode when multiple bits differ in same byte")
    void testShouldCreateSpanNode() {
      // Keys that differ in bits 0 and 4 of first byte
      byte[] leftMax = {0b00000000};
      byte[] rightMin = {0b00010001}; // Bits 0 and 4 set

      // Single discriminative bit (MSB)
      int discriminativeBit = 7; // LSB

      boolean shouldCreate = HeightOptimalSplitter.shouldCreateSpanNode(leftMax, rightMin, discriminativeBit);
      assertTrue(shouldCreate, "Should suggest SpanNode when multiple bits differ");
    }

    @Test
    @DisplayName("Should not create SpanNode when only one bit differs")
    void testShouldNotCreateSpanNode() {
      byte[] leftMax = {0b00000000};
      byte[] rightMin = {0b00000001}; // Only bit 0 set

      int discriminativeBit = 7;

      boolean shouldCreate = HeightOptimalSplitter.shouldCreateSpanNode(leftMax, rightMin, discriminativeBit);
      assertFalse(shouldCreate, "Should not suggest SpanNode when only one bit differs");
    }

    @Test
    @DisplayName("Should not create SpanNode when too many bits differ")
    void testShouldNotCreateSpanNodeTooManyBits() {
      byte[] leftMax = {0b00000000};
      byte[] rightMin = {(byte) 0b11111111}; // All 8 bits differ

      int discriminativeBit = 0;

      boolean shouldCreate = HeightOptimalSplitter.shouldCreateSpanNode(leftMax, rightMin, discriminativeBit);
      assertFalse(shouldCreate, "Should not suggest SpanNode when >4 bits differ");
    }
  }

  @Nested
  @DisplayName("BiNode Creation")
  class BiNodeCreationTests {

    @Test
    @DisplayName("Create BiNode with correct references")
    void testCreateBiNode() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HeightOptimalSplitter.createBiNode(1L, 1, 0, leftRef, rightRef);

      assertNotNull(biNode);
      assertEquals(NodeType.BI_NODE, biNode.getNodeType());
      assertEquals(2, biNode.getNumChildren());
      assertEquals(100, biNode.getChildReference(0).getKey());
      assertEquals(200, biNode.getChildReference(1).getKey());
    }
  }

  @Nested
  @DisplayName("Split Result Integration")
  class SplitIntegrationTests {

    @Test
    @DisplayName("Integrate BiNode as new root")
    void testIntegrateBiNodeAsRoot() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage newRoot = HeightOptimalSplitter.createBiNode(1L, 1, 0, leftRef, rightRef);

      HeightOptimalSplitter.SplitResult result = new HeightOptimalSplitter.SplitResult(newRoot, leftRef, rightRef, 0);

      PageReference rootRef = HeightOptimalSplitter.integrateBiNodeIntoTree(result, null, 0);

      assertNotNull(rootRef);
      assertEquals(1L, rootRef.getKey());
    }
  }
}

