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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for NodeUpgradeManager - verifies node type transitions.
 */
@DisplayName("NodeUpgradeManager Tests")
class NodeUpgradeManagerTest {

  @Nested
  @DisplayName("Node Type Determination by Children")
  class NodeTypeDeterminationTests {

    @ParameterizedTest(name = "{0} children → {1}")
    @CsvSource({"1, BI_NODE", "2, BI_NODE", "3, SPAN_NODE", "16, SPAN_NODE", "17, MULTI_NODE", "32, MULTI_NODE"})
    void testDetermineNodeType(int numChildren, NodeType expectedType) {
      assertEquals(expectedType, NodeUpgradeManager.determineNodeType(numChildren));
    }

    @Test
    @DisplayName("Invalid child count throws exception")
    void testInvalidChildCount() {
      assertThrows(IllegalArgumentException.class, () -> NodeUpgradeManager.determineNodeType(0));
      assertThrows(IllegalArgumentException.class, () -> NodeUpgradeManager.determineNodeType(33));
    }
  }

  @Nested
  @DisplayName("Node Type Determination by Bits")
  class NodeTypeByBitsTests {

    @ParameterizedTest(name = "{0} bits → {1}")
    @CsvSource({"0, BI_NODE", "1, BI_NODE", "2, SPAN_NODE", "3, SPAN_NODE", "4, SPAN_NODE", "5, MULTI_NODE",
        "8, MULTI_NODE"})
    void testDetermineNodeTypeByBits(int numBits, NodeType expectedType) {
      assertEquals(expectedType, NodeUpgradeManager.determineNodeTypeByBits(numBits));
    }
  }

  @Nested
  @DisplayName("Upgrade Detection")
  class UpgradeDetectionTests {

    @Test
    @DisplayName("BiNode needs upgrade when children exceed 2")
    void testBiNodeNeedsUpgrade() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      assertFalse(NodeUpgradeManager.needsUpgrade(biNode, 2));
      assertTrue(NodeUpgradeManager.needsUpgrade(biNode, 3));
    }

    @Test
    @DisplayName("SpanNode needs upgrade when children exceed 16")
    void testSpanNodeNeedsUpgrade() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0b1111L, partialKeys, children);

      assertFalse(NodeUpgradeManager.needsUpgrade(spanNode, 16));
      assertTrue(NodeUpgradeManager.needsUpgrade(spanNode, 17));
    }
  }

  @Nested
  @DisplayName("Downgrade Detection")
  class DowngradeDetectionTests {

    @Test
    @DisplayName("SpanNode should downgrade when children drop to 2")
    void testSpanNodeShouldDowngrade() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0b1111L, partialKeys, children);

      assertFalse(NodeUpgradeManager.shouldDowngrade(spanNode, 4));
      assertFalse(NodeUpgradeManager.shouldDowngrade(spanNode, 3));
      assertTrue(NodeUpgradeManager.shouldDowngrade(spanNode, 2));
    }

    @Test
    @DisplayName("Node should be removed when children drop to 0")
    void testNodeShouldBeRemoved() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      assertTrue(NodeUpgradeManager.shouldDowngrade(biNode, 0));
    }
  }

  @Nested
  @DisplayName("Full Node Detection")
  class FullNodeTests {

    @Test
    @DisplayName("Node with 32 children is full")
    void testNodeIsFull() {
      PageReference[] children = new PageReference[32];
      byte[] childIndex = new byte[256];
      for (int i = 0; i < 32; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
      }
      for (int i = 0; i < 256; i++) {
        childIndex[i] = (byte) (i % 32);
      }

      HOTIndirectPage multiNode = HOTIndirectPage.createMultiNode(1L, 1, (byte) 0, childIndex, children);

      assertTrue(NodeUpgradeManager.isFull(multiNode));
    }

    @Test
    @DisplayName("Node with less than 32 children is not full")
    void testNodeIsNotFull() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      assertFalse(NodeUpgradeManager.isFull(biNode));
    }
  }

  @Nested
  @DisplayName("Underfilled Detection")
  class UnderfilledTests {

    @ParameterizedTest(name = "BiNode with fill factor {0}")
    @ValueSource(doubles = {0.3, 0.5, 0.7})
    void testBiNodeUnderfilledWithFactor(double fillFactor) {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      // BiNode has 2 children, max is 2, so it's always 100% full
      assertFalse(NodeUpgradeManager.isUnderfilled(biNode, fillFactor));
    }
  }

  @Nested
  @DisplayName("Max Children by Type")
  class MaxChildrenTests {

    @ParameterizedTest(name = "{0} → max {1}")
    @CsvSource({"BI_NODE, 2", "SPAN_NODE, 16", "MULTI_NODE, 32"})
    void testGetMaxChildrenForType(NodeType nodeType, int expectedMax) {
      assertEquals(expectedMax, NodeUpgradeManager.getMaxChildrenForType(nodeType));
    }
  }

  @Nested
  @DisplayName("BiNode Merge Detection")
  class BiNodeMergeTests {

    @Test
    @DisplayName("BiNodes with same initial byte should merge")
    void testShouldMergeToSpanNode() {
      PageReference leftRef1 = new PageReference();
      PageReference rightRef1 = new PageReference();
      leftRef1.setKey(100);
      rightRef1.setKey(101);

      PageReference leftRef2 = new PageReference();
      PageReference rightRef2 = new PageReference();
      leftRef2.setKey(200);
      rightRef2.setKey(201);

      // Both BiNodes use discriminative bit in byte 0
      HOTIndirectPage biNode1 = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);
      HOTIndirectPage biNode2 = HOTIndirectPage.createBiNode(2L, 1, 4, leftRef2, rightRef2);

      assertTrue(NodeUpgradeManager.shouldMergeToSpanNode(biNode1, biNode2));
    }

    @Test
    @DisplayName("BiNodes with different initial bytes should not merge")
    void testShouldNotMergeToSpanNode() {
      PageReference leftRef1 = new PageReference();
      PageReference rightRef1 = new PageReference();
      leftRef1.setKey(100);
      rightRef1.setKey(101);

      PageReference leftRef2 = new PageReference();
      PageReference rightRef2 = new PageReference();
      leftRef2.setKey(200);
      rightRef2.setKey(201);

      // BiNode1 uses bit in byte 0, BiNode2 uses bit in byte 1
      HOTIndirectPage biNode1 = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);
      HOTIndirectPage biNode2 = HOTIndirectPage.createBiNode(2L, 1, 8, leftRef2, rightRef2);

      assertFalse(NodeUpgradeManager.shouldMergeToSpanNode(biNode1, biNode2));
    }
  }
}

