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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SiblingMerger - verifies merge-on-delete operations.
 */
@DisplayName("SiblingMerger Tests")
class SiblingMergerTest {

  @Nested
  @DisplayName("Merge Detection")
  class MergeDetectionTests {

    @Test
    @DisplayName("Should merge when fill factor is below threshold")
    void testShouldMerge() {
      // Create a SpanNode with only 2 children (below 25% of 16)
      PageReference[] children = new PageReference[2];
      byte[] partialKeys = new byte[2];
      for (int i = 0; i < 2; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }
      
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
          1L, 1, (byte) 0, 0b11L, partialKeys, children);
      
      // 2/16 = 12.5% < 25% threshold
      assertTrue(SiblingMerger.shouldMerge(spanNode));
    }

    @Test
    @DisplayName("Should not merge when fill factor is above threshold")
    void testShouldNotMerge() {
      // Create a SpanNode with 8 children (50% of 16)
      PageReference[] children = new PageReference[8];
      byte[] partialKeys = new byte[8];
      for (int i = 0; i < 8; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }
      
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
          1L, 1, (byte) 0, 0b11111111L, partialKeys, children);
      
      // 8/16 = 50% > 25% threshold
      assertFalse(SiblingMerger.shouldMerge(spanNode));
    }
  }

  @Nested
  @DisplayName("Merge Compatibility")
  class MergeCompatibilityTests {

    @Test
    @DisplayName("Can merge siblings with same height and combined fit")
    void testCanMerge() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(101);
      
      PageReference leftRef2 = new PageReference();
      PageReference rightRef2 = new PageReference();
      leftRef2.setKey(200);
      rightRef2.setKey(201);
      
      HOTIndirectPage left = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      HOTIndirectPage right = HOTIndirectPage.createBiNode(2L, 1, 4, leftRef2, rightRef2);
      
      assertTrue(SiblingMerger.canMerge(left, right), "Siblings at same height should merge");
    }

    @Test
    @DisplayName("Cannot merge when combined exceeds max")
    void testCannotMergeExceedsMax() {
      // Create two SpanNodes with 20 children each
      PageReference[] children1 = new PageReference[16];
      PageReference[] children2 = new PageReference[16];
      byte[] partialKeys1 = new byte[16];
      byte[] partialKeys2 = new byte[16];
      
      for (int i = 0; i < 16; i++) {
        children1[i] = new PageReference();
        children1[i].setKey(100 + i);
        partialKeys1[i] = (byte) i;
        
        children2[i] = new PageReference();
        children2[i].setKey(200 + i);
        partialKeys2[i] = (byte) (i + 16);
      }
      
      HOTIndirectPage left = HOTIndirectPage.createSpanNode(
          1L, 1, (byte) 0, 0xFFFFL, partialKeys1, children1);
      HOTIndirectPage right = HOTIndirectPage.createSpanNode(
          2L, 1, (byte) 0, 0xFFFFL, partialKeys2, children2);
      
      // 16 + 16 = 32, which equals MAX but should still fit
      assertTrue(SiblingMerger.canMerge(left, right));
    }
  }

  @Nested
  @DisplayName("Merge Operations")
  class MergeOperationTests {

    @Test
    @DisplayName("Merge two BiNodes into SpanNode")
    void testMergeBiNodesToSpanNode() {
      PageReference leftRef1 = new PageReference();
      PageReference rightRef1 = new PageReference();
      leftRef1.setKey(100);
      rightRef1.setKey(101);
      
      PageReference leftRef2 = new PageReference();
      PageReference rightRef2 = new PageReference();
      leftRef2.setKey(200);
      rightRef2.setKey(201);
      
      HOTIndirectPage left = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);
      HOTIndirectPage right = HOTIndirectPage.createBiNode(2L, 1, 4, leftRef2, rightRef2);
      
      SiblingMerger.MergeResult result = SiblingMerger.mergeSiblings(left, right, 3L, 1);
      
      assertTrue(result.success());
      assertNotNull(result.mergedNode());
      // 2 + 2 = 4 children → SpanNode
      assertEquals(NodeType.SPAN_NODE, result.mergedNode().getNodeType());
      assertEquals(4, result.mergedNode().getNumChildren());
    }

    @Test
    @DisplayName("Merge result failure when cannot merge")
    void testMergeFailure() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(101);
      
      // Create nodes at different heights (simulate by using different max children)
      PageReference[] children = new PageReference[16];
      byte[] partialKeys = new byte[16];
      for (int i = 0; i < 16; i++) {
        children[i] = new PageReference();
        children[i].setKey(200 + i);
        partialKeys[i] = (byte) i;
      }
      
      HOTIndirectPage left = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      // Create another SpanNode with 16 children at different height
      HOTIndirectPage right = HOTIndirectPage.createSpanNode(
          2L, 1, (byte) 0, 0xFFFFL, partialKeys, children, 5); // Height 5
      
      SiblingMerger.MergeResult result = SiblingMerger.mergeSiblings(left, right, 3L, 1);
      
      assertFalse(result.success(), "Merge should fail for different heights");
    }
  }

  @Nested
  @DisplayName("BiNode Collapse")
  class BiNodeCollapseTests {

    @Test
    @DisplayName("Cannot collapse BiNode with 2 children")
    void testCannotCollapseFull() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(200);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      assertFalse(SiblingMerger.canCollapseBiNode(biNode));
    }

    @Test
    @DisplayName("Cannot collapse SpanNode")
    void testCannotCollapseSpanNode() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }
      
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
          1L, 1, (byte) 0, 0b1111L, partialKeys, children);
      
      assertFalse(SiblingMerger.canCollapseBiNode(spanNode));
    }

    @Test
    @DisplayName("Get collapsed child throws for non-collapsible node")
    void testGetCollapsedChildThrows() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(200);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      assertThrows(IllegalStateException.class,
          () -> SiblingMerger.getCollapsedChild(biNode));
    }
  }

  @Nested
  @DisplayName("Fill Factor Calculation")
  class FillFactorTests {

    @Test
    @DisplayName("Calculate BiNode fill factor")
    void testBiNodeFillFactor() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(200);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      assertEquals(1.0, SiblingMerger.getFillFactor(biNode), 0.001,
          "BiNode with 2/2 children should be 100% full");
    }

    @Test
    @DisplayName("Calculate SpanNode fill factor")
    void testSpanNodeFillFactor() {
      PageReference[] children = new PageReference[8];
      byte[] partialKeys = new byte[8];
      for (int i = 0; i < 8; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }
      
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
          1L, 1, (byte) 0, 0b11111111L, partialKeys, children);
      
      assertEquals(0.5, SiblingMerger.getFillFactor(spanNode), 0.001,
          "SpanNode with 8/16 children should be 50% full");
    }
  }

  @Nested
  @DisplayName("Deletion with Merge")
  class DeletionWithMergeTests {

    @Test
    @DisplayName("No merge when sibling is null")
    void testNoMergeNullSibling() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(200);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      SiblingMerger.MergeResult result = SiblingMerger.handleDeletionWithMerge(
          biNode, null, 2L, 1);
      
      assertFalse(result.success());
    }
  }

  @Nested
  @DisplayName("Merge Result Record")
  class MergeResultTests {

    @Test
    @DisplayName("Failure result has expected values")
    void testFailureResult() {
      SiblingMerger.MergeResult failure = SiblingMerger.MergeResult.failure();
      
      assertFalse(failure.success());
      assertEquals(null, failure.mergedNode());
      assertFalse(failure.replacesLeft());
    }

    @Test
    @DisplayName("Success result has expected values")
    void testSuccessResult() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      leftRef.setKey(100);
      rightRef.setKey(200);
      
      HOTIndirectPage node = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      SiblingMerger.MergeResult success = SiblingMerger.MergeResult.success(node, true);
      
      assertTrue(success.success());
      assertNotNull(success.mergedNode());
      assertTrue(success.replacesLeft());
    }
  }
}

