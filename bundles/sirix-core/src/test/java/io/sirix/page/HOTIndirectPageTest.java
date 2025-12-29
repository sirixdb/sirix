/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HOTIndirectPage}.
 */
class HOTIndirectPageTest {

  @Test
  void testBiNodeCreation() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    // Create BiNode discriminating on bit 8 (first bit of second byte)
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 8, leftChild, rightChild);
    
    assertEquals(1L, biNode.getPageKey());
    assertEquals(1, biNode.getRevision());
    assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
    assertEquals(HOTIndirectPage.LayoutType.SINGLE_MASK, biNode.getLayoutType());
    assertEquals(2, biNode.getNumChildren());
  }

  @Test
  void testBiNodeChildLookup() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    // Discriminate on bit 0 of byte 0
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    // Key with bit 0 = 0 should go left
    byte[] keyLeft = new byte[] { 0x00 };
    assertEquals(0, biNode.findChildIndex(keyLeft));
    
    // Key with bit 0 = 1 should go right
    byte[] keyRight = new byte[] { 0x01 };
    assertEquals(1, biNode.findChildIndex(keyRight));
  }

  @Test
  void testSpanNodeCreation() {
    PageReference[] children = new PageReference[4];
    for (int i = 0; i < 4; i++) {
      children[i] = new PageReference();
      children[i].setKey(100L + i);
    }
    
    byte[] partialKeys = new byte[] { 0, 1, 2, 3 };
    
    HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
        1L, 1, (byte) 0, 0x03L, partialKeys, children);
    
    assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, spanNode.getNodeType());
    assertEquals(4, spanNode.getNumChildren());
    
    // Verify children
    for (int i = 0; i < 4; i++) {
      assertNotNull(spanNode.getChildReference(i));
      assertEquals(100L + i, spanNode.getChildReference(i).getKey());
    }
  }

  @Test
  void testSpanNodeChildLookup() {
    PageReference[] children = new PageReference[4];
    for (int i = 0; i < 4; i++) {
      children[i] = new PageReference();
      children[i].setKey(100L + i);
    }
    
    // Partial keys for 4 children
    byte[] partialKeys = new byte[] { 0, 1, 2, 3 };
    
    // Mask extracts bits 0-1 from byte 0
    HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
        1L, 1, (byte) 0, 0x03L, partialKeys, children);
    
    // Keys with different values in bits 0-1
    byte[] key0 = new byte[] { 0x00 }; // partial = 0
    byte[] key1 = new byte[] { 0x01 }; // partial = 1
    byte[] key2 = new byte[] { 0x02 }; // partial = 2
    byte[] key3 = new byte[] { 0x03 }; // partial = 3
    
    assertEquals(0, spanNode.findChildIndex(key0));
    assertEquals(1, spanNode.findChildIndex(key1));
    assertEquals(2, spanNode.findChildIndex(key2));
    assertEquals(3, spanNode.findChildIndex(key3));
  }

  @Test
  void testSpanNodeNotFound() {
    PageReference[] children = new PageReference[2];
    children[0] = new PageReference();
    children[0].setKey(100L);
    children[1] = new PageReference();
    children[1].setKey(200L);
    
    byte[] partialKeys = new byte[] { 0, 3 }; // Only 0 and 3 exist
    
    HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
        1L, 1, (byte) 0, 0x03L, partialKeys, children);
    
    // Key with partial = 1 should not be found
    byte[] key = new byte[] { 0x01 };
    assertEquals(HOTIndirectPage.NOT_FOUND, spanNode.findChildIndex(key));
  }

  @Test
  void testMultiNodeCreation() {
    PageReference[] children = new PageReference[17];
    for (int i = 0; i < 17; i++) {
      children[i] = new PageReference();
      children[i].setKey(100L + i);
    }
    
    // Create child index mapping (256 bytes)
    byte[] childIndex = new byte[256];
    for (int i = 0; i < 17; i++) {
      childIndex[i] = (byte) i;
    }
    // All other indices point to child 0
    for (int i = 17; i < 256; i++) {
      childIndex[i] = 0;
    }
    
    HOTIndirectPage multiNode = HOTIndirectPage.createMultiNode(
        1L, 1, (byte) 0, childIndex, children);
    
    assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, multiNode.getNodeType());
    assertEquals(17, multiNode.getNumChildren());
  }

  @Test
  void testMultiNodeChildLookup() {
    PageReference[] children = new PageReference[17];
    for (int i = 0; i < 17; i++) {
      children[i] = new PageReference();
      children[i].setKey(100L + i);
    }
    
    byte[] childIndex = new byte[256];
    for (int i = 0; i < 256; i++) {
      childIndex[i] = (byte) (i < 17 ? i : 0);
    }
    
    HOTIndirectPage multiNode = HOTIndirectPage.createMultiNode(
        1L, 1, (byte) 0, childIndex, children);
    
    // Test direct byte lookup
    byte[] key5 = new byte[] { 5 };
    assertEquals(5, multiNode.findChildIndex(key5));
    
    byte[] key16 = new byte[] { 16 };
    assertEquals(16, multiNode.findChildIndex(key16));
  }

  @Test
  void testCopyWithUpdatedChild() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage original = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    PageReference newRightChild = new PageReference();
    newRightChild.setKey(300L);
    
    HOTIndirectPage copy = original.copyWithUpdatedChild(1, newRightChild);
    
    // Original unchanged
    assertEquals(200L, original.getChildReference(1).getKey());
    
    // Copy has new child
    assertEquals(300L, copy.getChildReference(1).getKey());
    
    // Other children unchanged
    assertEquals(100L, copy.getChildReference(0).getKey());
  }

  @Test
  @SuppressWarnings("resource") // HOTIndirectPage doesn't manage closeable resources
  void testCopyConstructor() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage original = HOTIndirectPage.createBiNode(1L, 1, 8, leftChild, rightChild);
    HOTIndirectPage copy = new HOTIndirectPage(original);
    
    assertEquals(original.getPageKey(), copy.getPageKey());
    assertEquals(original.getRevision(), copy.getRevision());
    assertEquals(original.getNodeType(), copy.getNodeType());
    assertEquals(original.getNumChildren(), copy.getNumChildren());
    
    // Children should be independent copies
    assertEquals(original.getChildReference(0).getKey(), copy.getChildReference(0).getKey());
    assertEquals(original.getChildReference(1).getKey(), copy.getChildReference(1).getKey());
  }

  @Test
  void testGetReferences() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    List<PageReference> refs = biNode.getReferences();
    assertEquals(2, refs.size());
  }

  @Test
  void testGetOrCreateReference() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    PageReference ref0 = biNode.getOrCreateReference(0);
    assertNotNull(ref0);
    assertEquals(100L, ref0.getKey());
    
    PageReference refInvalid = biNode.getOrCreateReference(5);
    assertNull(refInvalid);
  }

  @Test
  void testSetOrCreateReference() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    PageReference newRef = new PageReference();
    newRef.setKey(999L);
    
    assertFalse(biNode.setOrCreateReference(0, newRef));
    assertEquals(999L, biNode.getChildReference(0).getKey());
    
    // Out of bounds should return true (page full)
    assertTrue(biNode.setOrCreateReference(10, newRef));
  }

  @Test
  void testSpanNodeInvalidChildCount() {
    PageReference[] tooFew = new PageReference[1];
    assertThrows(IllegalArgumentException.class, () -> {
      HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0x01L, new byte[1], tooFew);
    });
    
    PageReference[] tooMany = new PageReference[17];
    assertThrows(IllegalArgumentException.class, () -> {
      HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0x01L, new byte[17], tooMany);
    });
  }

  @Test
  void testMultiNodeInvalidChildCount() {
    PageReference[] tooFew = new PageReference[16];
    assertThrows(IllegalArgumentException.class, () -> {
      HOTIndirectPage.createMultiNode(1L, 1, (byte) 0, new byte[256], tooFew);
    });
    
    PageReference[] tooMany = new PageReference[257];
    assertThrows(IllegalArgumentException.class, () -> {
      HOTIndirectPage.createMultiNode(1L, 1, (byte) 0, new byte[256], tooMany);
    });
  }

  @Test
  void testShortKeyHandling() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    // Create BiNode discriminating on byte 5
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 40, leftChild, rightChild);
    
    // Key shorter than discriminative byte - should go left (default)
    byte[] shortKey = new byte[] { 0x01, 0x02 };
    assertEquals(0, biNode.findChildIndex(shortKey));
  }

  @Test
  void testToString() {
    PageReference leftChild = new PageReference();
    leftChild.setKey(100L);
    
    PageReference rightChild = new PageReference();
    rightChild.setKey(200L);
    
    HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftChild, rightChild);
    
    String str = biNode.toString();
    assertNotNull(str);
    assertTrue(str.contains("HOTIndirectPage"));
    assertTrue(str.contains("BI_NODE"));
  }
}

