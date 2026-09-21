/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The most significant bit a half keeps when its node is split.
 *
 * <p>
 * A half drops every discriminative bit that is constant within it, so its MSB can be far less
 * significant than the node's was. The node's children satisfied the trie condition against the
 * node (I11: an indirect child's MSB is strictly less significant than its parent's); against the
 * half they need not. That is the case where two siblings are told apart by a bit <em>less</em>
 * significant than one of them branches on internally — a shape that passes every invariant until a
 * split changes who the parent is. A writer has to ask
 * {@link HOTIncrementalInsert#mostSignificantLiveBit} before it publishes a split, and the answer
 * has to be the MSB {@link HOTIncrementalInsert#splitIndirect} really gives the half.
 * </p>
 *
 * <p>
 * Keys are one byte. The node routes on bit 0 ({@code 0x80}) and bit 4 ({@code 0x08}): {@code 00}
 * below bit 0, and above it a child node {@code 80 | 90} that branches on bit 3 ({@code 0x10})
 * beside the leaf {@code 98}, which bit 4 alone tells apart from it.
 * </p>
 */
final class HOTSplitHalfTrieConditionTest {

  private static final byte[] VALUE = {0x01};

  private static final int[] NODE_BITS = {0, 4};
  private static final int[] NODE_PARTIALS = {0b00, 0b10, 0b11};

  /**
   * The bit the child node branches on: more significant than the one that tells it from its sibling.
   */
  private static final int CHILD_BIT = 3;

  @Test
  @DisplayName("a half's most significant live bit is the MSB the split gives it, even past a child's")
  void liveBitOfAHalfIsTheMsbTheSplitGivesIt() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>(4);
    try {
      final PageReference below = swizzle(leaf(allocator, leaves, 0x00));
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(allocator.getAndIncrement(), 1, CHILD_BIT,
          swizzle(leaf(allocator, leaves, 0x80)), swizzle(leaf(allocator, leaves, 0x90)), 1);
      final PageReference childRef = swizzle(child);
      final PageReference sibling = swizzle(leaf(allocator, leaves, 0x98));
      final HOTIndirectPage node = HOTBulkBuilder.assembleIndirect(NODE_BITS, NODE_PARTIALS,
          new PageReference[] {below, childRef, sibling}, 2, 1, allocator::getAndIncrement);
      assertTrue(child.getMostSignificantBitIndex() > node.getMostSignificantBitIndex(),
          "against the node the child satisfies the trie condition");

      assertEquals(-1, HOTIncrementalInsert.mostSignificantLiveBit(NODE_BITS, NODE_PARTIALS, 0, 1),
          "a lone child is pulled up bare and gets no node");
      final int upperHalfMsb = HOTIncrementalInsert.mostSignificantLiveBit(NODE_BITS, NODE_PARTIALS, 1, 3);
      assertEquals(4, upperHalfMsb, "bit 0 is constant above the split, so bit 4 is all the upper half keeps");

      final HOTIncrementalInsert.BiNode split = HOTIncrementalInsert.splitIndirect(node, 2, allocator::getAndIncrement);

      assertSame(below, split.left(), "the lone lower child hangs directly under the split");
      final HOTIndirectPage upperHalf = (HOTIndirectPage) split.right().getPage();
      assertEquals(upperHalfMsb, upperHalf.getMostSignificantBitIndex(),
          "the predicate must name the MSB the split really gives the half");
      assertSame(childRef, upperHalf.getChildReference(0));
      assertTrue(child.getMostSignificantBitIndex() <= upperHalf.getMostSignificantBitIndex(),
          "against the half the same child breaks the trie condition: the split must not be published");
    } finally {
      for (final HOTLeafPage leaf : leaves) {
        leaf.close();
      }
    }
  }

  private static HOTLeafPage leaf(final AtomicLong allocator, final List<HOTLeafPage> leaves, final int key) {
    final HOTLeafPage leaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    leaves.add(leaf);
    assertTrue(leaf.put(new byte[] {(byte) key}, VALUE));
    return leaf;
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }
}
