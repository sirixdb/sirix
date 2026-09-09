/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fail-closed height accounting for pure incremental HOT structural primitives. */
final class HOTIncrementalHeightResolutionTest {

  @Test
  void splitRefusesAnUnresolvedHalfInsteadOfCountingItAsAHeightZeroLeaf() {
    final HOTLeafPage rightLeaf = leaf(1, 0x80);
    final PageReference unresolvedTallLeft = new PageReference();
    unresolvedTallLeft.setKey(100);
    final HOTIndirectPage node = HOTIndirectPage.createBiNode(2, 1, 0, unresolvedTallLeft, swizzle(rightLeaf), 2);

    try {
      assertThrows(IllegalStateException.class,
          () -> HOTIncrementalInsert.splitIndirect(node, 2, new AtomicLong(10)::getAndIncrement));
    } finally {
      rightLeaf.close();
    }
  }

  @Test
  void rangeCompressionRefusesAnyUnresolvedDirectChild() {
    final HOTLeafPage first = leaf(20, 0x00);
    final HOTLeafPage second = leaf(21, 0x40);
    final HOTLeafPage fourth = leaf(23, 0xC0);
    final PageReference unresolvedTallThird = new PageReference();
    unresolvedTallThird.setKey(200);
    final HOTIndirectPage node =
        HOTIndirectPage.createMultiNode(24, 1, 0, 0xC000_0000_0000_0000L, new int[] {0, 1, 2, 3},
            new PageReference[] {swizzle(first), swizzle(second), unresolvedTallThird, swizzle(fourth)}, 3);

    try {
      assertThrows(IllegalStateException.class,
          () -> HOTIncrementalInsert.compressChildRange(node, 0, 4, 2, new AtomicLong(30)::getAndIncrement));
    } finally {
      first.close();
      second.close();
      fourth.close();
    }
  }

  private static HOTLeafPage leaf(final long pageKey, final int key) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PATH);
    assertTrue(leaf.put(new byte[] {(byte) key}, new byte[] {(byte) key}));
    return leaf;
  }

  private static PageReference swizzle(final HOTLeafPage leaf) {
    final PageReference reference = new PageReference();
    reference.setPage(leaf);
    return reference;
  }
}
