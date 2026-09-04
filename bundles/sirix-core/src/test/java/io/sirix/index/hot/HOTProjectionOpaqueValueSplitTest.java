/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Projection values remain opaque and last-writer-wins through the shared incremental split. */
final class HOTProjectionOpaqueValueSplitTest {

  @Test
  void duplicateProjectionSlotIsReplacedWithoutBitmapDecoding() {
    assertReplacement(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
  }

  @Test
  void duplicateProjectionSlotCanBecomeAZeroLengthTombstone() {
    assertReplacement(new byte[0]);
  }

  @Test
  void anUnrelatedZeroLengthTombstoneSurvivesTheSplit() {
    final byte[] tombstoneKey = {(byte) 0x00};
    final byte[] replacedKey = {(byte) 0x40};
    final byte[] otherKey = {(byte) 0x80};
    final byte[] replacement = {(byte) 0xF0, 0x0D};
    final HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PROJECTION);
    assertTrue(source.put(tombstoneKey, new byte[0]));
    assertTrue(source.put(replacedKey, new byte[] {1}));
    assertTrue(source.put(otherKey, new byte[] {2}));

    HOTIncrementalInsert.BiNode split = null;
    try {
      split = HOTIncrementalInsert.splitLeafPage(source, replacedKey, replacement, 2, IndexType.PROJECTION,
          new AtomicLong(10)::getAndIncrement);
      assertEquals(3, countEntries(split.left()) + countEntries(split.right()));
      assertArrayEquals(new byte[0], valueFor(split, tombstoneKey));
      assertArrayEquals(replacement, valueFor(split, replacedKey));
      assertArrayEquals(new byte[] {2}, valueFor(split, otherKey));
    } finally {
      if (split != null) {
        final Map<Page, Boolean> visited = new IdentityHashMap<>();
        closeSubtree(split.left(), visited);
        closeSubtree(split.right(), visited);
      }
      source.close();
    }
  }

  private static void assertReplacement(final byte[] replacement) {
    final byte[] replacedKey = {(byte) 0x00};
    final byte[] otherKey = {(byte) 0x80};
    final byte[] originalOpaqueValue = {(byte) 0xC1, 0x23, 0x45};
    final byte[] otherOpaqueValue = {0x55, (byte) 0xAA};
    final HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PROJECTION);
    assertTrue(source.put(replacedKey, originalOpaqueValue));
    assertTrue(source.put(otherKey, otherOpaqueValue));

    HOTIncrementalInsert.BiNode split = null;
    try {
      split = HOTIncrementalInsert.splitLeafPage(source, replacedKey, replacement, 2, IndexType.PROJECTION,
          new AtomicLong(10)::getAndIncrement);
      assertEquals(2, countEntries(split.left()) + countEntries(split.right()));
      assertArrayEquals(replacement, valueFor(split, replacedKey));
      assertArrayEquals(otherOpaqueValue, valueFor(split, otherKey));
      assertArrayEquals(originalOpaqueValue, source.getValue(source.findEntry(replacedKey)),
          "the unpublished source page must remain untouched");
    } finally {
      if (split != null) {
        final Map<Page, Boolean> visited = new IdentityHashMap<>();
        closeSubtree(split.left(), visited);
        closeSubtree(split.right(), visited);
      }
      source.close();
    }
  }

  private static byte[] valueFor(final HOTIncrementalInsert.BiNode split, final byte[] key) {
    final PageReference half = HOTBulkBuilder.bitAt(key, split.discriminativeBitIndex())
        ? split.right()
        : split.left();
    final HOTLeafPage leaf = findLeaf(half, key);
    assertNotNull(leaf, "the split must route the projection slot to a physical leaf");
    final int index = leaf.findEntry(key);
    assertTrue(index >= 0, "the routed leaf must contain the projection slot");
    final long valueRef = leaf.valueRef(index);
    final int valueLength = HOTLeafPage.refLength(valueRef);
    final byte[] value = new byte[valueLength];
    if (valueLength > 0) {
      leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
    }
    return value;
  }

  private static HOTLeafPage findLeaf(final PageReference reference, final byte[] key) {
    Page page = reference.getPage();
    while (page instanceof HOTIndirectPage indirect) {
      final int childIndex = indirect.findChildIndex(key);
      if (childIndex < 0) {
        return null;
      }
      page = indirect.getChildReference(childIndex).getPage();
    }
    return page instanceof HOTLeafPage leaf
        ? leaf
        : null;
  }

  private static int countEntries(final PageReference reference) {
    final Page page = reference.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getEntryCount();
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int count = 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      count += countEntries(indirect.getChildReference(i));
    }
    return count;
  }

  private static void closeSubtree(final PageReference reference, final Map<Page, Boolean> visited) {
    final Page page = reference.getPage();
    if (page == null || visited.put(page, Boolean.TRUE) != null) {
      return;
    }
    if (page instanceof HOTLeafPage leaf) {
      leaf.close();
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      closeSubtree(indirect.getChildReference(i), visited);
    }
  }
}
