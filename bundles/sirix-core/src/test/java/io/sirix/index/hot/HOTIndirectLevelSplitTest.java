/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node may only route on a key byte its layout can name, so a bit set reaching past the
 * {@code MULTI_MASK} extraction range is spread over levels instead (FMT-305).
 */
final class HOTIndirectLevelSplitTest {

  private static final byte[] VALUE = {1, 2, 3, 4};

  private static final int EARLY_BYTE = 10;

  /** Past the highest key byte a {@code MULTI_MASK} extraction position can name. */
  private static final int UNNAMEABLE_BYTE = 300;

  /** Inside it, so a wide span is still one {@code MULTI_MASK} node. */
  private static final int NAMEABLE_BYTE = 200;

  /** The first byte past the extraction range, where the wide fixture puts its later bits. */
  private static final int LATE_BYTE = UNNAMEABLE_BYTE;

  @Test
  void aSpanReachingPastTheExtractionRangeIsSplitOverLevels() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>();
    try (final HOTIndirectPage root = assemble(UNNAMEABLE_BYTE, leaves, pageKeys)) {
      assertEquals(HOTIndirectPage.LayoutType.SINGLE_MASK, root.getLayoutType(),
          "the top level keeps only the bits one layout can name");
      assertNull(root.getExtractionPositions(), "no level may name a byte in an extraction position here");
      assertEquals(2, root.getNumChildren(), "one child per side of the early bit");
      assertEquals(EARLY_BYTE * Byte.SIZE, root.getMostSignificantBitIndex());
      assertEquals(2, root.getHeight(), "the split adds one level above the leaves");

      for (int side = 0; side < 2; side++) {
        final Page child = root.getChildReference(side).getPage();
        final HOTIndirectPage inner = assertInstanceOf(HOTIndirectPage.class, child, "side " + side + " is a level");
        assertEquals(HOTIndirectPage.LayoutType.SINGLE_MASK, inner.getLayoutType());
        assertNull(inner.getExtractionPositions());
        assertEquals(2, inner.getNumChildren());
        assertEquals(UNNAMEABLE_BYTE * Byte.SIZE, inner.getMostSignificantBitIndex(),
            "an inner level tests a strictly later bit than its parent");
        assertEquals(1, inner.getHeight());
      }

      // Every key reaches its own leaf, which is what the reduced extraction position broke.
      for (int index = 0; index < leaves.size(); index++) {
        final byte[] key = key(UNNAMEABLE_BYTE, index);
        assertSame(leaves.get(index), descend(root, key), "key " + index + " must reach its own leaf");
        assertTrue(descend(root, key).findEntry(key, key.length) >= 0);
      }
    } finally {
      close(leaves);
    }
  }

  @Test
  void aWideSpanInsideTheExtractionRangeStaysOneNode() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>();
    try (final HOTIndirectPage root = assemble(NAMEABLE_BYTE, leaves, pageKeys)) {
      assertEquals(HOTIndirectPage.LayoutType.MULTI_MASK, root.getLayoutType(),
          "a span of eight bytes or more is still one MultiMask node while its bytes fit");
      assertNotNull(root.getExtractionPositions());
      assertEquals(4, root.getNumChildren(), "no level was inserted");
      assertEquals(1, root.getHeight());
      for (int index = 0; index < leaves.size(); index++) {
        final byte[] key = key(NAMEABLE_BYTE, index);
        assertSame(leaves.get(index), descend(root, key), "key " + index + " must reach its own leaf");
      }
    } finally {
      close(leaves);
    }
  }

  @Test
  void aLeadingBitThatBranchesNoChildIsRefusedWithoutDrawingAPageKey() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final PageReference[] children = {new PageReference(), new PageReference()};
    // Both children sit on the 0-side of the early bit, so it branches nothing: not a block any
    // canonical sparse-path encoding produces, and not a set that can be split over it either.
    final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
        () -> HOTBulkBuilder.assembleIndirect(new int[] {EARLY_BYTE * Byte.SIZE, UNNAMEABLE_BYTE * Byte.SIZE},
            new int[] {0, 1}, children, 1, 1, IndexType.NAME, pageKeys::getAndIncrement));
    assertTrue(refusal.getMessage().contains("branch no child"), refusal.getMessage());
    assertEquals(1, pageKeys.get(), "a refused assembly must not draw a page key");
  }

  @Test
  void theExtractionRangeIsTheByteAnExtractionPositionIsStoredIn() {
    assertEquals(0xFF, HOTIndirectPage.MAX_EXTRACTION_BYTE_POSITION);
    assertTrue(HOTIndirectPage.canNameDiscriminativeBytes(300, 307), "an eight-byte window at any base");
    assertFalse(HOTIndirectPage.canNameDiscriminativeBytes(300, 308), "one byte wider than the window");
    assertTrue(HOTIndirectPage.canNameDiscriminativeBytes(0, 255), "extraction positions up to the last byte");
    assertFalse(HOTIndirectPage.canNameDiscriminativeBytes(0, 256), "one byte past them");
  }

  /** Four single-entry leaves under one bit at {@link #EARLY_BYTE} and one at {@code lateByte}. */
  /**
   * A nest over four discriminative bits in three regions: two the top level can name together, and
   * two past the extraction range. One group's children agree on the last bit, so that bit branches
   * nothing inside it and the group's own node must not carry it; the other group's children differ
   * in both. Every key must still reach its own leaf, which is what a wrong mask, a wrong shift or a
   * wrong repacking of the group partials breaks.
   */
  @Test
  void eachLevelOfANestCarriesExactlyItsOwnBranchingBits() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>();
    // Column weights over [b80, b81, b2400, b2408] are 8, 4, 2, 1.
    final int[] partials = {0b0000, 0b0010, 0b0100, 0b0111};
    try (final HOTIndirectPage root = assembleWide(partials, leaves, pageKeys)) {
      assertEquals(2, root.getNumChildren(), "the top level branches on the early byte alone");
      assertArrayEquals(new int[] {EARLY_BYTE * Byte.SIZE, EARLY_BYTE * Byte.SIZE + 1},
          root.getDiscriminativeBits(), "the top level keeps the bits it can name and no others");
      assertEquals(2, root.getHeight());

      final HOTIndirectPage agreeing = assertInstanceOf(HOTIndirectPage.class,
          root.getChildReference(0).getPage(), "first group");
      assertArrayEquals(new int[] {LATE_BYTE * Byte.SIZE}, agreeing.getDiscriminativeBits(),
          "a bit the whole group agrees on branches nothing there, so the group must not test it");
      assertEquals(2, agreeing.getNumChildren());
      assertEquals(1, agreeing.getHeight());

      final HOTIndirectPage differing = assertInstanceOf(HOTIndirectPage.class,
          root.getChildReference(1).getPage(), "second group");
      assertArrayEquals(new int[] {LATE_BYTE * Byte.SIZE, (LATE_BYTE + 1) * Byte.SIZE},
          differing.getDiscriminativeBits(), "this group branches on both remaining bits");
      assertEquals(2, differing.getNumChildren());
      assertEquals(1, differing.getHeight());

      for (int index = 0; index < partials.length; index++) {
        final byte[] key = wideKey(partials[index]);
        assertSame(leaves.get(index), descend(root, key),
            "the key of partial " + Integer.toBinaryString(partials[index]) + " must reach its own leaf");
      }
    } finally {
      close(leaves);
    }
  }

  /**
   * A group of one child needs no node of its own, and the nest's height is measured from the
   * children it was given. An unresolved reference is a persisted subtree whose height only the
   * caller knows, so the nest must use the height the caller stated for it rather than treat it as a
   * leaf.
   */
  @Test
  void aNestMeasuresItsHeightFromTheChildrenItWasGiven() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>();
    final PageReference[] children = new PageReference[3];
    for (int index = 0; index < 2; index++) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
      assertTrue(leaf.put(wideKey(index == 0 ? 0b0000 : 0b0010), VALUE));
      leaves.add(leaf);
      children[index] = new PageReference();
      children[index].setPage(leaf);
    }
    // A reference whose page is not resident: the caller's stated height is the only figure there is.
    children[2] = new PageReference();
    final int statedHeight = 3;
    try (final HOTIndirectPage root = HOTBulkBuilder.assembleIndirect(wideBits(), new int[] {0b0000, 0b0010, 0b0100},
        children, statedHeight, 1, IndexType.NAME, pageKeys::getAndIncrement)) {
      assertEquals(2, root.getNumChildren(), "the lone child joins the top level directly");
      assertSame(children[2], root.getChildReference(1), "a group of one child is linked, not wrapped");
      assertEquals(statedHeight, root.getHeight(),
          "the unresolved subtree is as tall as the caller said its tallest child was");
    } finally {
      close(leaves);
    }
  }

  /**
   * A refused group leaves nothing of the nest behind. The second group's children carry the same
   * partial, which no canonical encoding produces, so assembly refuses after the first group's node
   * was already built; the children the caller handed in are not the nest's to release.
   */
  @Test
  void aRefusedGroupReleasesOnlyTheNodesTheNestBuilt() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final List<HOTLeafPage> leaves = new ArrayList<>();
    final int[] partials = {0b0000, 0b0010, 0b0100, 0b0100};
    final PageReference[] children = new PageReference[partials.length];
    for (int index = 0; index < partials.length; index++) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
      assertTrue(leaf.put(wideKey(partials[index] | index), VALUE));
      leaves.add(leaf);
      children[index] = new PageReference();
      children[index].setPage(leaf);
    }
    try {
      final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
          () -> HOTBulkBuilder.assembleIndirect(wideBits(), partials, children, 1, 1, IndexType.NAME,
              pageKeys::getAndIncrement));
      assertTrue(refusal.getMessage().contains("identical partials"), refusal.getMessage());
      for (int index = 0; index < leaves.size(); index++) {
        assertFalse(leaves.get(index).isClosed(), "the caller's child " + index + " is not the nest's to release");
      }
    } finally {
      close(leaves);
    }
  }

  /** The four discriminative bits of the wide fixture, ascending. */
  private static int[] wideBits() {
    return new int[] {EARLY_BYTE * Byte.SIZE, EARLY_BYTE * Byte.SIZE + 1, LATE_BYTE * Byte.SIZE,
        (LATE_BYTE + 1) * Byte.SIZE};
  }

  private static HOTIndirectPage assembleWide(final int[] partials, final List<HOTLeafPage> leaves,
      final AtomicLong pageKeys) {
    final PageReference[] children = new PageReference[partials.length];
    for (int index = 0; index < partials.length; index++) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
      assertTrue(leaf.put(wideKey(partials[index]), VALUE));
      leaves.add(leaf);
      children[index] = new PageReference();
      children[index].setPage(leaf);
    }
    return HOTBulkBuilder.assembleIndirect(wideBits(), partials, children, 1, 1, IndexType.NAME,
        pageKeys::getAndIncrement);
  }

  /** A key carrying exactly the bits the four-column {@code partial} names. */
  private static byte[] wideKey(final int partial) {
    final byte[] key = new byte[LATE_BYTE + 2];
    Arrays.fill(key, (byte) 'x');
    key[EARLY_BYTE] = (byte) (((partial & 0b1000) != 0 ? 0x80 : 0) | ((partial & 0b0100) != 0 ? 0x40 : 0));
    key[LATE_BYTE] = (partial & 0b0010) != 0 ? (byte) 0x80 : 0x00;
    key[LATE_BYTE + 1] = (partial & 0b0001) != 0 ? (byte) 0x80 : 0x00;
    return key;
  }

  private static HOTIndirectPage assemble(final int lateByte, final List<HOTLeafPage> leaves,
      final AtomicLong pageKeys) {
    final PageReference[] children = new PageReference[4];
    for (int index = 0; index < children.length; index++) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
      final byte[] key = key(lateByte, index);
      assertTrue(leaf.put(key, VALUE));
      leaves.add(leaf);
      children[index] = new PageReference();
      children[index].setPage(leaf);
    }
    return HOTBulkBuilder.assembleIndirect(new int[] {EARLY_BYTE * Byte.SIZE, lateByte * Byte.SIZE},
        new int[] {0, 1, 2, 3}, children, 1, 1, IndexType.NAME, pageKeys::getAndIncrement);
  }

  /**
   * Key {@code index} of the fixture: bit two selects {@link #EARLY_BYTE}, bit one {@code lateByte}.
   */
  private static byte[] key(final int lateByte, final int index) {
    final byte[] key = new byte[lateByte + 1];
    Arrays.fill(key, (byte) 'x');
    key[EARLY_BYTE] = (index & 2) == 0
        ? 0x00
        : (byte) 0x80;
    key[lateByte] = (index & 1) == 0
        ? 0x00
        : (byte) 0x80;
    return key;
  }

  private static HOTLeafPage descend(final HOTIndirectPage root, final byte[] key) {
    Page page = root;
    while (page instanceof HOTIndirectPage indirect) {
      final int slot = indirect.findChildIndex(key, key.length);
      assertTrue(slot >= 0, "routing must find a child");
      page = indirect.getChildReference(slot).getPage();
    }
    return assertInstanceOf(HOTLeafPage.class, page, "a descent must end in a leaf");
  }

  private static void close(final List<HOTLeafPage> leaves) {
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
  }
}
