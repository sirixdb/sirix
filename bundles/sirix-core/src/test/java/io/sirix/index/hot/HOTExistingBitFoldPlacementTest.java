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
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Folding a split child into a node whose mask already holds the split bit.
 *
 * <p>
 * A node's mask is the union of the bits its branches discriminate on, so a bit one branch needs is
 * a zero column — off-path — for every child elsewhere, and a multi-value leaf there may hold keys
 * on both sides of it. Splitting such a leaf at that bit gives its 1-side half the slot's partial
 * with the bit's column set, and the fold inserts it at that partial's ascending position. When a
 * sibling discriminated by a <em>less</em> significant bit sits in between, that position is past
 * the sibling: the half carrying keys of the leaf's own range lands after a child whose keys all
 * sort above that range. The children are then no longer ordered by first key (I8), and the
 * sibling's keys, which carry the fold bit too, subset-match the inserted partial at a higher slot
 * and are routed away from their leaf. This is the shape {@code validatePublishedStructuralScope}
 * refused during a valid-time index load; the fold has to be declined, not published.
 * </p>
 *
 * <p>
 * Keys are one byte and the node routes on bits 0 ({@code 0x80}), 2 ({@code 0x20}) and 3
 * ({@code 0x10}): bit 2 is on-path only under bit 0's 1-side ({@code 0x80 | 0xa0}) and off-path for
 * the leaf {@code 00 05 0a 2e} and its sibling {@code 33 3d}, which bit 3 alone tells apart.
 * </p>
 */
final class HOTExistingBitFoldPlacementTest {

  private static final byte[] VALUE = {0x01};

  /** The bit the straddling leaf splits at: on both sides of it, {@code 0a} and {@code 2e}. */
  private static final int FOLD_BIT = 2;

  private static final int STRADDLING_SLOT = 0;

  @Test
  @DisplayName("a fold whose other half would land past a sibling is declined and refused")
  void foldPastASiblingIsDeclinedAndRefused() {
    final AtomicLong allocator = new AtomicLong(1);
    final Fixture fixture = fixture(allocator, true);
    HOTIncrementalInsert.BiNode halves = null;
    try {
      halves = HOTIncrementalInsert.splitLeafPage(fixture.straddling, key(0x0f), VALUE, 2, IndexType.VALIDTIME,
          allocator::getAndIncrement);
      assertEquals(FOLD_BIT, halves.discriminativeBitIndex(), "00 05 0a 0f | 2e splits at bit 2");
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();

      assertFalse(HOTIncrementalInsert.canMergeBiNodeAtExistingDiscBit(fixture.parent, FOLD_BIT, STRADDLING_SLOT),
          "partial 0b001 of the sibling sorts between the slot's 0b000 and the other half's 0b010");
      assertEquals(declinedBefore + 1, HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get(),
          "the decline must be counted as the placement refusal it is");

      final HOTIncrementalInsert.BiNode biNode = halves;
      assertThrows(IllegalArgumentException.class,
          () -> HOTIncrementalInsert.mergeBiNodeAtExistingDiscBit(fixture.parent, biNode, STRADDLING_SLOT, 2,
              allocator::getAndIncrement),
          "a fold that is not pre-checked must refuse too instead of returning a disordered node");
      assertThrows(IllegalArgumentException.class,
          () -> HOTIncrementalInsert.splitIndirectWithSlotReplaceAndInsertion(fixture.parent, STRADDLING_SLOT,
              biNode.left(), 0b010, biNode.right(), 2, allocator::getAndIncrement),
          "the full-node variant inserts at the same position and must refuse likewise");
    } finally {
      closeAll(fixture.parent, pageOf(halves, true), pageOf(halves, false));
    }
  }

  @Test
  @DisplayName("a fold whose other half lands beside its slot keeps the node ordered and every key routed")
  void foldBesideItsSlotStaysOrderedAndRouted() {
    final AtomicLong allocator = new AtomicLong(1);
    final Fixture fixture = fixture(allocator, false);
    HOTIncrementalInsert.BiNode halves = null;
    HOTIndirectPage folded = null;
    try {
      halves = HOTIncrementalInsert.splitLeafPage(fixture.straddling, key(0x0f), VALUE, 2, IndexType.VALIDTIME,
          allocator::getAndIncrement);
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();

      assertTrue(HOTIncrementalInsert.canMergeBiNodeAtExistingDiscBit(fixture.parent, FOLD_BIT, STRADDLING_SLOT),
          "with no sibling between 0b000 and 0b010 the other half lands right after its slot");
      assertEquals(declinedBefore, HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get());

      folded = HOTIncrementalInsert.mergeBiNodeAtExistingDiscBit(fixture.parent, halves, STRADDLING_SLOT, 2,
          allocator::getAndIncrement);

      assertEquals(4, folded.getNumChildren());
      assertSame(halves.left(), folded.getChildReference(0));
      assertSame(halves.right(), folded.getChildReference(1), "the 1-side half is the slot's right neighbour");
      assertOrderedAndRouted(folded);
    } finally {
      closeAll(fixture.parent, folded, pageOf(halves, true), pageOf(halves, false));
    }
  }

  @Test
  @DisplayName("the full-node variant folds beside the slot and splits into ordered, routed halves")
  void fullNodeVariantFoldsBesideItsSlot() {
    final AtomicLong allocator = new AtomicLong(1);
    final Fixture fixture = fixture(allocator, false);
    HOTIncrementalInsert.BiNode halves = null;
    HOTIncrementalInsert.BiNode nodeSplit = null;
    try {
      halves = HOTIncrementalInsert.splitLeafPage(fixture.straddling, key(0x0f), VALUE, 2, IndexType.VALIDTIME,
          allocator::getAndIncrement);

      nodeSplit = HOTIncrementalInsert.splitIndirectWithSlotReplaceAndInsertion(fixture.parent, STRADDLING_SLOT,
          halves.left(), 0b010, halves.right(), 2, allocator::getAndIncrement);

      assertEquals(0, nodeSplit.discriminativeBitIndex(), "the node splits at its own most significant bit");
      final HOTIndirectPage lower = (HOTIndirectPage) nodeSplit.left().getPage();
      assertEquals(2, lower.getNumChildren());
      assertSame(halves.left(), lower.getChildReference(0));
      assertSame(halves.right(), lower.getChildReference(1));
      assertOrderedAndRouted(lower);
      assertOrderedAndRouted((HOTIndirectPage) nodeSplit.right().getPage());
    } finally {
      closeAll(fixture.parent, pageOf(nodeSplit, true), pageOf(nodeSplit, false), pageOf(halves, true),
          pageOf(halves, false));
    }
  }

  // ===== Assertions =====

  /** I8/I12 and I6 over one node of leaves: ascending disjoint key ranges, every key routed home. */
  private static void assertOrderedAndRouted(final HOTIndirectPage node) {
    byte[] previousLast = null;
    for (int slot = 0; slot < node.getNumChildren(); slot++) {
      final HOTLeafPage leaf = (HOTLeafPage) node.getChildReference(slot).getPage();
      assertTrue(leaf.getEntryCount() > 0, "child " + slot + " must hold keys");
      if (previousLast != null) {
        assertTrue(Arrays.compareUnsigned(previousLast, leaf.getFirstKey()) < 0,
            "child " + slot + " must start after the preceding child's last key");
      }
      previousLast = leaf.getKey(leaf.getEntryCount() - 1);
      for (int entry = 0; entry < leaf.getEntryCount(); entry++) {
        assertEquals(slot, node.findChildIndex(leaf.getKey(entry)),
            "key " + String.format("%02x", leaf.getKey(entry)[0]) + " must route to the child that holds it");
      }
    }
  }

  // ===== Fixtures =====

  /**
   * {@code [00 05 0a 2e] [33 3d]? [80] [a0]} under the mask {@code {0, 2, 3}}. Bit 2 is in the mask
   * for {@code 80 | a0} alone; the first leaf straddles it, and {@code withSibling} adds the leaf bit
   * 3 tells apart from it — the one whose partial lies between the slot's and the other half's.
   */
  private static Fixture fixture(final AtomicLong allocator, final boolean withSibling) {
    final List<HOTLeafPage> leaves = new ArrayList<>(4);
    try {
      final HOTLeafPage straddling = leaf(allocator, leaves, 0x00, 0x05, 0x0a, 0x2e);
      if (withSibling) {
        leaf(allocator, leaves, 0x33, 0x3d);
      }
      leaf(allocator, leaves, 0x80);
      leaf(allocator, leaves, 0xa0);
      final PageReference[] references = new PageReference[leaves.size()];
      for (int i = 0; i < references.length; i++) {
        references[i] = swizzle(leaves.get(i));
      }
      final int[] partials = withSibling
          ? new int[] {0b000, 0b001, 0b100, 0b110}
          : new int[] {0b000, 0b100, 0b110};
      final HOTIndirectPage parent =
          HOTBulkBuilder.assembleIndirect(new int[] {0, 2, 3}, partials, references, 1, 1, allocator::getAndIncrement);
      assertOrderedAndRouted(parent);
      return new Fixture(parent, straddling);
    } catch (final RuntimeException | Error failure) {
      for (final HOTLeafPage leaf : leaves) {
        leaf.close();
      }
      throw failure;
    }
  }

  private record Fixture(HOTIndirectPage parent, HOTLeafPage straddling) {
  }

  private static HOTLeafPage leaf(final AtomicLong allocator, final List<HOTLeafPage> leaves, final int... keys) {
    final HOTLeafPage leaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    leaves.add(leaf);
    for (final int key : keys) {
      assertTrue(leaf.put(key(key), VALUE));
    }
    return leaf;
  }

  private static byte[] key(final int value) {
    return new byte[] {(byte) value};
  }

  private static Page pageOf(final HOTIncrementalInsert.BiNode biNode, final boolean left) {
    if (biNode == null) {
      return null;
    }
    return (left
        ? biNode.left()
        : biNode.right()).getPage();
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static void closeAll(final Page... roots) {
    final Set<HOTLeafPage> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final Page root : roots) {
      collectLeafPages(root, leaves);
    }
    for (final HOTLeafPage leaf : leaves) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
    }
  }

  private static void collectLeafPages(final Page page, final Set<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeafPages(reference.getPage(), out);
        }
      }
    }
  }
}
