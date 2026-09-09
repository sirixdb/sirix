/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("HOT canonical-rebuild source preflight")
final class HOTRebuildFootprintTest {

  private static final byte[] VALUE = {1};

  @Test
  @DisplayName("whole-node failure diagnostics preserve the complete beta bit position")
  void wholeNodeDiagnosticBetaRoundTripsAllIntBits() {
    for (final int beta : new int[] {-1, 0, 0xFFFF, 0x1_0000, Integer.MAX_VALUE}) {
      final long proof = AbstractHOTIndexWriter.packWholeNodeProof(3, beta, 1, 2);
      assertEquals(beta, AbstractHOTIndexWriter.wholeNodeProofBeta(proof));
      assertEquals(3, (int) (proof & 0xFFL), "the status remains in the low byte");
      assertEquals(1, (int) ((proof >>> 40) & 0xFFL));
      assertEquals(2, (int) ((proof >>> 48) & 0xFFL));
    }
  }

  @Test
  @DisplayName("a small source is measured exactly without materializing entries")
  void measuresSmallSourceExactly() {
    final HOTBulkBuilder.BuildResult built = build(1_000);
    try {
      final AbstractHOTIndexWriter.RebuildFootprint footprint = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(
          built.rootPage(), PageReference::getPage, new AbstractHOTIndexWriter.RebuildFootprint());

      assertTrue(footprint.withinBudget());
      assertEquals(AbstractHOTIndexWriter.RebuildFootprintStatus.WITHIN_BUDGET, footprint.status());
      assertEquals(1_000, footprint.entries());
      assertTrue(footprint.pages() > 1, "the fixture must exercise an indirect source");
      assertTrue(footprint.materializedBytes() > 0);
    } finally {
      closeLeaves(built.rootPage());
    }
  }

  @Test
  @DisplayName("an oversized source stops before resolving page 64")
  void oversizedSourceHasBoundedResolutionWork() {
    final Page root = unaryPageChain(64, 10_000);
    final AtomicInteger resolvedReferences = new AtomicInteger();
    try {
      final AbstractHOTIndexWriter.RebuildFootprint footprint =
          AbstractHOTIndexWriter.measureBoundedRebuildFootprint(root, reference -> {
            resolvedReferences.incrementAndGet();
            return reference.getPage();
          }, new AbstractHOTIndexWriter.RebuildFootprint());

      assertFalse(footprint.withinBudget());
      assertEquals(AbstractHOTIndexWriter.RebuildFootprintStatus.PAGE_LIMIT, footprint.status());
      assertEquals(63, footprint.pages());
      assertTrue(resolvedReferences.get() <= 62,
          "the refusal must not resolve the first page beyond the fixed 63-page ceiling");
    } finally {
      closeLeaves(root);
    }
  }

  @Test
  @DisplayName("a repeated physical page is rejected rather than double-counted")
  void rejectsRepeatedPage() {
    final HOTLeafPage sharedLeaf = new HOTLeafPage(1, 1, IndexType.CAS);
    try {
      sharedLeaf.put(longKey(1), VALUE);
      final HOTIndirectPage root = HOTIndirectPage.createSpanNode(2, 1, 0, 1L << 56, new int[] {0, 1},
          new PageReference[] {reference(sharedLeaf), reference(sharedLeaf)}, 1);

      final AbstractHOTIndexWriter.RebuildFootprint footprint = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(
          root, PageReference::getPage, new AbstractHOTIndexWriter.RebuildFootprint());

      assertFalse(footprint.withinBudget());
      assertEquals(AbstractHOTIndexWriter.RebuildFootprintStatus.REPEATED_PAGE, footprint.status());
    } finally {
      sharedLeaf.close();
    }
  }

  @Test
  @DisplayName("mandatory mutation scans refuse an oversized source and poison the transaction")
  void oversizedMutationScansFailClosed() {
    final HOTBulkBuilder.BuildResult built = build(17_000, 0x6000_0000_0000_0000L);
    final HOTLeafPage unaffectedLeaf = leaf(50_000, twoByteKey(0x80, 0));
    final HOTLeafPage newLeaf = leaf(50_001, twoByteKey(0x40, 0));
    final PageReference oversizedReference = reference(built.rootPage());
    final PageReference unaffectedReference = reference(unaffectedLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(50_002, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {oversizedReference, unaffectedReference}, 3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(50_003, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {oversizedReference, reference(newLeaf), unaffectedReference}, 3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    final PageReference rootReference = reference(built.rootPage());
    try {
      final IllegalStateException routingRefusal = assertThrows(IllegalStateException.class,
          () -> writer.existingKeyRoutesToSlot(oldNode, newNode, 1, twoByteKey(0x40, 0)));
      assertTrue(routingRefusal.getMessage().contains("mandatory bounded mutation traversal"));
      assertTrue(routingRefusal.getMessage().contains("LEAF_LIMIT"));
      verify(storageEngineWriter).markTransactionRollbackOnly(routingRefusal);

      final IllegalStateException bitRefusal = assertThrows(IllegalStateException.class,
          () -> writer.subtreeHasKeyWithBit(rootReference, 63, 1, longKey(-1)));
      assertTrue(bitRefusal.getMessage().contains("mandatory bounded mutation traversal"));
      assertTrue(bitRefusal.getMessage().contains("LEAF_LIMIT"));
      verify(storageEngineWriter).markTransactionRollbackOnly(bitRefusal);
    } finally {
      closeLeaves(built.rootPage());
      unaffectedLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("ordered endpoints discharge an oversized whole-subtree bit guard")
  void oversizedWholeSubtreeBitGuardUsesBoundedEndpointProof() {
    final HOTBulkBuilder.BuildResult built = build(17_000, 0x6000_0000_0000_0000L);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      // Every stored key starts on bit-zero's 0 side; the excluded/new key is on its 1 side.
      // The source exceeds the exact-scan frontier, so success is evidence that the two bounded
      // extreme paths proved the answer without increasing any traversal/materialization limit.
      assertFalse(writer.subtreeHasKeyWithBit(reference(built.rootPage()), 0, 1, longKey(0xE000_0000_0000_0000L)));
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(built.rootPage());
    }
  }

  @Test
  @DisplayName("existing-key routing accepts a healthy 63-page candidate without charging its parent")
  void existingKeyRoutingAcceptsMaximumHealthyCandidateWithoutChargingParent() {
    final HOTIndirectPage candidateBlock = fullBinaryLeafTree(60_000);
    final HOTLeafPage unaffectedLeaf = leaf(61_000, twoByteKey(0x80, 0));
    final HOTLeafPage newLeaf = leaf(61_001, twoByteKey(0x40, 0x7f));
    final PageReference candidateReference = reference(candidateBlock);
    final PageReference unaffectedReference = reference(unaffectedLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(61_002, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {candidateReference, unaffectedReference}, 2);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(61_003, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {candidateReference, reference(newLeaf), unaffectedReference}, 2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      final AbstractHOTIndexWriter.RebuildFootprint candidate = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(
          candidateBlock, PageReference::getPage, new AbstractHOTIndexWriter.RebuildFootprint());
      assertTrue(candidate.withinBudget());
      assertEquals(63, candidate.pages());
      assertEquals(32, candidate.leaves());

      final AbstractHOTIndexWriter.RebuildFootprint wholeNode = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(
          oldNode, PageReference::getPage, new AbstractHOTIndexWriter.RebuildFootprint());
      assertEquals(AbstractHOTIndexWriter.RebuildFootprintStatus.PAGE_LIMIT, wholeNode.status(),
          "the unscanned parent is the 64th page and must not consume candidate budget");

      assertTrue(writer.existingKeyRoutesToSlot(oldNode, newNode, 1, twoByteKey(0x40, 0x7f)),
          "a candidate key in the maximum healthy bounded subtree must still be found exactly");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(candidateBlock);
      unaffectedLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("a later shadowing slot prunes an oversized irrelevant sibling")
  void existingKeyRoutingSkipsOversizedImpossibleSibling() {
    final HOTBulkBuilder.BuildResult oversized = build(17_000);
    final HOTLeafPage candidateLeaf = leaf(70_000, twoByteKey(0x00, 0));
    final HOTLeafPage newLeaf = leaf(70_001, twoByteKey(0x40, 0));
    final PageReference candidateReference = reference(candidateLeaf);
    final PageReference oversizedReference = reference(oversized.rootPage());
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(70_002, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {candidateReference, oversizedReference}, 3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(70_003, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {candidateReference, reference(newLeaf), oversizedReference}, 3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.existingKeyRoutesToSlot(oldNode, newNode, 1, twoByteKey(0x40, 0)),
          "partial 2 shadows the new partial 1 for every key from the oversized old slot");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      candidateLeaf.close();
      newLeaf.close();
      closeLeaves(oversized.rootPage());
    }
  }

  @Test
  @DisplayName("old-slot ownership prunes an oversized child that candidate routing alone permits")
  void existingKeyRoutingUsesOldOwnershipToSkipOversizedChild() {
    final HOTBulkBuilder.BuildResult oversized = build(17_000);
    final HOTLeafPage retainedLeaf = leaf(75_000, twoByteKey(0x80, 0));
    final HOTLeafPage newLeaf = leaf(75_001, twoByteKey(0xC0, 0));
    final PageReference oversizedReference = reference(oversized.rootPage());
    final PageReference retainedReference = reference(retainedLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(75_002, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {oversizedReference, retainedReference}, 3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(75_003, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 2, 3}, new PageReference[] {oversizedReference, retainedReference, reference(newLeaf)}, 3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.existingKeyRoutesToSlot(oldNode, newNode, 2, twoByteKey(0xC0, 0)),
          "candidate partial 3 alone permits the old partial-0 child, but old partial 1 proves that "
              + "no physical key owned by slot 0 can have candidate dense bits 3");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(oversized.rootPage());
      retainedLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("an out-of-width sparse partial disables pruning and preserves the exact routing answer")
  void existingKeyRoutingScansMalformedPartialWidth() {
    final byte[] existingKey = twoByteKey(0x80, 1);
    final byte[] newKey = twoByteKey(0x80, 0);
    final HOTLeafPage capturedLeaf = leaf(76_000, existingKey);
    final HOTLeafPage retainedLeaf = leaf(76_001, twoByteKey(0x00, 0));
    final HOTLeafPage newLeaf = leaf(76_002, newKey);
    final PageReference capturedReference = reference(capturedLeaf);
    final PageReference retainedReference = reference(retainedLeaf);
    final long oneBitMask = 0x8000_0000_0000_0000L;
    // Partial 2 sets a bit outside this one-bit node's packed mask. Before the width check, the
    // same-mask classifier treated the shape as proven and could prune old slot 0 even though its
    // physical key has dense value 1 and is captured by the fresh partial-1 slot.
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(76_003, 1, 0, oneBitMask, new int[] {0, 2},
        new PageReference[] {capturedReference, retainedReference}, 1);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(76_004, 1, 0, oneBitMask, new int[] {0, 1, 2},
        new PageReference[] {capturedReference, reference(newLeaf), retainedReference}, 1);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertEquals(0, oldNode.findChildIndex(existingKey), "the malformed old node physically owns the key");
      assertEquals(1, newNode.findChildIndex(existingKey), "the candidate fresh slot captures the old key");
      assertEquals(1, newNode.findChildIndex(newKey));
      assertTrue(writer.existingKeyRoutesToSlot(oldNode, newNode, 1, newKey),
          "an invalid partial width must force the bounded exact scan, never an optimistic prune");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      capturedLeaf.close();
      retainedLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("a beta-1 whole-node append is proven by bounded physical extrema")
  void wholeNodeEndpointProofAcceptsBetaOne() {
    final HOTIndirectPage lowerHalf = fullBinaryLeafTree(90_000, 0x00);
    final HOTIndirectPage upperHalf = fullBinaryLeafTree(91_000, 0x40);
    final HOTLeafPage newLeaf = leaf(92_000, twoByteKey(0x80, 0));
    final PageReference lowerReference = reference(lowerHalf);
    final PageReference upperReference = reference(upperHalf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(92_001, 1, 0, 0x4000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {lowerReference, upperReference}, 6);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(92_002, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {lowerReference, upperReference, reference(newLeaf)}, 6);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertTrue(writer.wholeNodeOneSidedOnAddedBit(oldNode, newNode, 2, twoByteKey(0x80, 0)));
      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, twoByteKey(0x80, 0)),
          "the two 32-leaf children exceed the scan budget, so success proves the endpoint fast path ran");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(lowerHalf);
      closeLeaves(upperHalf);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("a beta-0 whole-node prepend is proven by bounded physical extrema")
  void wholeNodeEndpointProofAcceptsBetaZero() {
    final HOTIndirectPage lowerHalf = fullBinaryLeafTree(93_000, 0x80);
    final HOTIndirectPage upperHalf = fullBinaryLeafTree(94_000, 0xC0);
    final HOTLeafPage newLeaf = leaf(95_000, twoByteKey(0x00, 0));
    final PageReference lowerReference = reference(lowerHalf);
    final PageReference upperReference = reference(upperHalf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(95_001, 1, 0, 0x4000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {lowerReference, upperReference}, 6);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(95_002, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 2, 3}, new PageReference[] {reference(newLeaf), lowerReference, upperReference}, 6);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertTrue(writer.wholeNodeOneSidedOnAddedBit(oldNode, newNode, 0, twoByteKey(0x00, 0)));
      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, twoByteKey(0x00, 0)));
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(lowerHalf);
      closeLeaves(upperHalf);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("an interior added bit skips the preserved 9,895-entry opposite-side source")
  void interiorAddedBitSkipsPreservedOversizedOppositeSideSource() {
    final HOTLeafPage oldSlotZero = leaf(100_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(100_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTIndirectPage oldSlotTwo = flatStructuralOrderSource(100_100, 9_895, 0, 0);
    final HOTIndirectPage oldSlotThree = twoLeafStructuralOrderSource(100_200, 0);
    final byte[] newKey = structuralOrderKey(1, 1, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(100_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final long oldMask = (1L << (63 - 13)) | (1L << (63 - 43));
    final long newMask = oldMask | (1L << (63 - 42));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(100_400, 1, 0, oldMask, new int[] {0, 1, 2, 3},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference}, 2);
    final HOTIndirectPage newNode = HOTIndirectPage
                                                   .createSpanNode(100_401, 1, 0, newMask, new int[] {0, 1, 4, 5, 6},
                                                       new PageReference[] {slotZeroReference, slotOneReference,
                                                           slotTwoReference, slotThreeReference, reference(newLeaf)},
                                                       2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertArrayEquals(new int[] {13, 43}, HOTIncrementalInsert.discriminativeBits(oldNode));
      assertArrayEquals(new int[] {13, 42, 43}, HOTIncrementalInsert.discriminativeBits(newNode));
      assertEquals(2, oldNode.findChildIndex(newKey));
      assertEquals(4, newNode.findChildIndex(newKey));

      final AbstractHOTIndexWriter.RebuildFootprint preservedSource =
          AbstractHOTIndexWriter.measureBoundedRebuildFootprint(oldSlotTwo, PageReference::getPage,
              new AbstractHOTIndexWriter.RebuildFootprint());
      assertTrue(preservedSource.withinBudget());
      assertEquals(33, preservedSource.pages());
      assertEquals(32, preservedSource.leaves());
      assertEquals(9_895, preservedSource.entries());

      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, newKey),
          "the new interior bit is opposite in both physical extrema of every feasible old child; "
              + "the 32-leaf source must be discharged structurally instead of refused at LEAF_LIMIT");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      closeLeaves(oldSlotTwo);
      closeLeaves(oldSlotThree);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("a same-mask fresh combination skips retained opposite-side ranges")
  void sameMaskCombinationSkipsOversizedOppositeSideSources() {
    final HOTLeafPage oldSlotZero = leaf(104_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(104_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTLeafPage oldSlotTwo = leaf(104_002, structuralOrderKey(0, 1, 0, 0, 0));
    final HOTLeafPage oldSlotThree = leaf(104_003, structuralOrderKey(0, 1, 1, 0, 0));
    final HOTIndirectPage oldSlotFour = flatStructuralOrderSource(104_100, 9_895, 0, 0);
    final HOTIndirectPage oldSlotFive = twoLeafStructuralOrderSource(104_200, 0);
    final byte[] newKey = structuralOrderKey(1, 1, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(104_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final PageReference slotFourReference = reference(oldSlotFour);
    final PageReference slotFiveReference = reference(oldSlotFive);
    final long mask = (1L << (63 - 13)) | (1L << (63 - 42)) | (1L << (63 - 43));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(104_400, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference},
        2);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(104_401, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5, 6},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference, reference(newLeaf)},
        2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertArrayEquals(new int[] {13, 42, 43}, HOTIncrementalInsert.discriminativeBits(oldNode));
      assertArrayEquals(new int[] {13, 42, 43}, HOTIncrementalInsert.discriminativeBits(newNode));
      assertEquals(4, oldNode.findChildIndex(newKey));
      assertEquals(6, newNode.findChildIndex(newKey));

      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, newKey),
          "fresh partial 6 requires beta42=1, while both physical extrema of retained partials 4 and 5 "
              + "prove beta42=0; neither oversized source may be scanned");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      oldSlotTwo.close();
      oldSlotThree.close();
      closeLeaves(oldSlotFour);
      closeLeaves(oldSlotFive);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("a mixed same-mask source is scanned and its stranded key is found")
  void sameMaskCombinationMixedRangeFallsThroughToExactScan() {
    final HOTLeafPage oldSlotZero = leaf(105_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(105_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTLeafPage oldSlotTwo = leaf(105_002, structuralOrderKey(0, 1, 0, 0, 0));
    final HOTLeafPage oldSlotThree = leaf(105_003, structuralOrderKey(0, 1, 1, 0, 0));
    final HOTIndirectPage oldSlotFour = flatStructuralOrderSource(105_100, 32, 0, 1);
    final HOTIndirectPage oldSlotFive = twoLeafStructuralOrderSource(105_200, 0);
    final byte[] newKey = structuralOrderKey(1, 1, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(105_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final PageReference slotFourReference = reference(oldSlotFour);
    final PageReference slotFiveReference = reference(oldSlotFive);
    final long mask = (1L << (63 - 13)) | (1L << (63 - 42)) | (1L << (63 - 43));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(105_400, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference},
        2);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(105_401, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5, 6},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference, reference(newLeaf)},
        2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertTrue(writer.branchAddStrandsExisting(oldNode, newNode, newKey),
          "the last leaf crosses onto beta42=1 and contains a key captured by fresh partial 6");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      oldSlotTwo.close();
      oldSlotThree.close();
      closeLeaves(oldSlotFour);
      closeLeaves(oldSlotFive);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("strand confinement ignores an oversized route-infeasible sibling")
  void strandConfinementUsesTheRouteFeasiblePagePlan() {
    final HOTLeafPage oldSlotZero = leaf(106_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(106_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTLeafPage oldSlotTwo = leaf(106_002, structuralOrderKey(0, 1, 0, 0, 0));
    final HOTLeafPage oldSlotThree = leaf(106_003, structuralOrderKey(0, 1, 1, 0, 0));
    final HOTIndirectPage oldSlotFour = thirtyThreeLeafStructuralOrderSource(106_100, 0, 0);
    final HOTLeafPage oldSlotFive = leaf(106_200, structuralOrderKey(1, 1, 1, 0, 0));
    final byte[] newKey = structuralOrderKey(1, 1, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(106_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final PageReference slotFourReference = reference(oldSlotFour);
    final PageReference slotFiveReference = reference(oldSlotFive);
    final long mask = (1L << (63 - 13)) | (1L << (63 - 42)) | (1L << (63 - 43));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(106_400, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference},
        3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(106_401, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5, 6},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference, reference(newLeaf)},
        3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertTrue(writer.branchAddStrandsExisting(oldNode, newNode, newKey));
      assertTrue(writer.strandConfinedToLeaf(oldNode, newNode, 6, newKey, oldSlotFive.getPageKey()),
          "the 33-leaf beta-opposite sibling cannot route fresh; the only captured key is in slot 5's leaf");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      oldSlotTwo.close();
      oldSlotThree.close();
      closeLeaves(oldSlotFour);
      oldSlotFive.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("strand confinement refuses an oversized mixed route-relevant source")
  void strandConfinementMixedRelevantSourceRemainsFailClosed() {
    final HOTLeafPage oldSlotZero = leaf(107_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(107_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTLeafPage oldSlotTwo = leaf(107_002, structuralOrderKey(0, 1, 0, 0, 0));
    final HOTLeafPage oldSlotThree = leaf(107_003, structuralOrderKey(0, 1, 1, 0, 0));
    final HOTIndirectPage oldSlotFour = thirtyThreeLeafStructuralOrderSource(107_100, 0, 1, 0);
    final HOTLeafPage oldSlotFive = leaf(107_200, structuralOrderKey(1, 0, 1, 0, 0));
    final byte[] newKey = structuralOrderKeyWithSuffix(1, 1, 0, 0xFF, 0xFF);
    final HOTLeafPage newLeaf = leaf(107_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final PageReference slotFourReference = reference(oldSlotFour);
    final PageReference slotFiveReference = reference(oldSlotFive);
    final long mask = (1L << (63 - 13)) | (1L << (63 - 42)) | (1L << (63 - 43));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(107_400, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference},
        3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(107_401, 1, 0, mask, new int[] {0, 1, 2, 3, 4, 5, 6},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference,
            slotFourReference, slotFiveReference, reference(newLeaf)},
        3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      final IllegalStateException refusal = assertThrows(IllegalStateException.class,
          () -> writer.strandConfinedToLeaf(oldNode, newNode, 6, newKey, oldSlotFive.getPageKey()));
      assertTrue(refusal.getMessage().contains("status=LEAF_LIMIT pages=35/63 leaves=32/32"));
      verify(storageEngineWriter).markTransactionRollbackOnly(refusal);
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      oldSlotTwo.close();
      oldSlotThree.close();
      closeLeaves(oldSlotFour);
      oldSlotFive.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("an interior zero bit is shadowed by the retained later beta-one children")
  void interiorAddedZeroBitSkipsOversizedShadowedSource() {
    final HOTLeafPage oldSlotZero = leaf(101_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(101_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTIndirectPage oldSlotTwo = flatStructuralOrderSource(101_100, 32, 1, 1);
    final HOTIndirectPage oldSlotThree = twoLeafStructuralOrderSource(101_200, 1);
    final byte[] newKey = structuralOrderKey(1, 0, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(101_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final long oldMask = (1L << (63 - 13)) | (1L << (63 - 43));
    final long newMask = oldMask | (1L << (63 - 42));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(101_400, 1, 0, oldMask, new int[] {0, 1, 2, 3},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference}, 2);
    final HOTIndirectPage newNode = HOTIndirectPage
                                                   .createSpanNode(101_401, 1, 0, newMask, new int[] {0, 1, 4, 6, 7},
                                                       new PageReference[] {slotZeroReference, slotOneReference,
                                                           reference(newLeaf), slotTwoReference, slotThreeReference},
                                                       2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertArrayEquals(new int[] {13, 43}, HOTIncrementalInsert.discriminativeBits(oldNode));
      assertArrayEquals(new int[] {13, 42, 43}, HOTIncrementalInsert.discriminativeBits(newNode));
      assertEquals(2, oldNode.findChildIndex(newKey));
      assertEquals(2, newNode.findChildIndex(newKey));

      final AbstractHOTIndexWriter.RebuildFootprint source = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(
          oldSlotTwo, PageReference::getPage, new AbstractHOTIndexWriter.RebuildFootprint());
      assertEquals(33, source.pages());
      assertEquals(32, source.leaves());

      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, newKey),
          "for beta=0 the retained beta=1 partials 6 and 7 are later strict supersets of fresh partial 4");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      closeLeaves(oldSlotTwo);
      closeLeaves(oldSlotThree);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("beta-opposite extrema do not bypass a retained partial that cannot shadow")
  void interiorAddedZeroBitNonShadowingSourceRemainsFailClosed() {
    final HOTLeafPage oldSlotZero = leaf(103_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTIndirectPage oldSlotOne = thirtyThreeLeafStructuralOrderSource(103_100, 1, 0);
    final byte[] newKey = structuralOrderKey(1, 0, 1, 0, 0);
    final HOTLeafPage newLeaf = leaf(103_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final long oldMask = (1L << (63 - 13)) | (1L << (63 - 43));
    final long newMask = oldMask | (1L << (63 - 42));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(103_400, 1, 0, oldMask, new int[] {0, 2},
        new PageReference[] {slotZeroReference, slotOneReference}, 3);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(103_401, 1, 0, newMask, new int[] {0, 5, 6},
        new PageReference[] {slotZeroReference, reference(newLeaf), slotOneReference}, 3);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertEquals(1, oldNode.findChildIndex(newKey));
      assertEquals(1, newNode.findChildIndex(newKey));
      assertEquals(2, newNode.findChildIndex(structuralOrderKey(1, 1, 0, 0, 0)));

      final IllegalStateException refusal =
          assertThrows(IllegalStateException.class, () -> writer.branchAddStrandsExisting(oldNode, newNode, newKey));
      assertTrue(refusal.getMessage().contains("status=LEAF_LIMIT pages=35/63 leaves=32/32 entries=32/16384"));
      verify(storageEngineWriter).markTransactionRollbackOnly(refusal);
    } finally {
      oldSlotZero.close();
      closeLeaves(oldSlotOne);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("mixed interior-bit extrema fall through to the bounded fail-closed refusal")
  void interiorAddedBitMixedExtremaRemainFailClosed() {
    final HOTLeafPage oldSlotZero = leaf(102_000, structuralOrderKey(0, 0, 0, 0, 0));
    final HOTLeafPage oldSlotOne = leaf(102_001, structuralOrderKey(0, 0, 1, 0, 0));
    final HOTIndirectPage oldSlotTwo = flatStructuralOrderSource(102_100, 32, 0, 1);
    final HOTIndirectPage oldSlotThree = twoLeafStructuralOrderSource(102_200, 1);
    final byte[] newKey = structuralOrderKey(1, 1, 0, 0, 0);
    final HOTLeafPage newLeaf = leaf(102_300, newKey);
    final PageReference slotZeroReference = reference(oldSlotZero);
    final PageReference slotOneReference = reference(oldSlotOne);
    final PageReference slotTwoReference = reference(oldSlotTwo);
    final PageReference slotThreeReference = reference(oldSlotThree);
    final long oldMask = (1L << (63 - 13)) | (1L << (63 - 43));
    final long newMask = oldMask | (1L << (63 - 42));
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(102_400, 1, 0, oldMask, new int[] {0, 1, 2, 3},
        new PageReference[] {slotZeroReference, slotOneReference, slotTwoReference, slotThreeReference}, 2);
    final HOTIndirectPage newNode = HOTIndirectPage
                                                   .createSpanNode(102_401, 1, 0, newMask, new int[] {0, 1, 4, 5, 6},
                                                       new PageReference[] {slotZeroReference, slotOneReference,
                                                           slotTwoReference, slotThreeReference, reference(newLeaf)},
                                                       2);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      final IllegalStateException refusal =
          assertThrows(IllegalStateException.class, () -> writer.branchAddStrandsExisting(oldNode, newNode, newKey));
      assertTrue(refusal.getMessage().contains("status=LEAF_LIMIT pages=35/63 leaves=32/32 entries=32/16384"));
      verify(storageEngineWriter).markTransactionRollbackOnly(refusal);
    } finally {
      oldSlotZero.close();
      oldSlotOne.close();
      closeLeaves(oldSlotTwo);
      closeLeaves(oldSlotThree);
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("mixed endpoints fail open and the exact scan finds the stranded key")
  void wholeNodeEndpointProofRejectsMixedRange() {
    final HOTLeafPage firstLeaf = leaf(96_000, twoByteKey(0x00, 0));
    final HOTLeafPage lastLeaf = leaf(96_001, twoByteKey(0xC0, 0));
    final HOTLeafPage newLeaf = leaf(96_002, twoByteKey(0x80, 0));
    final PageReference firstReference = reference(firstLeaf);
    final PageReference lastReference = reference(lastLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(96_003, 1, 0, 0x4000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {firstReference, lastReference}, 1);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(96_004, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {firstReference, lastReference, reference(newLeaf)}, 1);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.wholeNodeOneSidedOnAddedBit(oldNode, newNode, 2, twoByteKey(0x80, 0)));
      assertTrue(writer.branchAddStrandsExisting(oldNode, newNode, twoByteKey(0x80, 0)));
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      firstLeaf.close();
      lastLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("an endpoint prefix mismatch cannot prove whole-node beta constancy")
  void wholeNodeEndpointProofRejectsPrefixMismatch() {
    final HOTLeafPage firstLeaf = leaf(97_000, twoByteKey(0x80, 0));
    final HOTLeafPage lastLeaf = leaf(97_001, twoByteKey(0xA0, 0));
    final HOTLeafPage newLeaf = leaf(97_002, twoByteKey(0x40, 0));
    final PageReference firstReference = reference(firstLeaf);
    final PageReference lastReference = reference(lastLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(97_003, 1, 0, 0x2000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {firstReference, lastReference}, 1);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(97_004, 1, 0, 0x6000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {firstReference, lastReference, reference(newLeaf)}, 1);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.wholeNodeOneSidedOnAddedBit(oldNode, newNode, 2, twoByteKey(0x40, 0)));
      assertFalse(writer.branchAddStrandsExisting(oldNode, newNode, twoByteKey(0x40, 0)),
          "prefix uncertainty must fall through to the exact bounded scan");
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      firstLeaf.close();
      lastLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("candidate reference reordering refuses instead of using the endpoint proof")
  void wholeNodeEndpointProofRejectsMalformedCandidateMapping() {
    final HOTLeafPage firstLeaf = leaf(98_000, twoByteKey(0x00, 0));
    final HOTLeafPage lastLeaf = leaf(98_001, twoByteKey(0x40, 0));
    final HOTLeafPage newLeaf = leaf(98_002, twoByteKey(0x80, 0));
    final PageReference firstReference = reference(firstLeaf);
    final PageReference lastReference = reference(lastLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(98_003, 1, 0, 0x4000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {firstReference, lastReference}, 1);
    final HOTIndirectPage malformed = HOTIndirectPage.createSpanNode(98_004, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {lastReference, firstReference, reference(newLeaf)}, 1);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.wholeNodeOneSidedOnAddedBit(oldNode, malformed, 2, twoByteKey(0x80, 0)));
      final IllegalStateException refusal = assertThrows(IllegalStateException.class,
          () -> writer.branchAddStrandsExisting(oldNode, malformed, twoByteKey(0x80, 0)));
      assertTrue(refusal.getMessage().contains("does not preserve old child"));
      verify(storageEngineWriter).markTransactionRollbackOnly(refusal);
    } finally {
      firstLeaf.close();
      lastLeaf.close();
      newLeaf.close();
    }
  }

  @Test
  @DisplayName("cyclic and over-depth extrema fail open to the bounded refusal")
  void wholeNodeEndpointProofRejectsCyclicAndOverDepthSources() {
    assertMalformedEndpointSourceRefused(cyclicPage(99_000), 99_100);
    final Page overDepth = unaryPageChain(65, 99_200, twoByteKey(0x00, 0));
    try {
      assertMalformedEndpointSourceRefused(overDepth, 99_300);
    } finally {
      closeLeaves(overDepth);
    }
  }

  @Test
  @DisplayName("bounded mutation scans preserve their routing and bit answers")
  void boundedMutationScansPreserveAnswers() {
    final HOTBulkBuilder.BuildResult built = build(1_000);
    final HOTLeafPage candidateLeaf = leaf(80_000, twoByteKey(0x40, 1));
    final HOTLeafPage unaffectedLeaf = leaf(80_001, twoByteKey(0x80, 0));
    final HOTLeafPage newLeaf = leaf(80_002, twoByteKey(0x40, 0x7f));
    final PageReference candidateReference = reference(candidateLeaf);
    final PageReference unaffectedReference = reference(unaffectedLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(80_003, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {candidateReference, unaffectedReference}, 1);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(80_004, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {candidateReference, reference(newLeaf), unaffectedReference}, 1);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    final PageReference rootReference = reference(built.rootPage());
    try {
      assertTrue(writer.existingKeyRoutesToSlot(oldNode, newNode, 1, twoByteKey(0x40, 0x7f)));
      assertTrue(writer.subtreeHasKeyWithBit(rootReference, 63, 1, longKey(-1)));
      verify(storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeLeaves(built.rootPage());
      candidateLeaf.close();
      unaffectedLeaf.close();
      newLeaf.close();
    }
  }

  private static HOTBulkBuilder.BuildResult build(final int entryCount) {
    return build(entryCount, 0L);
  }

  private static HOTBulkBuilder.BuildResult build(final int entryCount, final long highPrefix) {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(entryCount);
    for (int i = 0; i < entryCount; i++) {
      entries.add(new HOTBulkBuilder.Entry(longKey(highPrefix | Integer.toUnsignedLong(i)), VALUE));
    }
    return HOTBulkBuilder.build(entries, 1, IndexType.CAS, new AtomicLong(1)::getAndIncrement);
  }

  private static byte[] longKey(final long value) {
    final byte[] key = new byte[Long.BYTES];
    key[0] = (byte) (value >>> 56);
    key[1] = (byte) (value >>> 48);
    key[2] = (byte) (value >>> 40);
    key[3] = (byte) (value >>> 32);
    key[4] = (byte) (value >>> 24);
    key[5] = (byte) (value >>> 16);
    key[6] = (byte) (value >>> 8);
    key[7] = (byte) value;
    return key;
  }

  private static byte[] twoByteKey(final int firstByte, final int secondByte) {
    return new byte[] {(byte) firstByte, (byte) secondByte};
  }

  /** Keys matching the preserved [13, 42, 43] structural-order branch-add diagnostic. */
  private static byte[] structuralOrderKey(final int bit13, final int bit42, final int bit43, final int leafOrdinal,
      final int entryOrdinal) {
    return structuralOrderKeyWithSuffix(bit13, bit42, bit43, (leafOrdinal << 3) | (entryOrdinal >>> 8), entryOrdinal);
  }

  private static byte[] structuralOrderKeyWithSuffix(final int bit13, final int bit42, final int bit43,
      final int penultimateByte, final int lastByte) {
    return new byte[] {0, (byte) (bit13 << 2), 0, 0, 0, (byte) ((bit42 << 5) | (bit43 << 4)), (byte) penultimateByte,
        (byte) lastByte};
  }

  /** A flat 33-page/32-leaf source; the corpus fixture supplies its reported 9,895 entries. */
  private static HOTIndirectPage flatStructuralOrderSource(final long firstPageKey, final int entryCount,
      final int firstLeavesBit42, final int lastLeafBit42) {
    final int leafCount = HOTIndirectPage.MAX_NODE_ENTRIES;
    final int baseEntriesPerLeaf = entryCount / leafCount;
    final int remainder = entryCount % leafCount;
    final PageReference[] leaves = new PageReference[leafCount];
    final int[] partials = new int[leafCount];
    for (int leafOrdinal = 0; leafOrdinal < leafCount; leafOrdinal++) {
      final HOTLeafPage leaf = new HOTLeafPage(firstPageKey + leafOrdinal, 1, IndexType.CAS);
      final int entriesInLeaf = baseEntriesPerLeaf + (leafOrdinal < remainder
          ? 1
          : 0);
      final int bit42 = leafOrdinal == leafCount - 1
          ? lastLeafBit42
          : firstLeavesBit42;
      for (int entryOrdinal = 0; entryOrdinal < entriesInLeaf; entryOrdinal++) {
        assertTrue(leaf.put(structuralOrderKey(1, bit42, 0, leafOrdinal, entryOrdinal), VALUE));
      }
      leaves[leafOrdinal] = reference(leaf);
      partials[leafOrdinal] = leafOrdinal;
    }
    return HOTIndirectPage.createMultiNode(firstPageKey + leafCount, 1, 6, 0xF800_0000_0000_0000L, partials, leaves, 1);
  }

  /** A valid two-level source that crosses the fixed 32-leaf traversal ceiling by one leaf. */
  private static HOTIndirectPage thirtyThreeLeafStructuralOrderSource(final long firstPageKey, final int bit42,
      final int bit43) {
    return thirtyThreeLeafStructuralOrderSource(firstPageKey, bit42, bit42, bit43);
  }

  private static HOTIndirectPage thirtyThreeLeafStructuralOrderSource(final long firstPageKey,
      final int firstLeavesBit42, final int lastLeafBit42, final int bit43) {
    final int leftLeafCount = HOTIndirectPage.MAX_NODE_ENTRIES;
    final PageReference[] leftLeaves = new PageReference[leftLeafCount];
    final int[] partials = new int[leftLeafCount];
    for (int leafOrdinal = 0; leafOrdinal < leftLeafCount; leafOrdinal++) {
      leftLeaves[leafOrdinal] = reference(leaf(firstPageKey + leafOrdinal,
          structuralOrderKeyWithSuffix(1, firstLeavesBit42, bit43, leafOrdinal << 2, 0)));
      partials[leafOrdinal] = leafOrdinal;
    }
    final HOTIndirectPage left = HOTIndirectPage.createMultiNode(firstPageKey + leftLeafCount, 1, 6,
        0x7C00_0000_0000_0000L, partials, leftLeaves, 1);
    final HOTLeafPage right =
        leaf(firstPageKey + leftLeafCount + 1, structuralOrderKeyWithSuffix(1, lastLeafBit42, bit43, 0x80, 0));
    return HOTIndirectPage.createBiNode(firstPageKey + leftLeafCount + 2, 1, 48, reference(left), reference(right), 2);
  }

  /** The second feasible child has an indirect root, reproducing the preserved page=35 refusal. */
  private static HOTIndirectPage twoLeafStructuralOrderSource(final long firstPageKey, final int bit42) {
    final HOTLeafPage left = leaf(firstPageKey, structuralOrderKey(1, bit42, 1, 0, 0));
    final HOTLeafPage right = leaf(firstPageKey + 1, structuralOrderKey(1, bit42, 1, 1, 0));
    return HOTIndirectPage.createBiNode(firstPageKey + 2, 1, 52, reference(left), reference(right), 1);
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[] key) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.CAS);
    assertTrue(leaf.put(key, VALUE));
    return leaf;
  }

  private static void assertMalformedEndpointSourceRefused(final Page malformedFirstChild, final long firstPageKey) {
    final HOTLeafPage lastLeaf = leaf(firstPageKey, twoByteKey(0x40, 0));
    final HOTLeafPage newLeaf = leaf(firstPageKey + 1, twoByteKey(0x80, 0));
    final PageReference malformedReference = reference(malformedFirstChild);
    final PageReference lastReference = reference(lastLeaf);
    final HOTIndirectPage oldNode = HOTIndirectPage.createSpanNode(firstPageKey + 2, 1, 0, 0x4000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {malformedReference, lastReference}, 66);
    final HOTIndirectPage newNode = HOTIndirectPage.createSpanNode(firstPageKey + 3, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 1, 2}, new PageReference[] {malformedReference, lastReference, reference(newLeaf)}, 66);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(mock(TransactionIntentLog.class));
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    try {
      assertFalse(writer.wholeNodeOneSidedOnAddedBit(oldNode, newNode, 2, twoByteKey(0x80, 0)));
      final IllegalStateException refusal = assertThrows(IllegalStateException.class,
          () -> writer.branchAddStrandsExisting(oldNode, newNode, twoByteKey(0x80, 0)));
      assertTrue(refusal.getMessage().contains("mandatory bounded mutation traversal"));
      verify(storageEngineWriter).markTransactionRollbackOnly(refusal);
    } finally {
      lastLeaf.close();
      newLeaf.close();
    }
  }

  /** A valid binary HOT with 32 leaves and 31 indirects: the healthy 2L-1 maximum of 63 pages. */
  private static HOTIndirectPage fullBinaryLeafTree(final long firstPageKey) {
    return fullBinaryLeafTree(firstPageKey, 0x40);
  }

  private static HOTIndirectPage fullBinaryLeafTree(final long firstPageKey, final int baseFirstByte) {
    return (HOTIndirectPage) fullBinaryLeafTree(0, 0, baseFirstByte, new AtomicLong(firstPageKey));
  }

  private static Page fullBinaryLeafTree(final int depth, final int prefix, final int baseFirstByte,
      final AtomicLong pageKeys) {
    if (depth == 5) {
      return leaf(pageKeys.getAndIncrement(), twoByteKey(baseFirstByte | prefix, 0));
    }
    final int keyBit = 1 << (4 - depth);
    final PageReference left = reference(fullBinaryLeafTree(depth + 1, prefix, baseFirstByte, pageKeys));
    final PageReference right = reference(fullBinaryLeafTree(depth + 1, prefix | keyBit, baseFirstByte, pageKeys));
    final int absoluteBit = 3 + depth;
    return HOTIndirectPage.createSpanNode(pageKeys.getAndIncrement(), 1, 0, 1L << (63 - absoluteBit), new int[] {0, 1},
        new PageReference[] {left, right}, 5 - depth);
  }

  /** A corrupt/unary shape proving the independent physical cap, even when the leaf bound is idle. */
  private static Page unaryPageChain(final int pageCount, final long firstPageKey) {
    return unaryPageChain(pageCount, firstPageKey, longKey(1));
  }

  private static Page unaryPageChain(final int pageCount, final long firstPageKey, final byte[] key) {
    Page page = leaf(firstPageKey, key);
    for (int i = 1; i < pageCount; i++) {
      page = HOTIndirectPage.createMultiNode(firstPageKey + i, 1, 0, 1L << 56, new int[] {0},
          new PageReference[] {reference(page)}, i);
    }
    return page;
  }

  private static HOTIndirectPage cyclicPage(final long pageKey) {
    final PageReference self = new PageReference();
    final HOTIndirectPage cycle =
        HOTIndirectPage.createMultiNode(pageKey, 1, 0, 1L << 56, new int[] {0}, new PageReference[] {self}, 1);
    self.setPage(cycle);
    return cycle;
  }

  private static PageReference reference(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static void closeLeaves(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      leaf.close();
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference != null && childReference.getPage() != null) {
          closeLeaves(childReference.getPage());
        }
      }
    }
  }

  private static final class TestIndexWriter extends AbstractHOTIndexWriter<Long> {
    private byte[] keyBuffer = new byte[Long.BYTES];

    private TestIndexWriter(final StorageEngineWriter storageEngineWriter) {
      super(storageEngineWriter, IndexType.PATH, 0);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] newBuffer) {
      keyBuffer = newBuffer;
    }

    @Override
    protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
      return 0;
    }

    @Override
    protected void prepareIndexPage() {
      // The bounded traversal probes neither allocate persistent page keys nor mutate index pages.
    }
  }
}
