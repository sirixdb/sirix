/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PinnedTrieSpillEligibilityTest {

  @Test
  void indirectPageWaitsForEveryLiveChildAndRefreshesCompletedHandle() {
    final IndirectPage page = new IndirectPage();
    final PageReference child = page.getOrCreateReference(7);
    child.bindToTransactionLog(3, 1, false);

    assertFalse(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));

    final PageReference.TransactionLogReference handle = child.transactionLogReference();
    assertTrue(PageReference.completeTransactionLogReference(handle, 4_096L, 0x0102_0304_0506_0708L));
    assertTrue(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));
    assertEquals(4_096L, child.getKey());
    assertEquals(Constants.NULL_ID_INT, child.getLogKey());
    assertNull(child.transactionLogReference());
  }

  @Test
  void copiedFullIndirectPageIgnoresOnlyItsVirginMaterializedSlots() {
    final BitmapReferencesPage sparse = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    sparse.getOrCreateReference(17).setKey(4_096L);
    final FullReferencesPage full = new FullReferencesPage(sparse);
    final IndirectPage copied = new IndirectPage(new FullReferencesPage(full));

    assertTrue(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(copied),
        "the 1,023 virgin placeholders in a copied full delegate are not live children");

    copied.getOrCreateReference(18).setPage(new OverflowPage(new byte[] {7}));
    assertFalse(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(copied),
        "an unresolved in-memory page is not a virgin placeholder and must block publication");
  }

  @Test
  void hotIndirectPageRejectsOneActiveChildAmongDurableSiblings() {
    final PageReference left = new PageReference().setKey(8_192L);
    final PageReference right = new PageReference().setKey(12_288L);
    final HOTIndirectPage page = HOTIndirectPage.createBiNode(1, 1, 0, left, right);
    assertTrue(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));

    right.bindToTransactionLog(0, 2, false);
    assertFalse(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));
  }

  @Test
  void hotLeafWaitsForPendingSideReferencePublication() {
    final HOTLeafPage page = new HOTLeafPage(0, 1, IndexType.CAS);
    try {
      final PageReference sideReference = new PageReference();
      final OverflowPage sidePage = new OverflowPage(new byte[] {4, 5, 6});
      sideReference.setPage(sidePage);
      sideReference.bindPendingPageWrite(sidePage);
      page.setPageReference(9, sideReference);

      assertFalse(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));

      sideReference.completePendingPageWrite(16_384L);
      assertEquals(1, page.segmentRefCount());
      assertFalse(page.isClosed());
      assertEquals(16_384L, sideReference.getKey());
      assertEquals(Constants.NULL_ID_INT, sideReference.getLogKey());
      assertEquals(-1, sideReference.getActiveTilGeneration());
      assertNull(sideReference.transactionLogReference());
      assertTrue(sideReference.refreshesToUnclaimedDurableReference());
      assertTrue(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(page));
      assertFalse(sideReference.hasPendingPageWrite());
    } finally {
      page.close();
    }
  }
}
