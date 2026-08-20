/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TransactionIntentLogDocumentLocationTest {

  @Test
  void packedIdentityAlwaysResolvesTheCurrentSlotReplacement() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference reference = new PageReference();
      final KeyValueLeafPage original = documentPage();
      final PageContainer originalContainer = PageContainer.getInstance(original, original);
      log.put(reference, originalContainer);
      final long identity = originalContainer.getTransactionLogIdentity();

      assertEquals(0L, identity);
      assertSame(original, log.getAuthoritativeDocumentPage(identity));

      final KeyValueLeafPage replacement = documentPage();
      final PageContainer replacementContainer = PageContainer.getInstance(replacement, replacement);
      log.put(reference, replacementContainer);

      assertEquals(identity, replacementContainer.getTransactionLogIdentity(),
          "same-generation replacement retains the exact TIL slot identity");
      assertSame(replacement, log.getAuthoritativeDocumentPage(identity),
          "resolution must read the current container from the slot, never a cached page pointer");
    } finally {
      log.close();
    }
  }

  @Test
  void frozenIdentityMissesAndOverflowPromotionPublishesAPinnedIdentity() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference reference = new PageReference();
      final KeyValueLeafPage page = documentPage();
      final PageContainer container = PageContainer.getInstance(page, page);
      log.put(reference, container);
      final long frozenIdentity = container.getTransactionLogIdentity();

      assertEquals(1, log.snapshot());
      assertNull(log.getAuthoritativeDocumentPage(frozenIdentity),
          "a snapshot page must go through the ordinary CoW preparation path");

      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
      log.cleanupSnapshot();

      final long pinnedIdentity = container.getTransactionLogIdentity();
      assertNotEquals(frozenIdentity, pinnedIdentity);
      assertEquals(TransactionIntentLog.PINNED_GENERATION, (int) (pinnedIdentity >> 32));
      assertSame(page, log.getAuthoritativeDocumentPage(pinnedIdentity),
          "an overflow page retained for final recursive commit remains authoritative and mutable");

      final KeyValueLeafPage replacement = documentPage();
      final PageContainer replacementContainer = PageContainer.getInstance(replacement, replacement);
      log.put(reference, replacementContainer);
      assertEquals(pinnedIdentity, replacementContainer.getTransactionLogIdentity());
      assertSame(replacement, log.getAuthoritativeDocumentPage(pinnedIdentity),
          "pinned same-slot replacement must resolve to its newest modified page");
    } finally {
      log.close();
    }
  }

  @Test
  void wrongIndexClosedAndOrphanedPagesAreRejected() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference pathReference = new PageReference();
      final KeyValueLeafPage pathPage = page(IndexType.PATH);
      final PageContainer pathContainer = PageContainer.getInstance(pathPage, pathPage);
      log.put(pathReference, pathContainer);
      assertNull(log.getAuthoritativeDocumentPage(pathContainer.getTransactionLogIdentity()));

      final PageReference closedReference = new PageReference();
      final KeyValueLeafPage closedPage = documentPage();
      when(closedPage.isClosed()).thenReturn(true);
      final PageContainer closedContainer = PageContainer.getInstance(closedPage, closedPage);
      log.put(closedReference, closedContainer);
      assertNull(log.getAuthoritativeDocumentPage(closedContainer.getTransactionLogIdentity()));

      final PageReference orphanedReference = new PageReference();
      final KeyValueLeafPage orphanedPage = documentPage();
      when(orphanedPage.isOrphaned()).thenReturn(true);
      final PageContainer orphanedContainer = PageContainer.getInstance(orphanedPage, orphanedPage);
      log.put(orphanedReference, orphanedContainer);
      assertNull(log.getAuthoritativeDocumentPage(orphanedContainer.getTransactionLogIdentity()));
    } finally {
      log.close();
    }
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }

  private static KeyValueLeafPage documentPage() {
    return page(IndexType.DOCUMENT);
  }

  private static KeyValueLeafPage page(final IndexType indexType) {
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.getIndexType()).thenReturn(indexType);
    return page;
  }
}
