/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.IndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class TransactionIntentLogPinnedSpillTest {

  @Test
  void tombstoneKeepsDenseViewAndNeverReusesNumericSlot() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference[] references = new PageReference[4];
      final PageContainer[] containers = new PageContainer[4];
      for (int i = 0; i < references.length; i++) {
        references[i] = new PageReference();
        final IndirectPage page = new IndirectPage();
        containers[i] = PageContainer.getInstance(page, page);
        log.put(references[i], containers[i]);
        assertEquals(i, references[i].getLogKey());
        assertEquals(0, references[i].getActiveTilGeneration());
      }

      assertEquals(4, log.snapshot(), "the snapshot retains its sparse pre-pin array extent");
      log.cleanupSnapshot();
      assertEquals(4, log.pinnedSize());
      assertEquals(4, log.pinnedHighWater());
      assertDenseList(log.getList(), 4);

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      final int retiredSlot = batch.slotAt(0);
      final PageReference staleCopy = new PageReference(batch.referenceAt(0));
      batch.setWriteResult(0, 4_096L, 0L, true);
      log.validatePinnedSpillCandidate(batch, 0);
      log.publishPinnedSpillCandidate(batch, 0);
      batch.clear();

      assertEquals(3, log.pinnedSize());
      assertEquals(4, log.pinnedHighWater(), "retiring a page must not shrink the identity high-water mark");
      final List<PageContainer> dense = log.getList();
      assertDenseList(dense, 3);
      assertFalse(dense.contains(containers[0]), "the dense view must not expose a tombstoned hole");

      assertNull(log.get(staleCopy));
      assertEquals(4_096L, staleCopy.getKey(), "a reachable stale copy resolves through the published handle");
      assertTrue(staleCopy.hasHash(), "a zero checksum remains present after pinned-spill publication");
      assertEquals(0L, staleCopy.getHashAsLong());
      final PageReference handlelessStale = new PageReference().setLogKey(retiredSlot)
                                                              .setActiveTilGeneration(
                                                                  TransactionIntentLog.PINNED_GENERATION);
      assertNull(log.get(handlelessStale), "a tombstoned pinned slot must never alias another container");
      assertEquals(Constants.NULL_ID_LONG, handlelessStale.getKey());

      final PageReference nextReference = new PageReference();
      final IndirectPage nextPage = new IndirectPage();
      log.put(nextReference, PageContainer.getInstance(nextPage, nextPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      assertEquals(4, log.pinnedSize());
      assertEquals(5, log.pinnedHighWater());
      assertEquals(4, nextReference.getLogKey(), "new pages append after the tombstone instead of reusing it");
      assertDenseList(log.getList(), 4);
    } finally {
      log.close();
    }
  }

  @Test
  void captureHonoursBothBudgetsAndAdvancesPersistentCursor() {
    final TransactionIntentLog log = newLog();
    try {
      // Slot zero is a directly-mutated/non-trie anchor. It remains pinned and consumes scan budget,
      // but the exact page-class allow-list must never capture it.
      final PageReference anchorReference = new PageReference();
      final OverflowPage anchor = new OverflowPage(new byte[] {1});
      log.put(anchorReference, PageContainer.getInstance(anchor, anchor));
      for (int i = 0; i < 4; i++) {
        final PageReference reference = new PageReference();
        final IndirectPage page = new IndirectPage();
        log.put(reference, PageContainer.getInstance(page, page));
      }
      assertEquals(5, log.snapshot());
      log.cleanupSnapshot();

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(0, log.capturePinnedSpillCandidates(1, batch),
          "one-slot scan budget must stop after the unsupported anchor");
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      assertEquals(1, batch.slotAt(0));
      batch.clear();
      assertEquals(1, log.capturePinnedSpillCandidates(4, batch));
      assertEquals(2, batch.slotAt(0), "candidate capacity one must stop capture and persist its next cursor");
      batch.clear();
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      assertEquals(3, batch.slotAt(0));

      assertEquals(5, log.pinnedSize());
      assertEquals(5, log.pinnedHighWater());
      assertDenseList(log.getList(), 5);
    } finally {
      log.close();
    }
  }

  @Test
  void exactTupleValidationRejectsContainerReplacement() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference reference = new PageReference();
      final IndirectPage original = new IndirectPage();
      log.put(reference, PageContainer.getInstance(original, original));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      batch.setWriteResult(0, 8_192L, 1L, true);
      final IndirectPage replacement = new IndirectPage();
      final PageContainer replacementContainer = PageContainer.getInstance(replacement, replacement);
      log.put(reference, replacementContainer);

      assertThrows(IllegalStateException.class, () -> log.validatePinnedSpillCandidate(batch, 0));
      assertSame(replacementContainer, log.get(reference));
      assertEquals(1, log.pinnedSize(), "identity failure must leave the current live entry owned by the log");
    } finally {
      log.close();
    }
  }

  @Test
  void captureSkipsCompleteOnlyPinnedContainer() {
    final TransactionIntentLog log = newLog();
    try {
      final IndirectPage complete = new IndirectPage();
      final PageContainer completeOnly = mock(PageContainer.class);
      when(completeOnly.getComplete()).thenReturn(complete);
      when(completeOnly.getModified()).thenReturn(null);
      final PageReference reference = new PageReference();
      log.put(reference, completeOnly);

      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      assertEquals(1, log.pinnedSize());

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(0, log.capturePinnedSpillCandidates(1, batch));
      assertEquals(0, batch.size());
      assertSame(completeOnly, log.get(reference));
    } finally {
      log.close();
    }
  }

  @Test
  void clearReusesSlotZeroOnlyAfterThePriorSharedHandleIsDurable() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference firstReference = new PageReference();
      final IndirectPage firstPage = new IndirectPage();
      log.put(firstReference, PageContainer.getInstance(firstPage, firstPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      final PageReference staleCopy = new PageReference(firstReference);
      batch.setWriteResult(0, 32_768L, 0L, false);
      log.publishPinnedSpillCandidate(batch, 0);
      log.clear();
      assertEquals(0, log.pinnedHighWater(),
          "a fully durable handle epoch must not make a long-lived writer grow its slot arrays forever");

      final PageReference nextReference = new PageReference();
      final IndirectPage nextPage = new IndirectPage();
      log.put(nextReference, PageContainer.getInstance(nextPage, nextPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      assertEquals(0, nextReference.getLogKey(), "the next proven-safe identity epoch starts at slot zero");

      assertNull(log.get(staleCopy));
      assertEquals(32_768L, staleCopy.getKey(),
          "the old copy follows its completed handle, never the reused numeric slot");
      assertSame(nextPage, log.get(nextReference).getModified());
    } finally {
      log.close();
    }
  }

  @Test
  void unresolvedClearPermanentlyPreventsReuseAcrossALaterDurableEpoch() {
    final TransactionIntentLog log = newLog();
    try {
      final PageReference firstReference = new PageReference();
      final IndirectPage firstPage = new IndirectPage();
      log.put(firstReference, PageContainer.getInstance(firstPage, firstPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      final PageReference unresolvedCopy = new PageReference(firstReference);

      log.clear();
      assertEquals(1, log.pinnedHighWater(),
          "rollback-style teardown cannot recycle an identity whose shared handle never completed");

      final PageReference nextReference = new PageReference();
      final IndirectPage nextPage = new IndirectPage();
      log.put(nextReference, PageContainer.getInstance(nextPage, nextPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      assertEquals(1, nextReference.getLogKey());
      assertNull(log.get(unresolvedCopy), "an unresolved stale copy must address only its tombstoned slot");
      assertSame(nextPage, log.get(nextReference).getModified());

      final TransactionIntentLog.PinnedSpillBatch batch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(1, log.capturePinnedSpillCandidates(1, batch));
      assertEquals(1, batch.slotAt(0));
      batch.setWriteResult(0, 65_536L, 0L, false);
      log.publishPinnedSpillCandidate(batch, 0);
      log.clear();
      assertEquals(2, log.pinnedHighWater(),
          "a later all-durable/empty clear must not erase the earlier unresolved-handle retirement");

      final PageReference thirdReference = new PageReference();
      final IndirectPage thirdPage = new IndirectPage();
      log.put(thirdReference, PageContainer.getInstance(thirdPage, thirdPage));
      assertEquals(1, log.snapshot());
      log.cleanupSnapshot();
      assertEquals(2, thirdReference.getLogKey(),
          "the third epoch must append past both retired identities instead of aliasing stale slot zero");
      assertNull(log.get(unresolvedCopy));
      assertSame(thirdPage, log.get(thirdReference).getModified());
    } finally {
      log.close();
    }
  }

  @Test
  void pinnedTeardownDrainsEveryOwnedPageBeforeRethrowing() {
    final TransactionIntentLog log = newLog();
    final HOTLeafPage failingPage = mock(HOTLeafPage.class);
    final HOTLeafPage companionPage = mock(HOTLeafPage.class);
    final HOTLeafPage laterPage = mock(HOTLeafPage.class);
    final RuntimeException expected = new RuntimeException("release failed");
    doThrow(expected).when(failingPage).close();

    final PageReference firstReference = new PageReference();
    final PageReference secondReference = new PageReference();
    log.put(firstReference, PageContainer.getInstance(failingPage, companionPage));
    log.put(secondReference, PageContainer.getInstance(laterPage, laterPage));
    assertEquals(2, log.snapshot());
    log.cleanupSnapshot();

    final RuntimeException failure = assertThrows(RuntimeException.class, log::clear);

    assertSame(expected, failure);
    verify(companionPage).close();
    verify(laterPage).close();
    assertEquals(0, log.pinnedSize());
    assertEquals(0, log.getList().size());
    log.close();
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }

  private static void assertDenseList(final List<PageContainer> containers, final int expectedSize) {
    assertEquals(expectedSize, containers.size());
    for (int i = 0; i < containers.size(); i++) {
      assertTrue(containers.get(i) != null, "dense list contains null at position " + i);
    }
  }
}
