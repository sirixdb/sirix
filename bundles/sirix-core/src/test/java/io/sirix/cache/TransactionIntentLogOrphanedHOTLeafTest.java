/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Which merged-away HOT leaves {@link TransactionIntentLog#releaseOrphanedHOTLeaves(List)} may
 * free.
 *
 * <p>
 * A {@link PageReference} names a container by {@code (generation, logKey)}, and an async-flush
 * epoch rotation ({@link TransactionIntentLog#snapshot()}) installs fresh generation-scoped arrays
 * whose log keys restart at zero. A reference COPY — every indirect-page copy-on-write deep-copies
 * its child references — keeps the raw identity its original was rebound away from, so its log key
 * addresses an unrelated, live container one epoch later. Freeing that container's leaf frees a 64
 * KB off-heap frame the trie still points at.
 * </p>
 */
final class TransactionIntentLogOrphanedHOTLeafTest {

  /**
   * The regression: an orphan reference left over from a prior epoch must never free the live entry
   * its stale log key now aliases.
   */
  @Test
  void aPriorEpochOrphanReferenceNeverFreesTheLiveEntryItsLogKeyAliases() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = mock(HOTLeafPage.class);
      final PageReference mergedAwayReference = new PageReference();
      log.put(mergedAwayReference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));
      assertEquals(0, mergedAwayReference.getLogKey());
      assertEquals(0, mergedAwayReference.getActiveTilGeneration());

      // What the trie actually holds after an indirect-page CoW: an independent reference object
      // carrying the identity the original had at copy time.
      final PageReference copyHeldByTheTrie = new PageReference(mergedAwayReference);

      // One async-flush epoch: fresh arrays, log keys start at zero again.
      log.snapshot();
      log.cleanupSnapshot();

      final HOTLeafPage liveLeaf = mock(HOTLeafPage.class);
      final PageReference liveReference = new PageReference();
      log.put(liveReference, PageContainer.getInstance(liveLeaf, liveLeaf));
      assertEquals(copyHeldByTheTrie.getLogKey(), liveReference.getLogKey(),
          "the aliasing this test is about: the new epoch reuses the stale copy's log key");

      log.releaseOrphanedHOTLeaves(List.of(copyHeldByTheTrie));

      verify(liveLeaf, never()).close();
      final PageContainer live = log.get(liveReference);
      assertNotNull(live, "the live entry must survive an unrelated epoch's orphan release");
      assertSame(liveLeaf, live.getModified());
    } finally {
      log.close();
    }
  }

  /** The capability itself is unchanged: an orphan of the CURRENT epoch is still freed. */
  @Test
  void aCurrentEpochOrphanReferenceStillFreesItsOwnLeafAlone() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = mock(HOTLeafPage.class);
      final PageReference mergedAwayReference = new PageReference();
      log.put(mergedAwayReference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      final HOTLeafPage survivingLeaf = mock(HOTLeafPage.class);
      final PageReference survivingReference = new PageReference();
      log.put(survivingReference, PageContainer.getInstance(survivingLeaf, survivingLeaf));

      log.releaseOrphanedHOTLeaves(List.of(mergedAwayReference));

      verify(mergedAwayLeaf).close();
      verify(survivingLeaf, never()).close();
      assertSame(survivingLeaf, log.get(survivingReference).getModified());
    } finally {
      log.close();
    }
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }
}
