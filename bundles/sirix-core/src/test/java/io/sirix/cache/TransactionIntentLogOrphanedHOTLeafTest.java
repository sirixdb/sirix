/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which merged-away HOT leaves {@link TransactionIntentLog#releaseOrphanedHOTLeaves} may free.
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

  /** The page-key namespace these releases belong to; irrelevant to frame ownership, but required. */
  private static final long INDEX_SCOPE = TransactionIntentLog.indexScope(IndexType.PATH, 0);

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

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(copyHeldByTheTrie),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

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

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(mergedAwayReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      verify(mergedAwayLeaf).close();
      verify(survivingLeaf, never()).close();
      assertSame(survivingLeaf, log.get(survivingReference).getModified());
    } finally {
      log.close();
    }
  }

  /**
   * The regression behind the {@code windows-latest / query} lane's "HOT leaf disappeared while
   * acquiring a copy-on-write guard": freeing the frame left the container resolvable. Every
   * reference naming that entry — including the copies each indirect-page copy-on-write deep-copies —
   * kept getting the freed page back from {@link TransactionIntentLog#get}, and both
   * {@code loadHOTPage} implementations hand a container's page straight to their caller. Such a page
   * can never be guarded again, so the HOT writer's copy-on-write retry could not win on any schedule
   * — it either spun to its deadline or dead-ended.
   */
  @Test
  void aReleasedOrphanLeafIsNoLongerResolvableThroughTheLog() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      final PageReference mergedAwayReference = new PageReference();
      log.put(mergedAwayReference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(mergedAwayReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      verify(mergedAwayLeaf).close();
      assertNull(log.get(mergedAwayReference),
          "the log must not resolve a reference to a container whose leaf it just freed");
    } finally {
      log.close();
    }
  }

  /**
   * The same, one async-flush epoch rotation later. {@link TransactionIntentLog#snapshot()} moves
   * every container the background flush cannot write — which is every HOT leaf — into the pinned
   * region, so the CI failure's own orphans are pinned ones and the rule has to hold there too.
   */
  @Test
  void aReleasedPinnedOrphanLeafIsNoLongerResolvableThroughTheLog() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage mergedAwayLeaf = newReleasableLeaf();
      final PageReference mergedAwayReference = new PageReference();
      log.put(mergedAwayReference, PageContainer.getInstance(mergedAwayLeaf, mergedAwayLeaf));

      log.snapshot();
      log.cleanupSnapshot();
      assertEquals(1, log.pinnedSize(), "an epoch rotation pins the HOT leaf container");
      assertNotNull(log.get(mergedAwayReference), "the pinned container is still the live entry");

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(mergedAwayReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      verify(mergedAwayLeaf).close();
      assertNull(log.get(mergedAwayReference), "a freed pinned leaf must not stay resolvable");
    } finally {
      log.close();
    }
  }

  /**
   * A leaf a reader still guards is only marked orphaned — {@code isClosed()} stays false until the
   * last {@code releaseGuard()}. It is unusable from that moment on ({@code acquireGuard()} refuses
   * it), so it must stop resolving at release time rather than at teardown time; that window is the
   * one the writer's guard retry used to spin in.
   */
  @Test
  void aGuardDeferredReleaseIsAlsoUnresolvable() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage guardedLeaf = mock(HOTLeafPage.class);
      final AtomicBoolean orphaned = new AtomicBoolean();
      doAnswer(invocation -> {
        orphaned.set(true);
        return null;
      }).when(guardedLeaf).close();
      // A live guard defers the teardown: orphaned, but never closed while this reader holds it.
      when(guardedLeaf.isOrphaned()).thenAnswer(invocation -> orphaned.get());

      final PageReference guardedReference = new PageReference();
      log.put(guardedReference, PageContainer.getInstance(guardedLeaf, guardedLeaf));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(guardedReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      verify(guardedLeaf).close();
      assertNull(log.get(guardedReference), "an orphaned-but-not-yet-torn-down leaf must not stay resolvable");
    } finally {
      log.close();
    }
  }

  /** An entry the sharing walk spared keeps its container — the leak-never-free direction. */
  @Test
  void anEntryWhoseLeafSurvivedKeepsItsContainer() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage sharedLeaf = newReleasableLeaf();
      final PageReference orphanReference = new PageReference();
      log.put(orphanReference, PageContainer.getInstance(sharedLeaf, sharedLeaf));
      final PageReference otherHolder = new PageReference();
      log.put(otherHolder, PageContainer.getInstance(sharedLeaf, sharedLeaf));

      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(orphanReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      verify(sharedLeaf, never()).close();
      assertSame(sharedLeaf, log.get(orphanReference).getModified(),
          "a leaf that was never freed must keep both of its entries");
      assertSame(sharedLeaf, log.get(otherHolder).getModified());
    } finally {
      log.close();
    }
  }

  /** Sharing across the pinned/current region boundary is counted without scanning either region. */
  @Test
  void aLeafOwnedByPinnedAndCurrentEntriesIsNotReleased() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage sharedLeaf = newReleasableLeaf();
      final PageReference pinnedReference = new PageReference();
      log.put(pinnedReference, PageContainer.getInstance(sharedLeaf, sharedLeaf));
      log.snapshot();
      log.cleanupSnapshot();

      final PageReference currentReference = new PageReference();
      log.put(currentReference, PageContainer.getInstance(sharedLeaf, sharedLeaf));

      final long probesBefore = log.hotLeafOwnerProbeCount();
      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(pinnedReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      assertEquals(1, log.hotLeafOwnerProbeCount() - probesBefore,
          "one candidate must require one constant-time identity-owner probe");
      verify(sharedLeaf, never()).close();
      assertSame(sharedLeaf, log.get(pinnedReference).getModified());
      assertSame(sharedLeaf, log.get(currentReference).getModified());
    } finally {
      log.close();
    }
  }

  /** Retirement work is independent of the number of unrelated HOT leaves in the transaction. */
  @Test
  void orphanRetirementPerformsOneOwnerProbeRegardlessOfLogSize() {
    final TransactionIntentLog log = newLog();
    try {
      for (int i = 0; i < 256; i++) {
        final HOTLeafPage unrelatedLeaf = mock(HOTLeafPage.class);
        final PageReference unrelatedReference = new PageReference();
        log.put(unrelatedReference, PageContainer.getInstance(unrelatedLeaf, unrelatedLeaf));
      }
      final HOTLeafPage orphanLeaf = newReleasableLeaf();
      final PageReference orphanReference = new PageReference();
      log.put(orphanReference, PageContainer.getInstance(orphanLeaf, orphanLeaf));

      final long probesBefore = log.hotLeafOwnerProbeCount();
      log.releaseOrphanedHOTLeaves(INDEX_SCOPE, null, List.of(orphanReference),
          TransactionIntentLog.RELEASE_SITE_UNKNOWN);

      assertEquals(1, log.hotLeafOwnerProbeCount() - probesBefore,
          "retiring one orphan must not probe the other 256 log entries");
      verify(orphanLeaf).close();
    } finally {
      log.close();
    }
  }

  /**
   * The filter must not over-reach: a container still has a usable page while only its complete side
   * was given up, and a resolution that returns nothing there would send the writer to storage for a
   * page it is holding in-transaction changes for.
   */
  @Test
  void anEntryWhoseModifiedPageIsStillLiveKeepsResolving() {
    final TransactionIntentLog log = newLog();
    try {
      final HOTLeafPage releasedComplete = newReleasableLeaf();
      final HOTLeafPage liveModified = newReleasableLeaf();
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(releasedComplete, liveModified));

      releasedComplete.close();

      final PageContainer resolved = log.get(reference);
      assertNotNull(resolved, "a container with a live modified page must keep resolving");
      assertSame(liveModified, resolved.getModified());
    } finally {
      log.close();
    }
  }

  /** A mock whose {@code close()} reports through {@code isClosed()}/{@code isOrphaned()}. */
  private static HOTLeafPage newReleasableLeaf() {
    final HOTLeafPage leaf = mock(HOTLeafPage.class);
    final AtomicBoolean released = new AtomicBoolean();
    doAnswer(invocation -> {
      released.set(true);
      return null;
    }).when(leaf).close();
    when(leaf.isClosed()).thenAnswer(invocation -> released.get());
    when(leaf.isOrphaned()).thenAnswer(invocation -> released.get());
    return leaf;
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }
}
