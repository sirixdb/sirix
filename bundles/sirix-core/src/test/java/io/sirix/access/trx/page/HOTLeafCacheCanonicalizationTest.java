package io.sirix.access.trx.page;

import io.sirix.cache.EmptyCache;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("HOT leaf cache canonicalization")
class HOTLeafCacheCanonicalizationTest {

  private static HOTLeafPage newLeaf(final Arena arena, final long pageKey) {
    return new HOTLeafPage(pageKey, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
        new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }

  private static PageReference cacheKey(final long offset) {
    return new PageReference().setKey(offset).setDatabaseId(1).setResourceId(2);
  }

  @Test
  void emptyCacheLeavesTheIncomingPageCallerOwned() {
    try (Arena arena = Arena.ofShared()) {
      final EmptyCache<PageReference, HOTLeafPage> cache = new EmptyCache<>();
      final HOTLeafPage incoming = newLeaf(arena, 100);
      final PageReference handoff = new PageReference();

      final HOTLeafPage result =
          NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, cacheKey(100), handoff, incoming);

      assertSame(incoming, result);
      assertSame(incoming, handoff.getPage());
      assertFalse(incoming.isClosed());
      incoming.close();
    }
  }

  @Test
  void concurrentDecodesReturnOneCanonicalWinnerAndRetireOnlyTheLoser() throws Exception {
    try (Arena arena = Arena.ofShared()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = cacheKey(101);
      final HOTLeafPage first = newLeaf(arena, 101);
      final HOTLeafPage second = newLeaf(arena, 101);
      final PageReference firstHandoff = new PageReference();
      final PageReference secondHandoff = new PageReference();
      final CountDownLatch ready = new CountDownLatch(2);
      final CountDownLatch start = new CountDownLatch(1);
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        final Future<HOTLeafPage> firstResult = executor.submit(() -> {
          ready.countDown();
          assertTrue(start.await(5, TimeUnit.SECONDS));
          return NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, key, firstHandoff, first);
        });
        final Future<HOTLeafPage> secondResult = executor.submit(() -> {
          ready.countDown();
          assertTrue(start.await(5, TimeUnit.SECONDS));
          return NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, key, secondHandoff, second);
        });

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        final HOTLeafPage winner = firstResult.get(5, TimeUnit.SECONDS);
        assertSame(winner, secondResult.get(5, TimeUnit.SECONDS));

        assertSame(winner, cache.get(key));
        assertSame(winner, firstHandoff.getPage());
        assertSame(winner, secondHandoff.getPage());
        assertFalse(winner.isClosed(), "the instance returned to both callers must remain live");
        assertTrue(first.isClosed() ^ second.isClosed(), "exactly the unreturned decode must be retired");
        assertEquals(1L, cache.size());
        cache.clear();
      } finally {
        start.countDown();
        executor.shutdownNow();
      }
    }
  }

  @Test
  void synchronousBudgetEnforcementCannotEvictThePageBeingReturned() {
    try (Arena arena = Arena.ofShared()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1L);
      final PageReference key = cacheKey(102);
      final PageReference handoff = new PageReference();
      final HOTLeafPage incoming = newLeaf(arena, 102);

      final HOTLeafPage canonical = NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, key, handoff, incoming);

      assertSame(incoming, canonical);
      assertFalse(canonical.isClosed());
      assertSame(canonical, handoff.getPage());
      assertSame(canonical, cache.get(key));
      assertTrue(cache.getCurrentWeightBytes() > cache.getMaxWeightBytes(),
          "the protected result may temporarily remain over budget until a later eviction pass");
      cache.clear();
    }
  }

  @Test
  void nonCanonicalCandidateFailsAdmissionWithoutLeavingMappingOrCharge() {
    try (Arena arena = Arena.ofShared()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = cacheKey(103);
      final PageReference handoff = new PageReference();
      final HOTLeafPage incoming = newLeaf(arena, 103);
      final PageReference nonCanonicalSideReference = new PageReference().setKey(777);
      nonCanonicalSideReference.setPage(new OverflowPage(new byte[] {1}));
      incoming.setPageReference(0, nonCanonicalSideReference);

      assertThrows(IllegalStateException.class,
          () -> NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, key, handoff, incoming));
      assertTrue(incoming.isClosed(), "a rejected private candidate must be retired");
      assertEquals(0L, cache.size());
      assertEquals(0L, cache.getCurrentWeightBytes());
    }
  }

  @Test
  void existingCanonicalWinnerIsNotDisturbedByInvalidPrivateDecode() {
    try (Arena arena = Arena.ofShared()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = cacheKey(104);
      final PageReference handoff = new PageReference();
      final HOTLeafPage existing = newLeaf(arena, 104);
      final HOTLeafPage incoming = newLeaf(arena, 104);
      incoming.setCompletePageRef(incoming);
      cache.put(key, existing);
      final long existingWeight = cache.getCurrentWeightBytes();

      final HOTLeafPage canonical = NodeStorageEngineReader.adoptCanonicalHOTLeaf(cache, key, handoff, incoming);

      assertSame(existing, canonical);
      assertFalse(existing.isClosed());
      assertSame(existing, handoff.getPage());
      assertTrue(incoming.isClosed(), "the private decode that lost canonicalization must be retired");
      assertSame(existing, cache.get(key));
      assertEquals(existingWeight, cache.getCurrentWeightBytes());
      cache.clear();
    }
  }

  @Test
  void fragmentCleanupContinuesAfterAnEarlierReleaseFailure() {
    try (Arena arena = Arena.ofShared()) {
      final HOTLeafPage failingFirst = new HOTLeafPage(201, 1, IndexType.PROJECTION,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            throw new IllegalStateException("expected frame-release failure");
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      final HOTLeafPage guardedSecond = newLeaf(arena, 202);
      assertTrue(guardedSecond.acquireGuard());

      assertThrows(IllegalStateException.class,
          () -> NodeStorageEngineReader.releaseHOTLeafFragmentsCompletely(
              List.of(failingFirst, guardedSecond), null));

      assertTrue(failingFirst.isClosed());
      assertEquals(0, guardedSecond.getGuardCount(),
          "cleanup must release later guarded fragments after an earlier failure");
      guardedSecond.close();
    }
  }

  @Test
  void fragmentCleanupToleratesTheSameFailureInstanceAndStillReleasesLaterGuards() {
    try (Arena arena = Arena.ofShared()) {
      final AssertionError sharedFailure = new AssertionError("shared release failure");
      final HOTLeafPage failingFirst = new HOTLeafPage(203, 1, IndexType.PROJECTION,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            throw sharedFailure;
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      final HOTLeafPage failingSecond = new HOTLeafPage(204, 1, IndexType.PROJECTION,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            throw sharedFailure;
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      final HOTLeafPage guardedThird = newLeaf(arena, 205);
      assertTrue(failingSecond.acquireGuard());
      failingSecond.markOrphaned();
      assertTrue(guardedThird.acquireGuard());

      final AssertionError thrown = assertThrows(AssertionError.class,
          () -> NodeStorageEngineReader.releaseHOTLeafFragmentsCompletely(
              List.of(failingFirst, failingSecond, guardedThird), null));

      assertSame(sharedFailure, thrown);
      assertEquals(0, guardedThird.getGuardCount(),
          "self-suppression from an earlier release must not stop later cleanup");
      guardedThird.close();
    }
  }
}
