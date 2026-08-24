package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ShardedPageCache} eviction protections:
 * <ul>
 * <li>{@link ShardedPageCache#removePage} — instance-granular removal without close, used by the
 * transaction-intent log to shield a dirty, transaction-private page from eviction.</li>
 * <li>The HOT-bit second chance added to {@link ShardedPageCache#evictUnderPressure()} so it
 * matches {@code ClockSweeper.sweep()} / {@code evictIfOverBudget()}.</li>
 * </ul>
 */
@DisplayName("ShardedPageCache eviction-protection Tests")
class ShardedPageCacheTest {

  private static final long PAGE_BYTES = 1024L;
  private static final long HOT_FIXED_HEAP_BYTES = 4L * 1024L;
  private static final long HOT_SIDE_REFERENCE_HEAP_BYTES = 144L;
  private static final long ARRAY_OVERHEAD_BYTES = 24L;

  private static PageReference keyFor(long k) {
    return new PageReference().setKey(k).setDatabaseId(0).setResourceId(0);
  }

  private static HOTLeafPage hotLeaf(Arena arena, long pageKey, int sideReferenceCount) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE),
        null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    for (int index = 0; index < sideReferenceCount; index++) {
      leaf.setPageReference(index, new PageReference().setKey(index + 1L));
    }
    return leaf;
  }

  private static long expectedHotLeafWeight(HOTLeafPage leaf) {
    return leaf.getActualMemorySize() + leaf.estimatedRetainedHeapBytes();
  }

  @Test
  @DisplayName("HOT leaf with no side references includes its fixed heap allowance")
  void hotLeafWithoutSideReferencesIncludesFixedHeapCharge() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(101);
      final HOTLeafPage leaf = hotLeaf(arena, 101, 0);

      cache.put(key, leaf);

      assertEquals(HOTLeafPage.DEFAULT_SIZE + HOT_FIXED_HEAP_BYTES, cache.getCurrentWeightBytes());
      cache.remove(key);
      leaf.close();
    }
  }

  @Test
  @DisplayName("HOT leaf charge includes its variable common-prefix array")
  void hotLeafChargeIncludesCommonPrefix() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(103);
      final byte[] commonPrefix = new byte[257];
      final HOTLeafPage leaf = new HOTLeafPage(103, 1, IndexType.CAS, arena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
          new int[HOTLeafPage.MAX_ENTRIES], 0, 0, commonPrefix, commonPrefix.length);

      cache.put(key, leaf);

      assertEquals(HOTLeafPage.DEFAULT_SIZE + HOT_FIXED_HEAP_BYTES + ARRAY_OVERHEAD_BYTES + commonPrefix.length,
          cache.getCurrentWeightBytes());
      cache.remove(key);
      leaf.close();
    }
  }

  @Test
  @DisplayName("HOT leaf charge includes every retained side reference")
  void hotLeafChargeIncludesSideReferences() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(102);
      final HOTLeafPage leaf = hotLeaf(arena, 102, 0);
      cache.put(key, leaf);
      final long weightWithoutSideReferences = cache.getCurrentWeightBytes();
      for (int index = 0; index < 37; index++) {
        leaf.setPageReference(index, new PageReference().setKey(index + 1L));
      }

      // A re-put is the publication boundary for a changed page and replaces the prior charge.
      cache.put(key, leaf);

      assertEquals(weightWithoutSideReferences + 37L * HOT_SIDE_REFERENCE_HEAP_BYTES, cache.getCurrentWeightBytes());
      cache.remove(key);
      leaf.close();
    }
  }

  @Test
  @DisplayName("side-reference heap charge can trigger budget eviction")
  void sideReferenceHeapChargeTriggersBudgetEviction() {
    try (Arena arena = Arena.ofConfined()) {
      // Native-only accounting would retain both 64 KiB frames at this budget. Their fixed and
      // per-reference heap charges push the tracked total past the severe-eviction threshold.
      final long nativeOnlyBudget = 2L * HOTLeafPage.DEFAULT_SIZE;
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(nativeOnlyBudget);
      final HOTLeafPage first = hotLeaf(arena, 201, 64);
      final HOTLeafPage second = hotLeaf(arena, 202, 64);
      final long oneLeafWeight = expectedHotLeafWeight(first);

      cache.put(keyFor(201), first);
      cache.put(keyFor(202), second);

      assertEquals(1L, cache.size(), "one HOT leaf must be evicted to restore the budget");
      assertEquals(oneLeafWeight, cache.getCurrentWeightBytes());
      assertTrue(first.isClosed() ^ second.isClosed(), "exactly one of the two leaves must be evicted");
      cache.clear();
    }
  }

  @Test
  @DisplayName("removing a HOT leaf subtracts its complete recorded charge")
  void removingHotLeafReturnsWeightToZero() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(301);
      final HOTLeafPage leaf = hotLeaf(arena, 301, 23);
      cache.put(key, leaf);
      assertTrue(cache.getCurrentWeightBytes() > leaf.getActualMemorySize());

      cache.remove(key);

      assertEquals(0L, cache.getCurrentWeightBytes());
      assertEquals(0L, cache.size());
      leaf.close();
    }
  }

  @Test
  @DisplayName("invalid HOT replacement cannot disturb the existing mapping or charge")
  void invalidHotPutLeavesExistingWinnerUntouched() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(302);
      final HOTLeafPage existing = hotLeaf(arena, 302, 3);
      final HOTLeafPage invalidReplacement = hotLeaf(arena, 303, 0);
      invalidReplacement.setCompletePageRef(invalidReplacement);
      cache.put(key, existing);
      key.setPage(existing);
      final long existingWeight = cache.getCurrentWeightBytes();

      assertThrows(IllegalStateException.class, () -> cache.put(key, invalidReplacement));

      assertSame(existing, cache.asMap().get(key));
      assertSame(existing, key.getPage());
      assertFalse(existing.isClosed());
      assertEquals(existingWeight, cache.getCurrentWeightBytes());
      invalidReplacement.close();
      cache.clear();
    }
  }

  @Test
  @DisplayName("failed guarded admission leaves a stale existing mapping exactly accounted")
  void invalidGuardedLoadDoesNotMutateExistingMapping() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(303);
      final HOTLeafPage existing = hotLeaf(arena, 303, 2);
      final HOTLeafPage invalidCandidate = hotLeaf(arena, 304, 0);
      invalidCandidate.setCompletePageRef(invalidCandidate);
      cache.put(key, existing);
      final long existingWeight = cache.getCurrentWeightBytes();
      existing.retire(); // leave a deliberately stale mapping so the loader path is exercised

      assertThrows(IllegalStateException.class, () -> cache.getOrLoadAndGuard(key, _ -> invalidCandidate));

      assertSame(existing, cache.asMap().get(key), "failed admission must not mutate CHM ownership");
      assertEquals(existingWeight, cache.getCurrentWeightBytes(),
          "failed admission must not uncharge the retained mapping");
      invalidCandidate.close();
      cache.remove(key);
    }
  }

  @Test
  @DisplayName("guarded publication failure preserves the old entry and retires the private candidate")
  void guardedPublicationFailureIsOwnershipAndChargeAtomic() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(307);
    final FakePage existing = new FakePage(307);
    final FakePage candidate = new FakePage(308);
    final AssertionError publicationFailure = new AssertionError("injected publication bookkeeping failure");
    cache.put(key, existing);
    existing.rejectGuards = true;
    candidate.lastCacheKeyFailure = publicationFailure;
    final long existingWeight = cache.getCurrentWeightBytes();

    final AssertionError thrown =
        assertThrows(AssertionError.class, () -> cache.getOrLoadAndGuard(key, _ -> candidate));

    assertSame(publicationFailure, thrown);
    assertSame(existing, cache.asMap().get(key));
    assertEquals(existingWeight, cache.getCurrentWeightBytes());
    assertFalse(existing.isClosed(), "failed candidate publication must not retire the displaced entry");
    assertEquals(0, candidate.guards.get(), "candidate guard must be released exactly once");
    assertTrue(candidate.orphaned, "an unadopted candidate must lose local ownership");
    assertTrue(candidate.isClosed(), "the unadopted candidate's native frame must be reclaimed");
    cache.clear();
  }

  @Test
  @DisplayName("failed unguarded admission leaves a stale existing mapping exactly accounted")
  void invalidComputedLoadDoesNotMutateExistingMapping() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(305);
      final HOTLeafPage existing = hotLeaf(arena, 305, 1);
      final HOTLeafPage invalidCandidate = hotLeaf(arena, 306, 0);
      invalidCandidate.setCompletePageRef(invalidCandidate);
      cache.put(key, existing);
      final long existingWeight = cache.getCurrentWeightBytes();
      existing.retire();

      assertThrows(IllegalStateException.class, () -> cache.get(key, (_, _) -> invalidCandidate));

      assertSame(existing, cache.asMap().get(key));
      assertEquals(existingWeight, cache.getCurrentWeightBytes());
      invalidCandidate.close();
      cache.remove(key);
    }
  }

  @Test
  @DisplayName("an Error during page retirement cannot retain a dead mapping or charge")
  void retirementErrorStillDetachesMappingAndCharge() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(304);
      final HOTLeafPage page =
          new HOTLeafPage(304, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            throw new AssertionError("expected release failure");
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      cache.put(key, page);
      key.setPage(page);

      cache.evictUnderPressure(); // consume HOT second chance
      // Budget evictions run on behalf of operations on UNRELATED pages that already succeeded:
      // the retirement failure stays LOUD (monotonic counter + ERROR log) without failing that
      // caller. Owner paths (clear/remove/put) keep their throw — see the clear test below.
      final long failuresBefore = ShardedPageCache.evictionRetirementFailureCount();
      cache.evictUnderPressure();
      assertTrue(ShardedPageCache.evictionRetirementFailureCount() >= failuresBefore + 1,
          "the retirement failure must surface through the eviction-failure counter, not vanish");
      assertTrue(page.isClosed());
      assertEquals(0L, cache.size());
      assertEquals(0L, cache.getCurrentWeightBytes());
      assertNull(key.getPage(), "fail-closed eviction must clear the exact stale swizzle");
    }
  }

  @Test
  @DisplayName("clear retires every page before propagating a retirement Error")
  void clearDrainsEveryOwnerBeforeRethrowing() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final HOTLeafPage failing =
          new HOTLeafPage(305, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            throw new AssertionError("expected clear release failure");
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      final HOTLeafPage succeeding = hotLeaf(arena, 306, 1);
      cache.put(keyFor(305), failing);
      cache.put(keyFor(306), succeeding);

      final AssertionError failure = assertThrows(AssertionError.class, cache::clear);

      assertEquals("expected clear release failure", failure.getMessage());
      assertTrue(failing.isClosed());
      assertTrue(succeeding.isClosed());
      assertEquals(0L, cache.size());
      assertEquals(0L, cache.getCurrentWeightBytes());
    }
  }

  @Test
  @DisplayName("putIfAbsent publishes its mapping and charge atomically with removal")
  void putIfAbsentAndRemoveCannotLeaveGhostCharge() throws Exception {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(401);
    final CountDownLatch weightStarted = new CountDownLatch(1);
    final CountDownLatch allowWeight = new CountDownLatch(1);
    final FakePage page = new FakePage(401, weightStarted, allowWeight);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> insertion = executor.submit(() -> cache.putIfAbsent(key, page));
      assertTrue(weightStarted.await(5, TimeUnit.SECONDS), "putIfAbsent must reach its charge calculation");

      final CountDownLatch removalStarted = new CountDownLatch(1);
      final CountDownLatch removalReturned = new CountDownLatch(1);
      final Future<?> removal = executor.submit(() -> {
        removalStarted.countDown();
        cache.remove(key);
        removalReturned.countDown();
      });
      assertTrue(removalStarted.await(5, TimeUnit.SECONDS));

      // The mapping is not published until the compute callback (including chargeWeight) returns,
      // so same-key remove must still be waiting. The old putIfAbsent-then-charge sequence allowed
      // remove to return here, after which the delayed charge became permanently ownerless.
      final boolean removedBeforeChargeFinished = removalReturned.await(1, TimeUnit.SECONDS);
      allowWeight.countDown();
      insertion.get(5, TimeUnit.SECONDS);
      removal.get(5, TimeUnit.SECONDS);

      assertFalse(removedBeforeChargeFinished, "same-key removal must serialize behind publication and charging");
      assertEquals(0L, cache.size());
      assertEquals(0L, cache.getCurrentWeightBytes());
      page.close();
    } finally {
      allowWeight.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  @DisplayName("clear waits for an in-flight admission and retires its exact owner")
  void clearAndConcurrentAdmissionCannotLeaveMappingOrCharge() throws Exception {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final CountDownLatch weightStarted = new CountDownLatch(1);
    final CountDownLatch allowWeight = new CountDownLatch(1);
    final CountDownLatch clearStarted = new CountDownLatch(1);
    final CountDownLatch clearReturned = new CountDownLatch(1);
    final FakePage page = new FakePage(405, weightStarted, allowWeight);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> insertion = executor.submit(() -> cache.putIfAbsent(keyFor(405), page));
      assertTrue(weightStarted.await(5, TimeUnit.SECONDS));

      final Future<?> clearing = executor.submit(() -> {
        clearStarted.countDown();
        cache.clear();
        clearReturned.countDown();
      });
      assertTrue(clearStarted.await(5, TimeUnit.SECONDS));
      assertFalse(clearReturned.await(1, TimeUnit.SECONDS),
          "clear must establish quiescence instead of racing a half-published admission");

      allowWeight.countDown();
      insertion.get(5, TimeUnit.SECONDS);
      clearing.get(5, TimeUnit.SECONDS);

      assertEquals(0L, cache.size());
      assertEquals(0L, cache.getCurrentWeightBytes());
      assertTrue(page.isClosed(), "the admitted page must be retired before clear returns");
    } finally {
      allowWeight.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  @DisplayName("same-key HOT load replacement retires only the cache loser")
  void guardedHotLoadReplacementRetiresLoserAndPreservesReplacement() {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
      final PageReference key = keyFor(402);
      final HOTLeafPage loaded = hotLeaf(arena, 402, 7);
      final HOTLeafPage replacement = hotLeaf(arena, 403, 11);

      assertSame(loaded, cache.getOrLoadAndGuard(key, _ -> loaded));
      key.setPage(loaded); // model the reader's tree-reference swizzle

      cache.put(key, replacement);

      assertTrue(loaded.isOrphaned(), "the replaced HOT leaf must enter deferred teardown");
      assertFalse(loaded.isClosed(), "its live guard must retain physical ownership");
      assertNull(key.getPage(), "the replaced leaf's stale swizzle must be cleared");
      assertSame(replacement, cache.get(key));
      assertEquals(expectedHotLeafWeight(replacement), cache.getCurrentWeightBytes());

      loaded.releaseGuard();
      assertTrue(loaded.isClosed(), "the former cache page must close on its holder's final release");

      // loaded still remembers this key. An identity-conditional removePage must not remove or
      // uncharge the replacement, and must not clear a replacement swizzle.
      key.setPage(replacement);
      cache.removePage(loaded);
      assertSame(replacement, cache.get(key));
      assertSame(replacement, key.getPage());
      assertEquals(expectedHotLeafWeight(replacement), cache.getCurrentWeightBytes());
      cache.clear();
    }
  }

  @Test
  @DisplayName("late guard transfers eviction lifetime without retaining cache ownership")
  void lateGuardEvictionRemovesMappingAndChargeImmediately() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(PAGE_BYTES);
    final PageReference key = keyFor(403);
    final FakePage page = new FakePage(403);
    cache.put(key, page);

    cache.evictUnderPressure(); // consume the page's HOT-bit second chance
    page.acquireGuardAfterNextZeroObservation();
    cache.evictUnderPressure();

    assertEquals(0L, cache.size(), "the cache must relinquish the orphaned mapping immediately");
    assertEquals(0L, cache.getCurrentWeightBytes(), "the detached page must no longer consume cache budget");
    assertTrue(page.isOrphanedForTest());
    assertFalse(page.isClosed(), "the injected late guard still owns the physical page lifetime");
    assertEquals(0, page.versionForTest(), "a page version must never drift under a live holder");

    page.releaseGuard();
    assertTrue(page.isClosed(), "the last guard must complete deferred teardown");
  }

  @Test
  @DisplayName("raw get removes a closed orphan and its recorded charge")
  void rawGetCleansClosedOrphanedMapping() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(404);
    final FakePage page = new FakePage(404);
    cache.put(key, page);
    assertTrue(page.acquireGuard());

    page.retire(); // model an external owner ending cache ownership without removePage
    page.releaseGuard();
    assertTrue(page.isClosed());
    assertEquals(PAGE_BYTES, cache.getCurrentWeightBytes(), "precondition: stale charge is still recorded");

    assertNull(cache.get(key), "raw get must never expose a closed cached page");
    assertEquals(0L, cache.size());
    assertEquals(0L, cache.getCurrentWeightBytes());
  }

  @Test
  @DisplayName("asMap is a cached live view with no raw ownership-mutation escape")
  void asMapRejectsEveryMutationRouteBeforeInvokingCallbacks() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(405);
    final FakePage page = new FakePage(405);
    cache.put(key, page);
    final var view = cache.asMap();
    final var entry = view.entrySet().iterator().next();
    final AtomicInteger callbacks = new AtomicInteger();

    assertSame(view, cache.asMap(), "asMap must not allocate a wrapper on each observation");
    assertThrows(UnsupportedOperationException.class, () -> view.put(key, page));
    assertThrows(UnsupportedOperationException.class, () -> view.remove(key));
    assertThrows(UnsupportedOperationException.class, () -> view.putAll(Map.of()));
    assertThrows(UnsupportedOperationException.class, view::clear);
    assertThrows(UnsupportedOperationException.class, () -> view.putIfAbsent(key, page));
    assertThrows(UnsupportedOperationException.class, () -> view.remove(key, page));
    assertThrows(UnsupportedOperationException.class, () -> view.replace(key, page));
    assertThrows(UnsupportedOperationException.class, () -> view.replace(key, page, page));
    assertThrows(UnsupportedOperationException.class, () -> view.replaceAll((_, value) -> {
      callbacks.incrementAndGet();
      return value;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.computeIfAbsent(keyFor(406), _ -> {
      callbacks.incrementAndGet();
      return page;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.computeIfPresent(key, (_, value) -> {
      callbacks.incrementAndGet();
      return value;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.compute(key, (_, value) -> {
      callbacks.incrementAndGet();
      return value;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.merge(key, page, (left, _) -> {
      callbacks.incrementAndGet();
      return left;
    }));

    assertThrows(UnsupportedOperationException.class, () -> view.keySet().remove(key));
    assertThrows(UnsupportedOperationException.class, () -> view.keySet().removeAll(Set.of(key)));
    assertThrows(UnsupportedOperationException.class, () -> view.keySet().retainAll(Set.of()));
    assertThrows(UnsupportedOperationException.class, () -> view.keySet().removeIf(ignored -> {
      callbacks.incrementAndGet();
      return true;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.values().remove(page));
    assertThrows(UnsupportedOperationException.class, () -> view.values().removeAll(Set.of(page)));
    assertThrows(UnsupportedOperationException.class, () -> view.values().retainAll(Set.of()));
    assertThrows(UnsupportedOperationException.class, () -> view.values().removeIf(ignored -> {
      callbacks.incrementAndGet();
      return true;
    }));
    assertThrows(UnsupportedOperationException.class, () -> view.entrySet().remove(entry));
    assertThrows(UnsupportedOperationException.class, () -> view.entrySet().removeAll(Set.of(entry)));
    assertThrows(UnsupportedOperationException.class, () -> view.entrySet().retainAll(Set.of()));
    assertThrows(UnsupportedOperationException.class, () -> view.entrySet().removeIf(ignored -> {
      callbacks.incrementAndGet();
      return true;
    }));
    assertThrows(UnsupportedOperationException.class, () -> {
      final var iterator = view.keySet().iterator();
      iterator.next();
      iterator.remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      final var iterator = view.values().iterator();
      iterator.next();
      iterator.remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      final var iterator = view.entrySet().iterator();
      iterator.next();
      iterator.remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> entry.setValue(page));

    assertEquals(0, callbacks.get(), "unsupported mutation must fail before a user callback runs");
    assertSame(page, view.get(key));
    assertEquals(1L, cache.size());
    assertEquals(PAGE_BYTES, cache.getCurrentWeightBytes());
    assertFalse(page.isClosed());

    cache.clear();
    assertTrue(view.isEmpty(), "the observational view must remain live");
    assertTrue(page.isClosed());
  }

  @Test
  @DisplayName("removePage takes a page out of the cache by instance without closing it")
  void removePageEvictsByInstanceWithoutClosing() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(1);
    final FakePage page = new FakePage(1);
    cache.put(key, page);
    assertSame(page, cache.get(key), "page should be cached after put");

    cache.removePage(page);

    assertNull(cache.get(key), "page must be gone from the cache");
    assertFalse(page.isClosed(), "removePage must NOT close the page — the caller keeps ownership");
    assertEquals(0L, cache.getCurrentWeightBytes(), "tracked weight must drop back to zero");
  }

  @Test
  @DisplayName("removePage leaves other cached pages untouched and is a no-op when absent")
  void removePageNoOpWhenAbsent() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference keptKey = keyFor(1);
    final FakePage kept = new FakePage(1);
    cache.put(keptKey, kept);

    cache.removePage(new FakePage(99)); // not cached — must not throw or disturb anything

    assertSame(kept, cache.get(keptKey), "an unrelated cached page must be untouched");
    assertEquals(PAGE_BYTES, cache.getCurrentWeightBytes());
  }

  @Test
  @DisplayName("evictUnderPressure gives every HOT page a one-pass second chance")
  void evictUnderPressureHonoursHotBit() {
    // Budget 8 KiB; 7 pages of 1 KiB = 7 KiB. 7 KiB <= 1.1*budget so put() does not pre-evict,
    // and 7 KiB > 0.75*budget (= 6 KiB) so evictUnderPressure is active.
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(8192L);
    final FakePage[] pages = new FakePage[7];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = new FakePage(i);
      pages[i].markAccessed(); // sets the HOT bit
      cache.put(keyFor(i), pages[i]);
    }
    for (final FakePage p : pages) {
      assertTrue(p.isHot() && !p.isClosed(), "precondition: all pages hot and live");
    }

    // First pass: every page is HOT → all survive, all HOT bits cleared (second chance consumed).
    cache.evictUnderPressure();
    for (final FakePage p : pages) {
      assertTrue(cache.containsPage(p), "a HOT page must survive the first evictUnderPressure pass");
      assertFalse(p.isHot(), "the HOT bit must be cleared (second chance consumed)");
    }

    // Second pass: bits are cold now → eviction proceeds down to the 3/4-budget target.
    cache.evictUnderPressure();
    int closed = 0;
    for (final FakePage p : pages) {
      if (p.isClosed()) {
        closed++;
      }
    }
    assertTrue(closed >= 1, "with no HOT reprieve, evictUnderPressure must now evict a page");
    assertTrue(cache.getCurrentWeightBytes() <= 8192L * 3 / 4,
        "evictUnderPressure must bring the cache down to its 3/4 target");
  }

  /** Minimal {@link CacheablePage} test double — fixed 1 KiB weight, software HOT bit + guards. */
  private static final class FakePage implements CacheablePage {
    private final long pageKey;
    private final CountDownLatch weightStarted;
    private final CountDownLatch allowWeight;
    private volatile boolean hot;
    private volatile boolean closed;
    private volatile boolean orphaned;
    private volatile boolean rejectGuards;
    private volatile boolean injectGuardOnNextCount;
    private volatile Error lastCacheKeyFailure;
    private volatile PageReference lastCacheKey;
    private final AtomicInteger guards = new AtomicInteger();
    private final AtomicInteger version = new AtomicInteger();

    FakePage(long pageKey) {
      this(pageKey, null, null);
    }

    FakePage(long pageKey, CountDownLatch weightStarted, CountDownLatch allowWeight) {
      this.pageKey = pageKey;
      this.weightStarted = weightStarted;
      this.allowWeight = allowWeight;
    }

    @Override
    public long getActualMemorySize() {
      if (weightStarted != null) {
        weightStarted.countDown();
        try {
          if (!allowWeight.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out waiting to finish the blocked weight calculation");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("weight calculation interrupted", e);
        }
      }
      return PAGE_BYTES;
    }

    @Override
    public void markAccessed() {
      hot = true;
    }

    @Override
    public boolean isHot() {
      return hot;
    }

    @Override
    public void clearHot() {
      hot = false;
    }

    @Override
    public boolean acquireGuard() {
      if (rejectGuards || closed || (orphaned && guards.get() <= 0)) {
        return false;
      }
      guards.incrementAndGet();
      return true;
    }

    @Override
    public void markOrphaned() {
      orphaned = true;
    }

    @Override
    public void releaseGuard() {
      final int remaining = guards.decrementAndGet();
      if (remaining < 0) {
        throw new IllegalStateException("guard underflow");
      }
      if (remaining == 0 && orphaned) {
        close();
      }
    }

    @Override
    public int getGuardCount() {
      if (injectGuardOnNextCount) {
        injectGuardOnNextCount = false;
        guards.incrementAndGet();
        return 0; // the evictor observed zero immediately before this simulated late acquisition
      }
      return guards.get();
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      if (guards.get() == 0) {
        closed = true;
      }
    }

    @Override
    public void incrementVersion() {
      version.incrementAndGet();
    }

    @Override
    public long getPageKey() {
      return pageKey;
    }

    @Override
    public int getRevision() {
      return 0;
    }

    @Override
    public PageReference lastCacheKey() {
      return lastCacheKey;
    }

    @Override
    public void setLastCacheKey(PageReference cacheKey) {
      if (lastCacheKeyFailure != null) {
        throw lastCacheKeyFailure;
      }
      lastCacheKey = cacheKey;
    }

    @Override
    public IndexType getIndexType() {
      return IndexType.DOCUMENT;
    }

    void acquireGuardAfterNextZeroObservation() {
      injectGuardOnNextCount = true;
    }

    boolean isOrphanedForTest() {
      return orphaned;
    }

    int versionForTest() {
      return version.get();
    }
  }
}
