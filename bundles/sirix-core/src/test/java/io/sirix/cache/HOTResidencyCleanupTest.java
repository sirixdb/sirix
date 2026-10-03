/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

final class HOTResidencyCleanupTest {
  private static PageReference key(final long database, final long resource, final long offset) {
    return new PageReference().setDatabaseId(database).setResourceId(resource).setKey(offset);
  }

  private static HOTLeafPage page(final Arena arena, final boolean nativeImage) {
    final MemorySegment memory = nativeImage
        ? arena.allocate(HOTLeafPage.DEFAULT_SIZE)
        : MemorySegment.ofArray(new byte[0]);
    return new HOTLeafPage(1, 1, IndexType.PROJECTION, memory, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }

  private static HOTLeafPage failDetach(final HOTLeafPage page, final Error failure) {
    final HOTLeafPage failing = spy(page);
    doAnswer(invocation -> {
      invocation.callRealMethod();
      throw failure;
    }).when(failing).setLastCacheKey(null);
    return failing;
  }

  @ParameterizedTest
  @ValueSource(strings = {"remove", "removeAndGet", "clear", "close"})
  void bothResidenciesAreDetachedAndFirstFailureSurvives(final String operation) {
    try (Arena arena = Arena.ofConfined()) {
      final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
      try {
        final Error first = new OutOfMemoryError("heap detach");
        final Error second = new AssertionError("native detach");
        final PageReference key = key(1, 2, 3);
        final HOTLeafPage heap = failDetach(page(arena, false), first);
        final HOTLeafPage nativeImage = failDetach(page(arena, true), second);
        cache.put(key, heap);
        cache.put(key, nativeImage);
        final Error thrown = assertThrows(Error.class, () -> {
          switch (operation) {
            case "remove" -> cache.remove(key);
            case "removeAndGet" -> cache.removeAndGet(key);
            case "clear" -> cache.clear();
            case "close" -> cache.close();
            default -> throw new AssertionError(operation);
          }
        });
        assertSame(first, thrown);
        assertArrayEquals(new Throwable[] {second}, thrown.getSuppressed());
        assertTrue(heap.isClosed());
        assertTrue(nativeImage.isClosed());
        assertEquals(0, cache.size());
        assertEquals(0, cache.getCurrentWeightBytes());
      } finally {
        cache.clear();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedRemovalRetiresTheOtherDetachedImageWithoutDrainingItsGuard(final boolean nativeFailure) {
    try (Arena arena = Arena.ofConfined()) {
      final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
      try {
        final Error failure = new OutOfMemoryError("detach");
        final PageReference key = key(1, 2, 3);
        final PageReference unrelatedKey = key(1, 3, 4);
        final HOTLeafPage failing = failDetach(page(arena, nativeFailure), failure);
        final HOTLeafPage guarded = page(arena, !nativeFailure);
        final HOTLeafPage unrelated = page(arena, false);
        cache.put(key, failing);
        cache.put(key, guarded);
        cache.put(unrelatedKey, unrelated);
        assertTrue(guarded.acquireGuard());
        try {
          assertSame(failure, assertThrows(Error.class, () -> cache.removeAndGet(key)));
          assertNull(cache.get(key));
          assertTrue(failing.isClosed());
          assertFalse(guarded.isClosed());
          assertEquals(1, guarded.getGuardCount());
          assertSame(unrelated, cache.get(unrelatedKey));
        } finally {
          guarded.releaseGuard();
        }
        assertTrue(guarded.isClosed());
      } finally {
        cache.clear();
      }
    }
  }

  @Test
  void successfulRemovalTransfersOneImageAndRetiresAnUnreturnedDuplicate() {
    try (Arena arena = Arena.ofConfined()) {
      final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
      try {
        final PageReference key = key(1, 2, 3);
        final HOTLeafPage heap = page(arena, false);
        final HOTLeafPage nativeImage = page(arena, true);
        cache.put(key, heap);
        cache.put(key, nativeImage);
        final HOTLeafPage removed = cache.removeAndGet(key);
        assertSame(heap, removed);
        try {
          assertFalse(heap.isClosed());
          assertTrue(nativeImage.isClosed());
          assertNull(cache.get(key));
          assertNull(cache.removeAndGet(key));
        } finally {
          removed.retire();
        }
        final HOTLeafPage nativeOnly = page(arena, true);
        cache.put(key, nativeOnly);
        assertSame(nativeOnly, cache.removeAndGet(key));
        assertFalse(nativeOnly.isClosed());
        nativeOnly.retire();
      } finally {
        cache.clear();
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"resource,false", "database,false", "all,false", "resource,true", "database,true", "all,true"})
  void sweepsContinueAcrossResidenciesAndMiniPagesAfterFailures(final String scope, final boolean failComplete) {
    try (Arena arena = Arena.ofConfined();
        BufferManagerImpl manager = new BufferManagerImpl(1L << 20, 16L << 20, 1L << 20, 4, 4, 4)) {
      final Error completeFailure = new AssertionError("complete detach");
      final Error nativeFailure = new OutOfMemoryError("native detach");
      final Error heapFailure = new AssertionError("heap detach");
      final Error miniFailure = new AssertionError("mini retirement");
      final HOTLeafPage complete = failComplete
          ? failDetach(page(arena, true), completeFailure)
          : page(arena, true);
      final HOTLeafPage nativeImage = failDetach(page(arena, true), nativeFailure);
      final HOTLeafPage heap = failDetach(page(arena, false), heapFailure);
      final HOTLeafPage guarded = page(arena, false);
      final HOTLeafPage unrelated = page(arena, false);
      final HOTLeafPage sibling = page(arena, true);
      final PageReference unrelatedKey = key(9, 2, 5);
      final PageReference siblingKey = key(1, 9, 6);
      manager.getHOTLeafPageCache().put(key(1, 2, 1), complete);
      final Cache<PageReference, HOTLeafPage> fragments = manager.getHOTLeafFragmentCache();
      fragments.put(key(1, 2, 2), nativeImage);
      fragments.put(key(1, 2, 3), heap);
      fragments.put(key(1, 2, 4), guarded);
      fragments.put(unrelatedKey, unrelated);
      fragments.put(siblingKey, sibling);
      final HOTMiniPageCache miniCache = manager.getHOTMiniPageCache();
      final PageReference miniKey = key(1, 2, 7);
      final HOTMiniPage mini =
          spy(HOTMiniPage.append(null, 7, 1, new byte[] {1}, -1, new HOTLeafEntry(new byte[] {2}, null)));
      doAnswer(invocation -> {
        invocation.callRealMethod();
        throw miniFailure;
      }).when(mini).retire();
      assertNotNull(miniCache.pages());
      miniCache.pages().put(miniKey, mini);
      final PageReference otherMiniKey = key(1, 2, 8);
      miniCache.admit(otherMiniKey, miniCache.generation(otherMiniKey), 1, new byte[] {3}, -1, null);
      final HOTLookupCache lookups = manager.getHOTLookupCache();
      final HOTLookupKey lookup = HOTLookupKey.probe(1, 2, 1, IndexType.PATH, 0, new byte[] {1}, 0, 1).owned();
      assertTrue(lookups.put(lookup, new long[] {1}, lookups.generation()));
      assertTrue(guarded.acquireGuard());
      try {
        final Error thrown = assertThrows(Error.class, () -> {
          switch (scope) {
            case "resource" -> manager.clearCachesForResource(1, 2);
            case "database" -> manager.clearCachesForDatabase(1);
            case "all" -> manager.clearAllCaches();
            default -> throw new AssertionError(scope);
          }
        });
        assertSame(failComplete
            ? completeFailure
            : scope.equals("all")
                ? heapFailure
                : nativeFailure,
            thrown);
        assertTrue(containsFailure(thrown, heapFailure));
        assertTrue(containsFailure(thrown, miniFailure));
        assertTrue(containsFailure(thrown, nativeFailure));
        assertTrue(complete.isClosed());
        assertTrue(nativeImage.isClosed());
        assertTrue(heap.isClosed());
        assertTrue(mini.isClosed());
        assertEquals(0, miniCache.getCurrentWeightBytes());
        assertEquals(0, lookups.size());
        assertNull(fragments.get(key(1, 2, 4)));
        assertEquals(1, guarded.getGuardCount());
        assertFalse(guarded.isClosed());
        assertEquals(scope.equals("all"), unrelated.isClosed());
        assertEquals(!scope.equals("resource"), sibling.isClosed());
        if (!scope.equals("all")) {
          assertSame(unrelated, fragments.get(unrelatedKey));
        }
        if (scope.equals("resource")) {
          assertSame(sibling, fragments.get(siblingKey));
        }
      } finally {
        guarded.releaseGuard();
      }
      assertTrue(guarded.isClosed());
    }
  }

  private static boolean containsFailure(final Throwable primary, final Throwable expected) {
    return primary == expected
        || Arrays.stream(primary.getSuppressed()).anyMatch(failure -> containsFailure(failure, expected));
  }
}
