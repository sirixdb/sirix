package io.sirix.cache;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;

final class BufferManagerTeardownTest {
  private static final List<String> CACHE_FIELDS = List.of("pageCache", "recordPageCache", "recordPageFragmentCache",
      "revisionRootPageCache", "namesCache", "globalVerdictCache", "segmentVerdictCache", "globalDictionaryRecordCache",
      "globalDictionaryWarmMarkers", "pathSummaryCache", "hotLeafPageCache", "hotLeafFragmentCache", "hotMiniPageCache",
      "projectionBlobCache", "hotLookupCache");

  @ParameterizedTest
  @ValueSource(strings = {"pageCache", "recordPageCache", "recordPageFragmentCache", "revisionRootPageCache",
      "namesCache", "globalVerdictCache", "segmentVerdictCache", "globalDictionaryRecordCache",
      "globalDictionaryWarmMarkers", "pathSummaryCache", "hotLeafPageCache", "hotLeafFragmentCache", "hotMiniPageCache",
      "projectionBlobCache", "hotLookupCache"})
  void everyCacheIsClearedDespiteFailureAndCloseIsIdempotent(final String failingCache)
      throws ReflectiveOperationException {
    // Execute the real teardown without constructing caches or starting global sweepers.
    final BufferManagerImpl manager = mock(BufferManagerImpl.class, CALLS_REAL_METHODS);
    final Map<String, Object> caches = new LinkedHashMap<>();
    for (final String name : CACHE_FIELDS) {
      final Field field = BufferManagerImpl.class.getDeclaredField(name);
      field.setAccessible(true);
      final Object cache = mock(field.getType());
      field.set(manager, cache);
      caches.put(name, cache);
    }
    final ClockSweeper sweeper = mock(ClockSweeper.class);
    for (final String name : List.of("clockSweepers", "clockSweeperThreads")) {
      final Field field = BufferManagerImpl.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(manager, name.equals("clockSweepers")
          ? new ArrayList<>(List.of(sweeper))
          : new ArrayList<>());
    }
    final AssertionError first = new AssertionError("first clear failure");
    failClear(caches.get(failingCache), first);
    final boolean hasLaterCache = !failingCache.equals("hotLookupCache");
    final IllegalStateException later = new IllegalStateException("later clear failure");
    if (hasLaterCache) {
      failClear(caches.get("hotLookupCache"), later);
    }
    assertSame(first, assertThrows(AssertionError.class, manager::close));
    if (hasLaterCache) {
      assertEquals(1, first.getSuppressed().length);
      assertSame(later, first.getSuppressed()[0]);
    }
    manager.close();
    verify(sweeper).stop();
    for (final Map.Entry<String, Object> cache : caches.entrySet()) {
      assertEquals(1,
          mockingDetails(cache.getValue()).getInvocations()
                                          .stream()
                                          .filter(invocation -> invocation.getMethod().getName().equals("clear"))
                                          .count(),
          cache.getKey());
    }
  }

  private static void failClear(final Object cache, final Throwable failure) throws ReflectiveOperationException {
    final Method clear = cache.getClass().getMethod("clear");
    clear.invoke(doThrow(failure).when(cache));
  }
}
