package io.sirix.index.hot.cache;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

final class HOTFragmentWindowEvictionTest {
  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @Test
  void retainsHistoricalImagesWhenThePartitionHasRoom() {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
    final HOTLeafPage image = fragment(1);
    try {
      cache.put(key(1), image);
      assertFalse(cache.evictSupersededFragment(image));
      assertSame(image, cache.get(key(1)));
      assertFalse(image.isClosed());
    } finally {
      cache.clear();
    }
  }

  @Test
  void pressureEvictionPreservesAnOutstandingHistoricalReader() throws Exception {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, imageWeight());
    final HOTLeafPage image = cache.getOrLoadAndGuard(key(1), ignored -> fragment(1));
    final CountDownLatch reading = new CountDownLatch(1);
    final CountDownLatch evicted = new CountDownLatch(1);
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var reader = executor.submit(() -> {
        HOTLeafPage guarded = null;
        try {
          guarded = cache.getAndGuard(key(1));
          assertSame(image, guarded);
          reading.countDown();
          evicted.await();
          assertArrayEquals(new byte[] {7, 9}, guarded.getValue(0));
        } finally {
          reading.countDown();
          if (guarded != null) {
            guarded.releaseGuard();
          }
        }
        return null;
      });
      boolean writerReleased = false;
      try {
        reading.await();
        assertTrue(cache.evictSupersededFragment(image));
        assertNull(cache.get(key(1)));
        assertEquals(0, cache.getCurrentWeightBytes());
        assertFalse(image.isClosed());
        writerReleased = true;
        image.releaseGuard();
        assertFalse(image.isClosed(), "the historical reader still owns a guard");
      } finally {
        if (!writerReleased) {
          image.releaseGuard();
        }
        evicted.countDown();
      }
      reader.get();
      assertTrue(image.isClosed());
      final HOTLeafPage reloaded = cache.getOrLoadAndGuard(key(1), ignored -> fragment(1));
      try {
        assertArrayEquals(new byte[] {7, 9}, reloaded.getValue(0));
      } finally {
        reloaded.releaseGuard();
      }
    } finally {
      cache.clear();
    }
  }

  @Test
  void aStaleVictimCannotEvictItsReplacement() {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, imageWeight());
    final PageReference key = key(1);
    final HOTLeafPage old = cache.getOrLoadAndGuard(key, ignored -> fragment(1));
    try {
      cache.remove(key);
      final HOTLeafPage replacement = fragment(1);
      cache.put(key, replacement);
      assertFalse(cache.compactImages().evictIfSame(key, old));
      assertSame(replacement, cache.get(key));
      assertEquals(imageWeight(), cache.getCurrentWeightBytes());
      assertFalse(old.isClosed());
    } finally {
      old.releaseGuard();
      cache.clear();
    }
    assertTrue(old.isClosed());
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void onlySuccessfulSlidingAdvancementOffersTheDroppedFragment(final VersioningType versioning) {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 2 * imageWeight());
    final HOTLeafPage newest = fragment(3);
    final HOTLeafPage middle = cache.getOrLoadAndGuard(key(2), ignored -> fragment(2));
    final HOTLeafPage oldest = cache.getOrLoadAndGuard(key(1), ignored -> fragment(1));
    final List<HOTLeafPage> window = List.of(newest, middle, oldest);
    final PageReference reference =
        key(3).setPageFragments(List.of(new PageFragmentKeyImpl(2, 2, 1, 1), new PageFragmentKeyImpl(1, 1, 1, 1)));
    final StorageEngineReader reader = reader(cache, reference, window);
    HOTLeafPage modified = null;
    try {
      modified = versioning.combineHOTLeafPagesForModification(newest, 3, reader, reference);
      if (versioning == VersioningType.SLIDING_SNAPSHOT) {
        assertNull(cache.get(key(1)), "the dropped image must yield room for the advancing window");
        assertTrue(oldest.isClosed());
        assertSame(middle, cache.get(key(2)), "the surviving fragment stays reusable");
        assertEquals(imageWeight(), cache.getCurrentWeightBytes());
      } else {
        assertSame(oldest, cache.get(key(1)), "other versioning strategies supply no last-use hint");
      }
      assertArrayEquals(new byte[] {7, 9}, modified.getValue(0));
    } finally {
      if (modified != null) {
        modified.close();
      }
      // FULL/INCREMENTAL do not borrow the window in this fixture.
      if (versioning == VersioningType.FULL || versioning == VersioningType.INCREMENTAL) {
        middle.releaseGuard();
        oldest.releaseGuard();
      }
      newest.close();
      cache.clear();
    }
  }

  @Test
  void failedCarryForwardKeepsTheOldWindowCached() {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, imageWeight());
    final HOTLeafPage newest = spy(fragment(3));
    final HOTLeafPage oldest = cache.getOrLoadAndGuard(key(1), ignored -> fragment(1));
    final List<HOTLeafPage> window = List.of(newest, oldest);
    final PageReference reference = key(3).setPageFragments(List.of(new PageFragmentKeyImpl(1, 1, 1, 1)));
    final var previous = reference.getPageFragments();
    final StorageEngineReader reader = reader(cache, reference, window);
    final IllegalStateException failure = new IllegalStateException("unreadable newer fragment");
    doThrow(failure).when(newest).findEntry(any(byte[].class));
    try {
      assertSame(failure, assertThrows(IllegalStateException.class,
          () -> VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPagesForModification(newest, 2, reader, reference)));
      assertSame(previous, reference.getPageFragments());
      assertSame(oldest, cache.get(key(1)));
      assertFalse(oldest.isClosed());
    } finally {
      newest.close();
      cache.clear();
    }
  }

  private static StorageEngineReader reader(final HOTFragmentCache cache, final PageReference reference,
      final List<HOTLeafPage> window) {
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final BufferManager buffers = mock(BufferManager.class);
    when(reader.getBufferManager()).thenReturn(buffers);
    when(buffers.getHOTLeafFragmentCache()).thenReturn(cache);
    when(reader.getRevisionNumber()).thenReturn(4);
    when(reader.getDatabaseId()).thenReturn(1L);
    when(reader.getResourceId()).thenReturn(1L);
    when(reader.loadHOTLeafFragments(reference)).thenReturn(window);
    doAnswer(ignored -> {
      for (int i = 1; i < window.size(); i++) {
        window.get(i).releaseGuard();
      }
      return null;
    }).when(reader).releaseHOTLeafFragments(window, window.getFirst());
    return reader;
  }

  private static long imageWeight() {
    final HOTFragmentCache probe = new HOTFragmentCache(1L << 20, 1L << 20);
    try {
      probe.put(key(0), fragment(0));
      return probe.getCurrentWeightBytes();
    } finally {
      probe.clear();
    }
  }

  private static PageReference key(final long offset) {
    return new PageReference().setKey(offset).setDatabaseId(1).setResourceId(1);
  }

  private static HOTLeafPage fragment(final int revision) {
    final byte[] packed = {2, 0, 0, 1, 2, 0, 7, 9};
    return new HOTLeafPage(revision, revision, IndexType.PROJECTION, MemorySegment.ofArray(packed), null, new int[] {0},
        1, packed.length, new byte[0], 0);
  }
}
