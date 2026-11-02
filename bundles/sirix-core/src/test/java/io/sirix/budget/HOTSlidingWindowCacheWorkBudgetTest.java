package io.sirix.budget;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.index.IndexType;
import io.sirix.index.hot.cache.HOTFragmentCache;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A writer must not displace its next window with fragments whose last carry-forward just ran. */
final class HOTSlidingWindowCacheWorkBudgetTest {
  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @Test
  void advancingSixtyFourLeavesLoadsEachNewFragmentOnceWithinTheSameBudget() {
    final int leaves = 64;
    final int revisions = 12;
    final long weight = imageWeight();
    final long ceiling = 2L * leaves * weight;
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, ceiling);
    final BufferManager buffers = mock(BufferManager.class);
    when(buffers.getHOTLeafFragmentCache()).thenReturn(cache);
    int loads = 0;
    try {
      for (int revision = 3; revision <= revisions; revision++) {
        for (int leaf = 0; leaf < leaves; leaf++) {
          final HOTLeafPage newest = fragment(leaf, revision);
          final PageReference middleKey = key(leaf, revision - 1);
          final PageReference oldestKey = key(leaf, revision - 2);
          HOTLeafPage middle = cache.getAndGuard(middleKey);
          if (middle == null) {
            final int physicalRevision = revision - 1;
            final int pageKey = leaf;
            middle = cache.getOrLoadAndGuard(middleKey, ignored -> fragment(pageKey, physicalRevision));
            loads++;
          }
          HOTLeafPage oldest = cache.getAndGuard(oldestKey);
          if (oldest == null) {
            final int physicalRevision = revision - 2;
            final int pageKey = leaf;
            oldest = cache.getOrLoadAndGuard(oldestKey, ignored -> fragment(pageKey, physicalRevision));
            loads++;
          }
          final List<HOTLeafPage> window = List.of(newest, middle, oldest);
          final PageReference reference = key(leaf, revision).setPageFragments(
              List.of(new PageFragmentKeyImpl(revision - 1, middleKey.getKey(), 1, 1),
                  new PageFragmentKeyImpl(revision - 2, oldestKey.getKey(), 1, 1)));
          final StorageEngineReader reader = mock(StorageEngineReader.class);
          when(reader.getBufferManager()).thenReturn(buffers);
          when(reader.getRevisionNumber()).thenReturn(revision + 1);
          when(reader.getDatabaseId()).thenReturn(1L);
          when(reader.getResourceId()).thenReturn(1L);
          when(reader.loadHOTLeafFragments(reference)).thenReturn(window);
          doAnswer(ignored -> {
            window.get(1).releaseGuard();
            window.get(2).releaseGuard();
            return null;
          }).when(reader).releaseHOTLeafFragments(eq(window), any(HOTLeafPage.class));
          HOTLeafPage modified = null;
          try {
            modified = VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPagesForModification(newest, 3, reader, reference);
            assertArrayEquals(new byte[] {7, 9}, modified.getValue(0));
            assertTrue(cache.getCurrentWeightBytes() <= ceiling,
                "the useful window must fit without enlarging the partition or waiting for a sweeper");
          } finally {
            if (modified != null) {
              modified.close();
            }
            newest.close();
          }
        }
      }
      // First access reads two fragments per leaf; every subsequent revision introduces one.
      assertEquals(leaves * (revisions - 1), loads,
          "surviving fragments must not be reread because dropped windows polluted the cache");
    } finally {
      cache.clear();
    }
  }

  private static long imageWeight() {
    final HOTFragmentCache probe = new HOTFragmentCache(1L << 20, 1L << 20);
    try {
      probe.put(key(0, 1), fragment(0, 1));
      return probe.getCurrentWeightBytes();
    } finally {
      probe.clear();
    }
  }

  private static PageReference key(final int leaf, final int revision) {
    return new PageReference().setKey((long) revision * 1024 + leaf).setDatabaseId(1).setResourceId(1);
  }

  private static HOTLeafPage fragment(final int leaf, final int revision) {
    final byte[] packed = {2, 0, 0, 1, 2, 0, 7, 9};
    return new HOTLeafPage(leaf, revision, IndexType.PROJECTION, MemorySegment.ofArray(packed), null, new int[] {0}, 1,
        packed.length, new byte[0], 0);
  }
}
