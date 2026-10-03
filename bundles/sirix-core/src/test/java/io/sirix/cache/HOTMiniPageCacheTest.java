/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HOTMiniPageCacheTest {
  private static final byte[] KEY = {7, 11};
  private static final byte[] VALUE = {13, 17};

  private static PageReference key(final long database, final long resource, final long offset) {
    return new PageReference().setKey(offset).setDatabaseId(database).setResourceId(resource);
  }

  @Test
  void sideReferenceVariantsDoNotTurnOneKeyIntoSeveralPointMisses() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference pageKey = key(1, 2, 3);
    try {
      for (int side = 0; side < 8; side++)
        cache.admit(pageKey, 0, 7, KEY, side, null);
      final HOTMiniPage page = cache.getAndGuard(pageKey);
      assertNotNull(page);
      assertEquals(8, page.getEntryCount());
      assertEquals(1, page.getDistinctKeyCount());
      page.releaseGuard();
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {8}, null));
      cache.admit(pageKey, 0, 7, new byte[] {8}, -1, null);
      cache.admit(pageKey, 0, 7, new byte[] {9}, -1, null);
      assertFalse(cache.claimPointPromotion(pageKey, 0, KEY, null),
          "another side-reference variant still asks for the same serialized key");
      assertTrue(cache.claimPointPromotion(pageKey, 0, new byte[] {10}, null));
    } finally {
      cache.clear();
    }
  }

  @Test
  void distinctPointPromotionHasOneClaimAndSharesTheAdmissionBudget() throws Exception {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference pageKey = key(1, 2, 3);
    try {
      for (int i = 0; i < HOTMiniPageCache.POINT_PROMOTION_DISTINCT_KEYS - 1; i++) {
        assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {(byte) i}, null));
        cache.admit(pageKey, 0, 7, new byte[] {(byte) i}, -1, new HOTLeafEntry(VALUE, null));
      }
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {0}, null),
          "a repeated key does not spend another distinct miss");
      final long weight = cache.getCurrentWeightBytes();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<Boolean>> claims = new ArrayList<>();
      try (var pool = Executors.newFixedThreadPool(8)) {
        for (int i = 0; i < 8; i++) {
          final byte distinct = (byte) (i + 16);
          claims.add(pool.submit(() -> {
            final ReadScope scope = new ReadScope();
            scope.rememberMetadata(cache, key(1, 2, 9), 0, 7, KEY, -1, null);
            start.await();
            final boolean claimed = cache.claimPointPromotion(pageKey, 0, new byte[] {distinct}, scope);
            if (claimed) {
              scope.finish(true);
              assertNull(cache.getAndGuard(key(1, 2, 9)), "promotion spends the metadata allowance too");
            } else {
              scope.finish(false);
            }
            return claimed;
          }));
        }
        start.countDown();
        int winners = 0;
        for (final Future<Boolean> claim : claims) {
          if (claim.get())
            winners++;
        }
        assertEquals(1, winners);
      }
      cache.admit(pageKey, 0, 7, new byte[] {99}, -1, null);
      assertEquals(weight, cache.getCurrentWeightBytes(), "an admission cannot replace an in-flight claim");
      final HOTMiniPage old = cache.getAndGuard(pageKey);
      assertNotNull(old);
      assertEquals(HOTMiniPageCache.POINT_PROMOTION_DISTINCT_KEYS - 1, old.getEntryCount());
      assertArrayEquals(VALUE, old.copyEntry(old.find(new byte[] {0}, -1)).value());
      cache.discard(pageKey);
      assertFalse(old.isClosed(), "the immutable prefix stays alive through a pinned reader");
      old.releaseGuard();
      assertTrue(old.isClosed());
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {100}, null),
          "invalidation rejects the previous generation");
    } finally {
      cache.clear();
    }
  }

  @Test
  void appendCopiesOnlyTheDirectoryUntilGrowthAndForksNeverOverwritePublishedBytes()
      throws ReflectiveOperationException {
    final Field packed = HOTMiniPage.class.getDeclaredField("data");
    packed.setAccessible(true);
    final HOTMiniPage first = HOTMiniPage.append(null, 19, 7, new byte[] {1}, 42, new HOTLeafEntry(VALUE, null));
    assertNotNull(first);
    assertTrue(first.acquireGuard());
    final byte[] original = (byte[]) packed.get(first);
    final byte[] prefix = Arrays.copyOf(original, 18);
    final HOTMiniPage second = HOTMiniPage.append(first, 19, 7, new byte[] {2}, 42, new HOTLeafEntry(KEY, null));
    final HOTMiniPage fork = HOTMiniPage.append(first, 19, 7, new byte[] {3}, 42, new HOTLeafEntry(new byte[0], null));
    assertNotNull(second);
    assertNotNull(fork);
    try {
      assertSame(original, packed.get(second), "no existing payload bytes are copied within reserved capacity");
      assertNotSame(original, packed.get(fork), "an old-view fork cannot overwrite another publication's tail");
      assertArrayEquals(prefix, Arrays.copyOf(original, prefix.length));
      assertTrue(first.find(new byte[] {2}, 42) < 0);
      assertTrue(second.find(new byte[] {3}, 42) < 0);
      assertTrue(fork.find(new byte[] {2}, 42) < 0);
      assertArrayEquals(VALUE, first.copyEntry(first.find(new byte[] {1}, 42)).value());
      assertArrayEquals(KEY, second.copyEntry(second.find(new byte[] {2}, 42)).value());
      assertEquals(0, fork.copyEntry(fork.find(new byte[] {3}, 42)).value().length);
      first.retire();
      assertFalse(first.isClosed());
    } finally {
      first.releaseGuard();
      first.retire();
      second.retire();
      fork.retire();
    }
    assertTrue(first.isClosed());
  }

  @Test
  void arbitraryInsertionsPreserveOrderingAndEveryPreviouslyGuardedImage() {
    final Map<Integer, byte[]> expected = new TreeMap<>();
    HOTMiniPage current = null;
    try {
      for (int step = 0; step < 257; step++) {
        final int id = step * 73 % 257;
        final byte[] key = {(byte) (id >>> 8), (byte) id};
        final byte[] value = id % 7 == 0
            ? null
            : new byte[id % 11 == 0
                ? 0
                : 19];
        if (value != null && value.length != 0) {
          Arrays.fill(value, (byte) id);
        }
        final HOTMiniPage previous = current;
        if (previous != null) {
          assertTrue(previous.acquireGuard());
        }
        try {
          current = HOTMiniPage.append(previous, 19, 7, key, 42, value == null
              ? null
              : new HOTLeafEntry(value, null));
          assertNotNull(current);
          if (previous != null) {
            previous.retire();
            assertFalse(previous.isClosed(), "the old immutable view survives while guarded");
            assertTrue(previous.find(key, 42) < 0, "publication cannot mutate the old directory");
            assertPackedRecords(previous, expected);
          }
        } finally {
          if (previous != null) {
            previous.releaseGuard();
            assertTrue(previous.isClosed());
          }
        }
        expected.put(id, value == null
            ? null
            : value.clone());
        if (value != null) {
          Arrays.fill(value, (byte) 0x7f);
        }
        assertTrue(current.acquireGuard());
        try {
          assertPackedRecords(current, expected);
        } finally {
          current.releaseGuard();
        }
      }
    } finally {
      if (current != null) {
        current.retire();
      }
    }
  }

  private static void assertPackedRecords(final HOTMiniPage page, final Map<Integer, byte[]> expected) {
    assertEquals(expected.size(), page.getDistinctKeyCount());
    int ordinal = 0;
    for (final Entry<Integer, byte[]> record : expected.entrySet()) {
      final int id = record.getKey();
      final int slot = page.find(new byte[] {(byte) (id >>> 8), (byte) id}, 42);
      assertEquals(ordinal++, slot, "unsigned ordering survives arbitrary insertion and capacity growth");
      final HOTLeafEntry entry = page.copyEntry(slot);
      if (record.getValue() == null) {
        assertNull(entry, "known absence remains distinct from an empty tombstone");
      } else {
        assertNotNull(entry);
        assertArrayEquals(record.getValue(), entry.value());
      }
    }
  }

  @Test
  void oneAdmissionBudgetPrioritizesDataAndReusesItsUnusedBudgetForOnlyOneMetadataRecord() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference data = key(1, 2, 3);
    final PageReference metadata = key(1, 2, 4);
    final ReadScope first = new ReadScope();
    first.rememberMetadata(cache, metadata, 0, 7, KEY, -1, null);
    cache.admit(data, 0, 7, KEY, -1, new HOTLeafEntry(VALUE, null), first);
    cache.admit(key(1, 2, 5), 0, 7, KEY, -1, null, first);
    first.finish(true);
    assertNull(cache.getAndGuard(metadata));
    assertNull(cache.getAndGuard(key(1, 2, 5)), "even multiple scoped data calls have only one admission");
    final long dataWeight = cache.getCurrentWeightBytes();

    final ReadScope repeated = new ReadScope();
    repeated.rememberMetadata(cache, metadata, 0, 7, KEY, -1, null);
    repeated.rememberMetadata(cache, key(1, 2, 6), 0, 7, KEY, -1, null);
    cache.admit(data, 0, 7, KEY, -1, new HOTLeafEntry(VALUE, null), repeated);
    repeated.finish(true);
    final HOTMiniPage page = cache.getAndGuard(metadata);
    assertNotNull(page);
    assertNull(page.copyEntry(page.find(KEY, -1)));
    assertEquals(dataWeight + page.getActualMemorySize(), cache.getCurrentWeightBytes());
    page.releaseGuard();
    assertNull(cache.getAndGuard(key(1, 2, 6)), "only the first metadata miss can retain a pending copy");
    cache.clear();
    assertTrue(page.isClosed());
    first.finish(true);
    repeated.finish(true);
    assertEquals(0, cache.getCurrentWeightBytes(), "finishing twice cannot resurrect any entry");
  }

  @Test
  void pendingMetadataIsDetachedAndPreservesNewestSideProvenanceAndPresentZeroHash() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference reference = key(1, 2, 3);
    final PageReference side = key(4, 5, 6);
    side.setHash(0L);
    final byte[] slot = KEY.clone();
    final byte[] value = VALUE.clone();
    final ReadScope scope = new ReadScope();
    scope.rememberMetadata(cache, reference, 0, 7, slot, 9, new HOTLeafEntry(value, side));
    assertEquals(0, cache.getCurrentWeightBytes(), "an ambiguous open has no admission authority");
    reference.setKey(99);
    side.setKey(100);
    slot[0] = 101;
    value[0] = 102;
    scope.finish(true);
    final HOTMiniPage page = cache.getAndGuard(key(1, 2, 3));
    assertNotNull(page);
    try {
      final HOTLeafEntry actual = page.copyEntry(page.find(KEY, 9));
      assertArrayEquals(VALUE, actual.value());
      assertEquals(key(4, 5, 6), actual.sideReference());
      assertTrue(actual.sideReference().hasHash());
      assertEquals(0L, actual.sideReference().getHashAsLong());
      assertEquals(7, page.getRevision());
    } finally {
      page.releaseGuard();
      cache.clear();
    }
    assertTrue(page.isClosed());
  }

  @Test
  void scanInvalidationAndPromotionCancelPendingMetadataWithoutHoldingAnyGuard() {
    for (int mode = 0; mode < 4; mode++) {
      final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
      final PageReference key = key(1, 2, 3);
      final ReadScope scope = new ReadScope();
      scope.rememberMetadata(cache, key, cache.generation(), 1, KEY, -1, null);
      switch (mode) {
        case 0 -> scope.finish(false);
        case 1 -> cache.clear();
        case 2 -> cache.invalidate(k -> k.getResourceId() == 2);
        case 3 -> cache.discard(key);
        default -> throw new AssertionError(mode);
      }
      scope.finish(true);
      assertEquals(0, cache.getCurrentWeightBytes());
      assertNull(cache.getAndGuard(key));
      cache.admit(key, cache.generation(), 2, KEY, -1, new HOTLeafEntry(VALUE, null));
      final HOTMiniPage current = cache.getAndGuard(key);
      assertNotNull(current);
      assertArrayEquals(VALUE, current.copyEntry(current.find(KEY, -1)).value(),
          "pending negative metadata cannot leak into a reused offset after invalidation");
      current.releaseGuard();
      cache.clear();
      assertTrue(current.isClosed());
    }
  }

  @Test
  void deferredMetadataRetainsAtMostOneBoundedRecordAndNeverPromotesAtThePackedCap() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference key = key(1, 2, 3);
    final ReadScope scope = new ReadScope();
    scope.rememberMetadata(cache, key, 0, 1, KEY, -1, new HOTLeafEntry(new byte[HOTMiniPage.MAX_DATA_BYTES], null));
    scope.finish(true);
    assertEquals(0, cache.getCurrentWeightBytes());
    final HOTLeafEntry large = new HOTLeafEntry(new byte[HOTMiniPage.MAX_DATA_BYTES - 32], null);
    cache.admit(key, 0, 1, KEY, -1, large);
    final HOTMiniPage full = cache.getAndGuard(key);
    assertNotNull(full);
    final ReadScope capped = new ReadScope();
    capped.rememberMetadata(cache, key, 0, 1, new byte[] {99}, -1, new HOTLeafEntry(new byte[64], null));
    capped.finish(true);
    assertEquals(full.getActualMemorySize(), cache.getCurrentWeightBytes());
    assertTrue(full.find(new byte[] {99}, -1) < 0);
    assertEquals(1, full.getGuardCount(), "only this test's explicit inspection holds a guard");
    full.releaseGuard();
    cache.clear();
    assertTrue(full.isClosed());
  }

  @Test
  void packedRecordsPreserveOrderingAbsenceTombstonesAndSideReferenceChecksums() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference cacheKey = key(1, 2, 3);
    final PageReference side = key(4, 5, 6);
    side.setHash(0L); // A present zero checksum must not turn into an absent checksum.
    assertFalse(cache.admit(cacheKey, 0, 7, KEY, 9, new HOTLeafEntry(VALUE, side)));
    assertFalse(cache.admit(cacheKey, 0, 7, KEY, 8, null));
    assertFalse(cache.admit(cacheKey, 0, 7, new byte[] {0}, 8, new HOTLeafEntry(new byte[0], null)));
    assertFalse(cache.admit(cacheKey, 0, 7, new byte[] {(byte) 255}, 8, new HOTLeafEntry(VALUE, null)));
    side.setKey(99);
    cacheKey.setKey(99); // Cache identity and side-reference provenance are owned, not aliases.
    final HOTMiniPage page = cache.getAndGuard(key(1, 2, 3));
    assertNotNull(page);
    try {
      assertEquals(7, page.getRevision());
      assertNull(page.copyEntry(page.find(KEY, 8)));
      assertEquals(0, page.copyEntry(page.find(new byte[] {0}, 8)).value().length);
      assertArrayEquals(VALUE, page.copyEntry(page.find(new byte[] {(byte) 255}, 8)).value());
      assertTrue(page.find(new byte[] {7, 12}, 9) < 0);
      final HOTLeafEntry copy = page.copyEntry(page.find(KEY, 9));
      assertArrayEquals(VALUE, copy.value());
      assertEquals(key(4, 5, 6), copy.sideReference());
      assertTrue(copy.sideReference().hasHash());
      assertEquals(0L, copy.sideReference().getHashAsLong());
      copy.value()[0] = 99;
      copy.sideReference().setKey(100);
      assertArrayEquals(VALUE, page.copyEntry(page.find(KEY, 9)).value());
      assertEquals(6L, page.copyEntry(page.find(KEY, 9)).sideReference().getKey());
    } finally {
      page.releaseGuard();
      cache.clear();
    }
    assertEquals(0, cache.getCurrentWeightBytes());
    assertTrue(page.isClosed());
  }

  @Test
  void growsFromOneCacheLineAndPromotesAtThePackedCap() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference key = key(1, 2, 3);
    assertFalse(cache.admit(key, 0, 1, KEY, -1, null));
    final HOTMiniPage first = cache.getAndGuard(key);
    assertNotNull(first);
    final long firstWeight = first.getActualMemorySize();
    final int payloadBytes = 1_024;
    final HOTLeafEntry value = new HOTLeafEntry(new byte[payloadBytes], null);
    int admitted = 0;
    while (!cache.admit(key, 0, 1, new byte[] {(byte) admitted}, -1, value)) {
      admitted++;
      assertTrue(admitted <= HOTMiniPage.MAX_DATA_BYTES / payloadBytes);
    }
    assertTrue(admitted > 16, "the former 16-key heuristic is not a promotion trigger");
    assertFalse(first.isClosed(), "replacement must respect an outstanding guard");
    assertFalse(first.acquireGuard(), "a retired image cannot acquire another reader");
    assertNull(first.copyEntry(first.find(KEY, -1)));
    first.releaseGuard();
    assertTrue(first.isClosed());
    assertThrows(IllegalStateException.class, first::releaseGuard);
    final HOTMiniPage full = cache.getAndGuard(key);
    assertNotNull(full);
    assertEquals(HOTMiniPage.MAX_DATA_BYTES - 64, full.getActualMemorySize() - firstWeight);
    full.releaseGuard();
    cache.discard(key);
    assertNull(cache.getAndGuard(key));
    assertEquals(0, cache.getCurrentWeightBytes());
  }

  @Test
  void oversizedFirstSlotAndUnsupportedProvenanceAreReturnedWithoutCachingOrPromotion() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference key = key(1, 2, 3);
    assertFalse(cache.admit(key, 0, 1, KEY, -1, new HOTLeafEntry(new byte[HOTMiniPage.MAX_DATA_BYTES], null)));
    final PageReference pending = key(4, 5, 6).setLogKey(1);
    assertFalse(cache.admit(key, 0, 1, KEY, -1, new HOTLeafEntry(VALUE, pending)));
    assertEquals(0, cache.getCurrentWeightBytes());
    assertNull(cache.getAndGuard(key));
    cache.clear();
  }

  @Test
  void durableLeafOffsetsDatabaseAndResourceIsolateNegativeEntries() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference absent = key(1, 2, 3);
    assertFalse(cache.admit(absent, 0, 1, KEY, -1, null));
    for (final PageReference other : List.of(key(2, 2, 3), key(1, 3, 3), key(1, 2, 4))) {
      assertNull(cache.getAndGuard(other));
      assertFalse(cache.admit(other, 0, 2, KEY, -1, new HOTLeafEntry(VALUE, null)));
      final HOTMiniPage page = cache.getAndGuard(other);
      assertNotNull(page);
      assertArrayEquals(VALUE, page.copyEntry(page.find(KEY, -1)).value());
      page.releaseGuard();
    }
    final HOTMiniPage page = cache.getAndGuard(absent);
    assertNotNull(page);
    assertNull(page.copyEntry(page.find(KEY, -1)));
    page.releaseGuard();
    cache.clear();
  }

  @Test
  void scopedClearAndPromotionFenceInFlightAdmissionsWithoutDrainingGuards() {
    for (final boolean promote : new boolean[] {false, true}) {
      final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
      final PageReference removed = key(1, 2, 3);
      final PageReference sibling = key(1, 3, 3);
      final long generation = cache.generation();
      cache.admit(removed, generation, 1, KEY, -1, null);
      cache.admit(sibling, generation, 1, KEY, -1, null);
      final HOTMiniPage held = cache.getAndGuard(removed);
      assertNotNull(held);
      if (promote) {
        cache.discard(removed);
      } else {
        cache.invalidate(key -> key.getResourceId() == 2);
      }
      assertNull(cache.getAndGuard(removed));
      assertFalse(held.isClosed());
      assertFalse(held.acquireGuard());
      assertNull(held.copyEntry(held.find(KEY, -1)));
      cache.admit(removed, generation, 1, KEY, -1, null);
      assertNull(cache.getAndGuard(removed), "an old read must not resurrect the removed answer");
      final HOTMiniPage retained = cache.getAndGuard(sibling);
      assertNotNull(retained);
      retained.releaseGuard();
      held.releaseGuard();
      assertTrue(held.isClosed());
      cache.admit(removed, cache.generation(), 2, KEY, -1, new HOTLeafEntry(VALUE, null));
      final HOTMiniPage replacement = cache.getAndGuard(removed);
      assertNotNull(replacement);
      assertArrayEquals(VALUE, replacement.copyEntry(replacement.find(KEY, -1)).value());
      replacement.releaseGuard();
      cache.clear();
    }
  }

  @Test
  void concurrentAdmissionsRetainEverySlotAndReadersSurviveReplacement() throws Exception {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference key = key(1, 2, 3);
    final CountDownLatch start = new CountDownLatch(1);
    try (final var workers = Executors.newFixedThreadPool(4)) {
      final List<Future<?>> results = new ArrayList<>();
      for (int thread = 0; thread < 4; thread++) {
        final int partition = thread;
        results.add(workers.submit(() -> {
          start.await();
          for (int i = 0; i < 32; i++) {
            final byte[] slot = {(byte) partition, (byte) i};
            assertFalse(cache.admit(key, 0, 1, slot, -1, new HOTLeafEntry(slot, null)));
            final HOTMiniPage page = cache.getAndGuard(key);
            assertNotNull(page);
            try {
              assertFalse(page.isClosed());
              assertArrayEquals(slot, page.copyEntry(page.find(slot, -1)).value());
            } finally {
              page.releaseGuard();
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> result : results) {
        result.get();
      }
    }
    final HOTMiniPage page = cache.getAndGuard(key);
    assertNotNull(page);
    for (int thread = 0; thread < 4; thread++) {
      for (int i = 0; i < 32; i++) {
        final byte[] slot = {(byte) thread, (byte) i};
        assertArrayEquals(slot, page.copyEntry(page.find(slot, -1)).value());
      }
    }
    assertEquals(page.getActualMemorySize(), cache.getCurrentWeightBytes());
    page.releaseGuard();
    cache.clear();
    assertEquals(0, cache.getCurrentWeightBytes());
  }

  @Test
  void evictionAccountsEveryAllocationAndBufferManagerKeepsTheExistingHotBudget() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(4_096);
    for (int i = 0; i < 100; i++) {
      cache.admit(key(1, 2, i), cache.generation(), 1, KEY, -1, null);
    }
    assertTrue(cache.getCurrentWeightBytes() <= 4_096);
    cache.evictUnderPressure(); // First pass clears recently used pages' HOT bits.
    cache.evictUnderPressure(); // Second pass can evict the unguarded cold pages.
    assertTrue(cache.getCurrentWeightBytes() <= 4_096 * 3 / 4);
    cache.clear();
    assertEquals(0, cache.getCurrentWeightBytes());
    try (final BufferManagerImpl manager = new BufferManagerImpl(1L << 20, 1L << 28, 1L << 20, 4, 4, 4)) {
      assertEquals(1L << 26, manager.getHOTLeafPageCacheMaxWeightBytes()
          + manager.getHOTLeafFragmentCacheMaxWeightBytes() + manager.getHOTMiniPageCacheMaxWeightBytes());
      final HOTMiniPageCache mini = manager.getHOTMiniPageCache();
      final PageReference key = key(1, 2, 3);
      mini.admit(key, mini.generation(), 1, KEY, -1, null);
      manager.clearCachesForResource(1, 2);
      assertNull(mini.getAndGuard(key));
      mini.admit(key, mini.generation(), 1, KEY, -1, null);
      manager.clearCachesForDatabase(1);
      assertNull(mini.getAndGuard(key));
      mini.admit(key, mini.generation(), 1, KEY, -1, null);
      manager.clearAllCaches();
      assertEquals(0, mini.getCurrentWeightBytes());
    }
    assertThrows(IllegalArgumentException.class, () -> new HOTMiniPageCache(0));
    assertEquals(0, HOTMiniPageCache.disabled().getMaxWeightBytes());
    assertFalse(HOTMiniPageCache.disabled().admit(key(1, 2, 3), 0, 1, KEY, -1, null));
  }
}
