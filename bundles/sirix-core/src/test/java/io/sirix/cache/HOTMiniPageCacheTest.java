/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {8}));
      cache.admit(pageKey, 0, 7, new byte[] {8}, -1, null);
      cache.admit(pageKey, 0, 7, new byte[] {9}, -1, null);
      assertFalse(cache.claimPointPromotion(pageKey, 0, KEY),
          "another side-reference variant still asks for the same serialized key");
      assertTrue(cache.claimPointPromotion(pageKey, 0, new byte[] {10}));
    } finally {
      cache.clear();
    }
  }

  @Test
  void distinctPointPromotionHasExactlyOneClaimUnderConcurrentMisses() throws Exception {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference pageKey = key(1, 2, 3);
    try {
      for (int i = 0; i < HOTMiniPageCache.POINT_PROMOTION_DISTINCT_KEYS - 1; i++) {
        assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {(byte) i}));
        cache.admit(pageKey, 0, 7, new byte[] {(byte) i}, -1, new HOTLeafEntry(VALUE, null));
      }
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {0}),
          "a repeated key does not spend another distinct miss");
      final long weight = cache.getCurrentWeightBytes();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<Boolean>> claims = new ArrayList<>();
      try (var pool = Executors.newFixedThreadPool(8)) {
        for (int i = 0; i < 8; i++) {
          final byte distinct = (byte) (i + 16);
          claims.add(pool.submit(() -> {
            start.await();
            return cache.claimPointPromotion(pageKey, 0, new byte[] {distinct});
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
      assertFalse(cache.claimPointPromotion(pageKey, 0, new byte[] {100}),
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
  void everyInvalidationRejectsAnAdmissionReadBeforeItWithoutHoldingAnyGuard() {
    for (int mode = 0; mode < 3; mode++) {
      final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
      final PageReference key = key(1, 2, 3);
      final long stale = cache.generation(key);
      switch (mode) {
        case 0 -> cache.clear();
        case 1 -> cache.invalidate(k -> k.getResourceId() == 2);
        case 2 -> cache.discard(key);
        default -> throw new AssertionError(mode);
      }
      cache.admit(key, stale, 1, KEY, -1, new HOTLeafEntry(new byte[] {99}, null));
      assertEquals(0, cache.getCurrentWeightBytes());
      assertNull(cache.getAndGuard(key));
      cache.admit(key, cache.generation(key), 2, KEY, -1, new HOTLeafEntry(VALUE, null));
      final HOTMiniPage current = cache.getAndGuard(key);
      assertNotNull(current);
      assertArrayEquals(VALUE, current.copyEntry(current.find(KEY, -1)).value(),
          "a stale admission cannot leak into a reused offset after invalidation");
      current.releaseGuard();
      cache.clear();
      assertTrue(current.isClosed());
    }
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
      final long generation = cache.generation(removed);
      cache.admit(removed, generation, 1, KEY, -1, null);
      cache.admit(sibling, cache.generation(sibling), 1, KEY, -1, null);
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
      cache.admit(removed, cache.generation(removed), 2, KEY, -1, new HOTLeafEntry(VALUE, null));
      final HOTMiniPage replacement = cache.getAndGuard(removed);
      assertNotNull(replacement);
      assertArrayEquals(VALUE, replacement.copyEntry(replacement.find(KEY, -1)).value());
      replacement.releaseGuard();
      cache.clear();
    }
  }

  @Test
  void aPromotionInOneResourceCannotRejectAnAdmissionInAnotherEvenWhenTheKeysCollide() {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference promoted = key(1, 2, 3);
    try {
      // Same durable offset, other resources: the fence hashes the resource in, so a promotion in
      // one cannot be mistaken for a promotion in another.
      int survived = 0;
      for (long resource = 3; resource < 67; resource++) {
        final PageReference sibling = key(1, resource, 3);
        final long siblingGeneration = cache.generation(sibling);
        cache.discard(promoted);
        cache.admit(sibling, siblingGeneration, 1, KEY, -1, new HOTLeafEntry(VALUE, null));
        final HOTMiniPage admitted = cache.getAndGuard(sibling);
        if (admitted != null) {
          assertArrayEquals(VALUE, admitted.copyEntry(admitted.find(KEY, -1)).value());
          admitted.releaseGuard();
          survived++;
        }
      }
      assertTrue(survived >= 60,
          "promoting one resource's leaf rejected admissions for the same offset in other resources: " + survived
              + " of 64 survived");
    } finally {
      cache.clear();
    }
  }

  @Test
  void aPromotionFencesItsOwnKeyUnderARaceAndLeavesUnrelatedLeavesAdmitting() throws Exception {
    final HOTMiniPageCache cache = new HOTMiniPageCache(1L << 20);
    final PageReference promoted = key(1, 2, 3);
    try {
      final PageReference[] unrelated = new PageReference[64];
      final long[] captured = new long[unrelated.length];
      for (int i = 0; i < unrelated.length; i++) {
        unrelated[i] = key(1, 2, 1_000 + i);
        captured[i] = cache.generation(unrelated[i]);
      }
      cache.discard(promoted);
      int survived = 0;
      for (int i = 0; i < unrelated.length; i++) {
        cache.admit(unrelated[i], captured[i], 1, KEY, -1, new HOTLeafEntry(VALUE, null));
        final HOTMiniPage page = cache.getAndGuard(unrelated[i]);
        if (page != null) {
          assertArrayEquals(VALUE, page.copyEntry(page.find(KEY, -1)).value());
          page.releaseGuard();
          survived++;
        }
      }
      assertTrue(survived >= unrelated.length / 2,
          "promoting one leaf rejected resolutions of unrelated leaves read before it: " + survived + " of "
              + unrelated.length + " survived");

      try (final var workers = Executors.newFixedThreadPool(2)) {
        for (int round = 0; round < 64; round++) {
          final PageReference raced = key(1, 2, 10_000 + round);
          final long generation = cache.generation(raced);
          final CountDownLatch start = new CountDownLatch(1);
          final Future<?> admission = workers.submit(() -> {
            start.await();
            cache.admit(raced, generation, 1, KEY, -1, new HOTLeafEntry(VALUE, null));
            return null;
          });
          final Future<?> promotion = workers.submit(() -> {
            start.await();
            cache.discard(raced);
            return null;
          });
          start.countDown();
          admission.get();
          promotion.get();
          assertNull(cache.getAndGuard(raced),
              "a resolution read before the complete-page promotion outlived it in round " + round);
          assertNotEquals(generation, cache.generation(raced), "the promoted key's own fence must advance");
        }
      }
    } finally {
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
      final PageReference evicted = key(1, 2, i);
      cache.admit(evicted, cache.generation(evicted), 1, KEY, -1, null);
    }
    assertTrue(cache.getCurrentWeightBytes() <= 4_096);
    cache.evictUnderPressure(); // First pass clears recently used pages' HOT bits.
    cache.evictUnderPressure(); // Second pass can evict the unguarded cold pages.
    assertTrue(cache.getCurrentWeightBytes() <= 4_096 * 3 / 4);
    cache.clear();
    assertEquals(0, cache.getCurrentWeightBytes());
    try (final BufferManagerImpl manager = new BufferManagerImpl(1L << 20, 1L << 28, 1L << 20, 4, 4, 4)) {
      assertEquals(1L << 26,
          manager.getHOTLeafPageCacheMaxWeightBytes() + manager.getNativeHOTLeafFragmentCacheMaxWeightBytes()
              + manager.getHOTMiniPageCacheMaxWeightBytes(),
          "the off-heap HOT allowance covers the complete leaves, the native fragments and the slots");
      final HOTMiniPageCache mini = manager.getHOTMiniPageCache();
      final PageReference key = key(1, 2, 3);
      mini.admit(key, mini.generation(key), 1, KEY, -1, null);
      manager.clearCachesForResource(1, 2);
      assertNull(mini.getAndGuard(key));
      mini.admit(key, mini.generation(key), 1, KEY, -1, null);
      manager.clearCachesForDatabase(1);
      assertNull(mini.getAndGuard(key));
      mini.admit(key, mini.generation(key), 1, KEY, -1, null);
      manager.clearAllCaches();
      assertEquals(0, mini.getCurrentWeightBytes());
    }
    assertThrows(IllegalArgumentException.class, () -> new HOTMiniPageCache(0));
    assertEquals(0, HOTMiniPageCache.disabled().getMaxWeightBytes());
    assertFalse(HOTMiniPageCache.disabled().admit(key(1, 2, 3), 0, 1, KEY, -1, null));
  }
}
