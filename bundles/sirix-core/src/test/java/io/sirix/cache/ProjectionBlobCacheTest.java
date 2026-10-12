/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.hot.cache.HOTMiniPageCache.ReadScope;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionBlobCacheTest {
  private static ProjectionBlobCache.Key key(final long database, final long resource, final long offset,
      final int revision, final long slot) {
    return new ProjectionBlobCache.Key(database, resource, offset, 91, revision, slot);
  }

  @Test
  void ownsBothSidesOfItsArrayBoundaryAndSeparatesRoutingIdentities() {
    final ProjectionBlobCache cache = new ProjectionBlobCache(1 << 20);
    final ProjectionBlobCache.Key key = key(1, 2, 3, 4, 5);
    final byte[] bytes = {1, 2, 3};
    assertTrue(cache.put(key, bytes, cache.generation()));
    bytes[0] = 9;
    final byte[] hit = cache.get(key);
    assertArrayEquals(new byte[] {1, 2, 3}, hit);
    Objects.requireNonNull(hit)[1] = 8;
    assertArrayEquals(new byte[] {1, 2, 3}, cache.get(key));
    assertNull(cache.get(key(2, 2, 3, 4, 5)));
    assertNull(cache.get(key(1, 3, 3, 4, 5)));
    assertNull(cache.get(key(1, 2, 4, 4, 5)));
    assertNull(cache.get(key(1, 2, 3, 5, 5)));
    assertNull(cache.get(key(1, 2, 3, 4, 6)));
    assertNull(cache.get(new ProjectionBlobCache.Key(1, 2, 3, 92, 4, 5)));
  }

  @Test
  void repeatedLargeScansAndRejectedEntriesRespectTheByteCeiling() {
    final ProjectionBlobCache cache = new ProjectionBlobCache(128 * 1024);
    final long empty = cache.getCurrentWeightBytes();
    final byte[] payload = new byte[8192];
    for (int pass = 0; pass < 3; pass++) {
      for (int slot = 0; slot < 1024; slot++) {
        assertTrue(cache.put(key(1, 2, 3, 4, slot), payload, cache.generation()));
        assertTrue(cache.getCurrentWeightBytes() <= cache.getMaxWeightBytes());
      }
    }
    assertTrue(cache.getCurrentWeightBytes() > empty, "the budget is exercised by retained payloads");
    assertFalse(cache.put(key(1, 2, 3, 4, 1024), new byte[32769], cache.generation()));
    cache.clear();
    assertEquals(empty, cache.getCurrentWeightBytes(), "clearing leaves only the accounted fixed table");
  }

  @Test
  void resourceInvalidationFencesAReadThatFinishesAfterOffsetsAreReused() throws Exception {
    final ProjectionBlobCache cache = new ProjectionBlobCache(1 << 20);
    final ProjectionBlobCache.Key removed = key(1, 2, 3, 4, 5);
    final ProjectionBlobCache.Key other = key(1, 7, 3, 4, 5);
    assertTrue(cache.put(other, new byte[] {7}, cache.generation()));
    final CountDownLatch captured = new CountDownLatch(1);
    final CountDownLatch resolved = new CountDownLatch(1);
    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      final var stale = executor.submit(() -> {
        final long generation = cache.generation();
        captured.countDown();
        resolved.await();
        return cache.put(removed, new byte[] {1}, generation);
      });
      captured.await();
      try {
        cache.invalidate(reference -> reference.getDatabaseId() == 1 && reference.getResourceId() == 2);
        assertTrue(cache.put(removed, new byte[] {2}, cache.generation()));
      } finally {
        resolved.countDown();
      }
      assertFalse(stale.get());
    }
    assertArrayEquals(new byte[] {2}, cache.get(removed));
    assertArrayEquals(new byte[] {7}, cache.get(other));
    cache.invalidate(reference -> reference.getDatabaseId() == 1);
    assertNull(cache.get(removed));
    assertNull(cache.get(other));
  }

  @Test
  void onePointSeekAdmitsDataOrDeferredMetadataAndAnAbandonedOpenAdmitsNothing() {
    final ProjectionBlobCache cache = new ProjectionBlobCache(1 << 20);
    final ProjectionBlobCache.Key metadata = key(1, 2, 3, 4, 5);
    final ProjectionBlobCache.Key data = key(1, 2, 3, 4, 6);
    final ReadScope scope = new ReadScope();
    scope.retainBlob(cache, metadata, new byte[] {1}, cache.generation(), false);
    assertNull(cache.get(metadata));
    scope.retainBlob(cache, data, new byte[] {2}, cache.generation(), true);
    scope.finish(true);
    assertNotNull(cache.get(data));
    assertNull(cache.get(metadata));
    final ReadScope next = new ReadScope();
    final byte[] bytes = {3};
    next.retainBlob(cache, metadata, bytes, cache.generation(), false);
    bytes[0] = 9;
    next.finish(true);
    assertArrayEquals(new byte[] {3}, cache.get(metadata));
    cache.clear();
    final ReadScope scan = new ReadScope();
    scan.retainBlob(cache, metadata, bytes, cache.generation(), false);
    scan.finish(false);
    assertNull(cache.get(metadata));
  }

  @Test
  void rejectedDataAdmissionLeavesTheSingleOpportunityForMetadata() {
    final ProjectionBlobCache cache = new ProjectionBlobCache(4096);
    final ReadScope scope = new ReadScope();
    final ProjectionBlobCache.Key metadata = key(1, 2, 3, 4, 5);
    scope.retainBlob(cache, metadata, new byte[] {1}, cache.generation(), false);
    scope.retainBlob(cache, key(1, 2, 3, 4, 6), new byte[8192], cache.generation(), true);
    scope.finish(true);
    assertNotNull(cache.get(metadata));
    assertEquals(0, ProjectionBlobCache.disabled().getCurrentWeightBytes());
    assertFalse(ProjectionBlobCache.disabled().put(metadata, new byte[1], 0));
  }
}
