/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RetainedProbeChunkTest {
  private static final String RETAIN_PROBE = GroupTableSpill.RETAIN_PROBE_PROPERTY;
  private int previousRetain;
  private int previousPool;
  private long previousCeiling;
  private String previousProbeProperty;
  private String previousDenseProperty;

  @BeforeEach
  void preparePools() {
    LongChunkPool.releaseShared();
    previousRetain = LongChunkPool.setRetainForTesting(1);
    previousPool = GroupTableSpill.setChunkPoolForTesting(1);
    previousCeiling = LongChunkPool.setRetainBytesForTesting(32L << 20);
    previousProbeProperty = System.getProperty(RETAIN_PROBE);
    previousDenseProperty = System.getProperty(GroupTableSpill.DENSE_INDEX_PROPERTY);
    System.setProperty(RETAIN_PROBE, "true");
    System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "true");
  }

  @AfterEach
  void restorePools() {
    LongChunkPool.releaseShared();
    LongChunkPool.setRetainForTesting(previousRetain);
    GroupTableSpill.setChunkPoolForTesting(previousPool);
    LongChunkPool.setRetainBytesForTesting(previousCeiling);
    restoreProperty(RETAIN_PROBE, previousProbeProperty);
    restoreProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, previousDenseProperty);
  }

  @Test
  void companionSurvivesPayloadGeometryChangesAndOrdinaryScansEvictBoth() {
    final LongChunkPool payload = LongChunkPool.sharedWithProbe(97, 8, 127);
    final LongChunkPool index = LongChunkPool.sharedProbe(127, 8, 97);
    final long[] recordChunk = payload.take();
    final long[] indexChunk = index.take();
    Arrays.fill(recordChunk, 41L);
    Arrays.fill(indexChunk, 73L);
    assertTrue(payload.give(recordChunk));
    assertTrue(index.give(indexChunk));
    assertSame(payload, LongChunkPool.sharedWithProbe(97, 8, 127));
    assertSame(index, LongChunkPool.sharedProbe(127, 8, 97));
    assertSame(recordChunk, payload.take());
    assertSame(indexChunk, index.take());
    for (final long value : recordChunk) {
      assertEquals(0L, value);
    }
    for (final long value : indexChunk) {
      assertEquals(0L, value);
    }
    assertTrue(payload.give(recordChunk));
    assertTrue(index.give(indexChunk));
    final LongChunkPool next = LongChunkPool.sharedWithProbe(103, 8, 127);
    assertSame(index, LongChunkPool.sharedProbe(127, 8, 103));
    assertEquals(0, payload.pooled());
    assertEquals(1, index.pooled());
    assertTrue(next.give(new long[103]));
    assertEquals((103L + 127L) * Long.BYTES, LongChunkPool.retainedBytes());
    LongChunkPool.shared(107, 8);
    assertEquals(0, next.pooled());
    assertEquals(0, index.pooled());
    assertEquals(0L, LongChunkPool.retainedBytes());
  }

  @Test
  void equalLengthPayloadAndIndexPoolsKeepSeparateOwnership() {
    final LongChunkPool payload = LongChunkPool.sharedWithProbe(109, 8, 109);
    final LongChunkPool index = LongChunkPool.sharedProbe(109, 8, 109);
    assertNotSame(payload, index);
    final long[] records = payload.take();
    final long[] pointers = index.take();
    assertNotSame(records, pointers);
    Arrays.fill(records, 91L);
    assertTrue(index.give(pointers));
    assertSame(pointers, index.take());
    for (final long value : records) {
      assertEquals(91L, value, "recycling an index must not clear live records");
    }
    assertEquals(0, payload.pooled());
  }

  @Test
  void payloadAndIndexShareOneRetainedByteCeiling() {
    LongChunkPool.setRetainBytesForTesting((113L + 127L) * Long.BYTES);
    final LongChunkPool payload = LongChunkPool.sharedWithProbe(113, 8, 127);
    final LongChunkPool index = LongChunkPool.sharedProbe(127, 8, 113);
    assertTrue(payload.give(new long[113]));
    assertTrue(index.give(new long[127]));
    assertFalse(payload.give(new long[113]));
    assertFalse(index.give(new long[127]));
    assertEquals((113L + 127L) * Long.BYTES, LongChunkPool.retainedBytes());
    payload.take();
    assertFalse(index.give(new long[127]), "the released payload chunk leaves less than one index chunk free");
    index.take();
    assertEquals(0L, LongChunkPool.retainedBytes());
    assertTrue(index.give(new long[127]));
    assertTrue(payload.give(new long[113]));
  }

  @Test
  void concurrentBorrowersNeverReceiveOneLiveArrayTwice() throws Exception {
    final LongChunkPool payload = LongChunkPool.sharedWithProbe(131, 16, 137);
    final LongChunkPool index = LongChunkPool.sharedProbe(137, 16, 131);
    final Set<long[]> live = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> tasks = new ArrayList<>(6);
    try (final var executor = Executors.newFixedThreadPool(6)) {
      for (int worker = 0; worker < 6; worker++) {
        final int owner = worker + 1;
        tasks.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          final LongChunkPool pool = (owner & 1) == 0
              ? payload
              : index;
          for (int round = 0; round < 500; round++) {
            final long[] chunk = pool.take();
            assertTrue(live.add(chunk), "an array must have exactly one live owner");
            for (final long value : chunk) {
              assertEquals(0L, value);
            }
            Arrays.fill(chunk, owner);
            if (round % 31 == 0) {
              LongChunkPool.sharedWithProbe(131, 16, 137);
              LongChunkPool.sharedProbe(137, 16, 131);
            }
            for (final long value : chunk) {
              assertEquals(owner, value);
            }
            assertTrue(live.remove(chunk));
            pool.give(chunk);
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> task : tasks) {
        task.get(20, TimeUnit.SECONDS);
      }
    }
    assertTrue(live.isEmpty());
    assertEquals(((long) payload.pooled() * 131 + (long) index.pooled() * 137) * Long.BYTES,
        LongChunkPool.retainedBytes());
    assertTrue(LongChunkPool.retainedBytes() <= LongChunkPool.retainBytes());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedSpillsReuseOnlyReleasedIndexesAndKeepGroupsExact(final boolean retainProbe) {
    System.setProperty(RETAIN_PROBE, Boolean.toString(retainProbe));
    final GroupTableSpill first = spill();
    final LongChunkPool firstPool = first.probeChunkPool();
    assertEquals(retainProbe, firstPool.isShared());
    fillAndVerify(first, 40_000, 5L);
    first.releaseTables();
    final long hits = firstPool.hits();
    if (retainProbe) {
      assertTrue(firstPool.pooled() > 0);
    } else {
      assertEquals(0, firstPool.pooled());
    }
    final GroupTableSpill second = spill();
    if (retainProbe) {
      assertSame(firstPool, second.probeChunkPool());
    } else {
      assertNotSame(firstPool, second.probeChunkPool());
    }
    fillAndVerify(second, 45_000, 9L);
    second.releaseTables();
    if (retainProbe) {
      assertTrue(firstPool.hits() > hits);
    }
  }

  @Test
  void disabledCrossScanRetentionKeepsBothPoolsLocal() {
    LongChunkPool.setRetainForTesting(0);
    final GroupTableSpill spill = spill();
    assertFalse(spill.chunkPool().isShared());
    assertFalse(spill.probeChunkPool().isShared());
    fillAndVerify(spill, 20_000, 3L);
    spill.releaseTables();
    assertEquals(0, spill.chunkPool().pooled());
    assertEquals(0, spill.probeChunkPool().pooled());
    assertEquals(0L, LongChunkPool.retainedBytes());
  }

  @Test
  void runtimeDefaultAndExplicitOverridesSelectTheExpectedPoolLifetime() {
    System.clearProperty(RETAIN_PROBE);
    final boolean nativeImage = System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    assertEquals(nativeImage, GroupTableSpill.retainProbeAcrossScans());
    final GroupTableSpill defaultSpill = spill();
    assertEquals(nativeImage, defaultSpill.probeChunkPool().isShared());
    defaultSpill.releaseTables();
    for (final boolean enabled : new boolean[] {true, false}) {
      System.setProperty(RETAIN_PROBE, Boolean.toString(enabled));
      assertEquals(enabled, GroupTableSpill.retainProbeAcrossScans());
      final GroupTableSpill configured = spill();
      assertEquals(enabled, configured.probeChunkPool().isShared());
      configured.releaseTables();
    }
  }

  @Test
  void invalidCompanionDimensionsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedWithProbe(0, 8, 127));
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedWithProbe(97, 0, 127));
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedWithProbe(97, 8, 0));
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedProbe(0, 8, 97));
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedProbe(127, 0, 97));
    assertThrows(IllegalArgumentException.class, () -> LongChunkPool.sharedProbe(127, 8, 0));
  }

  private static GroupTableSpill spill() {
    return new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(0, hint, true, 0L, 3), 50_000L, 0, 1, 100_000L);
  }

  private static void fillAndVerify(final GroupTableSpill spill, final int groups, final long count) {
    final NumericGroupAggTable table = spill.freshLocal();
    final long[] identity = new long[3];
    for (int group = 0; group < groups; group++) {
      identity[0] = group;
      identity[1] = ~group;
      identity[2] = group % 17;
      final int handle = table.acquireExact(group % 8191, group + 1L, identity, 0);
      final long[] values = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(0L, values[offset]);
      values[offset] = count;
      table.setAuxAtAccBase(handle, group * 3L);
    }
    assertEquals(groups, table.size());
    for (int group = groups - 1; group >= 0; group--) {
      identity[0] = group;
      identity[1] = ~group;
      identity[2] = group % 17;
      final int handle = table.acquireExact(group % 8191, Long.MAX_VALUE, identity, 0);
      final long[] values = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(count, values[offset]);
      assertEquals(group + 1L, values[offset + 1]);
      assertEquals(group * 3L, table.auxAtAccBase(handle));
    }
    table.release();
  }

  private static void restoreProperty(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }
}
