/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

@Isolated
final class ProjectionParallelDecodeLifetimeTest {

  private static final int LEAVES = 128;
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void decoderErrorDrainsSiblingBeforeReleasingSlab(final boolean masked) throws InterruptedException {
    final MemorySegmentAllocator allocator = Allocators.getInstance();
    final MemorySegmentAllocator observed = mock(MemorySegmentAllocator.class);
    final AtomicReference<MemorySegment> allocation = new AtomicReference<>();
    final AtomicBoolean armed = new AtomicBoolean();
    final AtomicBoolean blockerClaimed = new AtomicBoolean();
    final AtomicBoolean blockerReturned = new AtomicBoolean();
    final AtomicBoolean errorClaimed = new AtomicBoolean();
    final AtomicBoolean prematureRelease = new AtomicBoolean();
    final AtomicInteger releases = new AtomicInteger();
    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch finishBlocker = new CountDownLatch(1);
    final CountDownLatch errorInjected = new CountDownLatch(1);
    final CountDownLatch releaseAttempted = new CountDownLatch(1);
    final Thread caller = Thread.currentThread();
    final OutOfMemoryError injected = new OutOfMemoryError("decoder presence allocation");
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    final Map<Long, byte[]> segments = new HashMap<>();
    final Random random = new Random(20261111L);
    long nextOffset = 1_000L;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final long[] values = new long[KINDS.length];
      final boolean[] booleans = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      for (int row = 0; row < 200 - leaf; row++) {
        values[0] = random.nextLong();
        page.appendRow(leaf * 1_000L + row + 1, values, booleans, strings);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int[] ids = encoded.columnSegmentIds().clone();
      final long[] offsets = new long[ids.length];
      for (int segment = 0; segment < ids.length; segment++) {
        offsets[segment] = nextOffset;
        segments.put(nextOffset, encoded.segments()[segment]);
        nextOffset += 1L + encoded.segments()[segment].length;
      }
      final byte[] descriptor = encoded.descriptor();
      final RowGroupDirectory directory =
          spy(new RowGroupDirectory(leaf + 1L, descriptor, ids, offsets, new byte[ids.length][]));
      doAnswer(invocation -> {
        if (armed.get()) {
          if (Thread.currentThread() != caller && blockerClaimed.compareAndSet(false, true)) {
            blockerStarted.countDown();
            try {
              finishBlocker.await();
            } finally {
              blockerReturned.set(true);
            }
          } else if (Thread.currentThread() == caller && errorClaimed.compareAndSet(false, true)) {
            blockerStarted.await();
            errorInjected.countDown();
            throw injected;
          }
        }
        return descriptor;
      }).when(directory).descriptor();
      directories.add(directory);
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] fetched = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        fetched[i] = segments.get(wanted[i]);
      }
      if (masked) {
        armed.set(true);
      }
      return fetched;
    };
    doAnswer(invocation -> {
      final MemorySegment slab = allocator.allocateSlab(invocation.getArgument(0));
      allocation.set(slab);
      if (!masked) {
        armed.set(true);
      }
      return slab;
    }).when(observed).allocateSlab(anyLong());
    doAnswer(invocation -> {
      prematureRelease.compareAndSet(false, !blockerReturned.get());
      releases.incrementAndGet();
      releaseAttempted.countDown();
      return null;
    }).when(observed).releaseSlab(any(MemorySegment.class));
    final Thread controller = new Thread(() -> {
      try {
        errorInjected.await();
        releaseAttempted.await(1L, TimeUnit.SECONDS);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } finally {
        finishBlocker.countDown();
      }
    }, "parallel-decode-release-observer");
    final long previousHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    final long previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    final boolean previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    controller.start();
    try (MockedStatic<Allocators> allocators = mockStatic(Allocators.class)) {
      allocators.when(Allocators::getInstance).thenReturn(observed);
      try {
        final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> {
          if (masked) {
            store.columnMasked(0, fetcher, new long[] {-1L, -1L});
          } else {
            store.column(0, fetcher);
          }
        });
        assertSame(injected, failure);
        controller.join();
        assertTrue(blockerClaimed.get());
        assertTrue(blockerReturned.get());
        assertEquals(1, releases.get());
        assertFalse(prematureRelease.get(), "a sibling decoder must finish before its shared slab is released");
        assertFalse(store.columnFilled(0));
        assertFalse(store.columnKnownCorrupt(0));
        assertNotNull(allocation.get());
      } finally {
        finishBlocker.countDown();
        controller.interrupt();
        controller.join();
        while (!ForkJoinPool.commonPool().awaitQuiescence(1L, TimeUnit.SECONDS)) {
          Thread.onSpinWait();
        }
        store.retire();
      }
    } finally {
      final MemorySegment slab = allocation.get();
      if (slab != null) {
        allocator.releaseSlab(slab);
      }
      ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
      HeapHeadroom.setHeadroomForTesting(previousHeadroom);
      ProjectionColumnStore.sampleHeadroomShare();
    }
  }
}
