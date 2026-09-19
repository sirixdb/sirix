package io.sirix.io.filechannel;

import io.sirix.io.filechannel.FileChannelReader.BufferPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FileChannelBufferPoolTest {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void boundedRetentionAndDraining(final boolean lockFree) {
    final BufferPool pool = new BufferPool(3, lockFree);
    assertNull(pool.poll());
    final Set<ByteBuffer> expected = Collections.newSetFromMap(new IdentityHashMap<>());
    for (int i = 0; i < 3; i++) {
      final ByteBuffer buffer = ByteBuffer.allocate(8);
      expected.add(buffer);
      assertTrue(pool.offer(buffer));
    }
    assertFalse(pool.offer(ByteBuffer.allocate(8)));
    for (int i = 0; i < 3; i++) {
      assertTrue(expected.remove(pool.poll()));
    }
    assertTrue(expected.isEmpty());
    assertNull(pool.poll());
    assertThrows(NullPointerException.class, () -> pool.offer(null));
  }

  @Test
  void invalidCapacitiesFailBeforeAllocation() {
    for (final int capacity : new int[] {0, -1, Integer.MAX_VALUE}) {
      assertThrows(IllegalArgumentException.class, () -> new BufferPool(capacity, true));
      assertThrows(IllegalArgumentException.class, () -> new BufferPool(capacity, false));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7, 40})
  void concurrentBorrowersOwnDistinctBuffersAndSeePublishedContents(final int capacity) throws Exception {
    final BufferPool pool = new BufferPool(capacity, true);
    for (int i = 0; i < capacity; i++) {
      final ByteBuffer buffer = (i & 1) == 0
          ? ByteBuffer.allocateDirect(16)
          : ByteBuffer.allocate(16);
      buffer.putInt(0, i).putInt(4, 0).putLong(8, ~0L);
      assertTrue(pool.offer(buffer));
    }
    final AtomicIntegerArray owners = new AtomicIntegerArray(capacity);
    final CountDownLatch start = new CountDownLatch(1);
    final var executor = Executors.newFixedThreadPool(20);
    final ArrayList<Future<?>> futures = new ArrayList<>(20);
    try {
      for (int worker = 1; worker <= 20; worker++) {
        final int owner = worker;
        futures.add(executor.submit(() -> {
          start.await();
          for (int iteration = 0; iteration < 5000; iteration++) {
            ByteBuffer buffer;
            while ((buffer = pool.poll()) == null) {
              if (Thread.currentThread().isInterrupted()) {
                return null;
              }
              Thread.onSpinWait();
            }
            final int id = buffer.getInt(0);
            // Observe contents before the test's ownership CAS: otherwise that unrelated atomic
            // marker could publish the prior writer's data and mask a broken pool handoff.
            final int generation = buffer.getInt(4);
            final long payload = buffer.getLong(8);
            assertTrue(owners.compareAndSet(id, 0, owner), "buffer has concurrent owners");
            assertEquals(~(long) generation, payload, "publication lost buffer contents");
            buffer.putInt(4, generation + 1).putLong(8, ~((long) generation + 1));
            assertTrue(owners.compareAndSet(id, owner, 0));
            // The pool is deliberately best-effort under contention. A failed bounded scan may
            // discard an extra buffer in production; retry here to retain the fixed test population.
            while (!pool.offer(buffer)) {
              if (Thread.currentThread().isInterrupted()) {
                return null;
              }
              Thread.onSpinWait();
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    final boolean[] returned = new boolean[capacity];
    int totalGenerations = 0;
    for (int i = 0; i < capacity; i++) {
      final ByteBuffer buffer = pool.poll();
      final int id = buffer.getInt(0);
      assertFalse(returned[id]);
      returned[id] = true;
      assertEquals(0, owners.get(id));
      totalGenerations += buffer.getInt(4);
      assertEquals(~(long) buffer.getInt(4), buffer.getLong(8));
    }
    assertEquals(100000, totalGenerations);
    assertNull(pool.poll());
  }
}
