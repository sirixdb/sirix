/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter.ChunkBuild;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter.PendingBuild;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated
final class ParallelBulkChunkOwnershipTest {
  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L << 30);
  }

  @Test
  void neverStartedChunksReturnOnInterruptedAndTimedOutUnwind() {
    for (final boolean interrupted : new boolean[] {false, true}) {
      final ScratchStats before = Allocators.getInstance().scratchStats();
      final FusedSliceAndScan.ChunkBuffer bytes = bytes();
      final AtomicBoolean ran = new AtomicBoolean();
      final ChunkBuild build = new ChunkBuild(bytes, () -> {
        ran.set(true);
        return List.of();
      });
      final NeverTerminatingExecutor pool = new NeverTerminatingExecutor();
      final ArrayDeque<PendingBuild> pending = new ArrayDeque<>();
      pending.addLast(new PendingBuild(build, null, 0L, 0L, 0L));
      pool.execute(build);
      try {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        ParallelBulkJsonImporter.unwindInFlight(pending, pool);
        assertEquals(interrupted, Thread.currentThread().isInterrupted());
        assertTrue(pool.awaited);
        assertFalse(pool.isTerminated());
        assertTrue(pending.isEmpty());
        assertTrue(build.isCancelled());
        assertEquals(0, bytes.slabCount());
        assertEquals(before.totalLentBuffers(), Allocators.getInstance().scratchStats().totalLentBuffers());
        build.run();
        assertFalse(ran.get());
        assertEquals(before.staleReleases(), Allocators.getInstance().scratchStats().staleReleases());
      } finally {
        Thread.interrupted();
        bytes.release();
      }
    }
  }

  // NeverTerminatingExecutor never terminates: ExecutorService.close() would wait on it forever.
  @SuppressWarnings("java:S2093")
  @Test
  void interruptedAdoptionLeavesItsPendingChunkAvailableForUnwind(final @TempDir Path directory) throws Exception {
    final Path databasePath = directory.resolve("adoption-interrupt");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").build());
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final Constructor<ParallelBulkJsonImporter> constructor = ParallelBulkJsonImporter.class.getDeclaredConstructor(
            JsonNodeTrxImpl.class, int.class, ProjectionBulkLoad[].class);
        constructor.setAccessible(true);
        final ParallelBulkJsonImporter importer = constructor.newInstance(trx, 1024, new ProjectionBulkLoad[0]);
        final Method adopt = ParallelBulkJsonImporter.class.getDeclaredMethod("adoptNext", ArrayDeque.class);
        adopt.setAccessible(true);
        final ScratchStats before = Allocators.getInstance().scratchStats();
        final FusedSliceAndScan.ChunkBuffer bytes = bytes();
        final ChunkBuild build = new ChunkBuild(bytes, List::of);
        final ArrayDeque<PendingBuild> pending = new ArrayDeque<>();
        pending.addLast(new PendingBuild(build, null, 0L, 0L, 0L));
        try {
          Thread.currentThread().interrupt();
          final InvocationTargetException failure =
              assertThrows(InvocationTargetException.class, () -> adopt.invoke(importer, pending));
          assertInstanceOf(IllegalStateException.class, failure.getCause());
          assertTrue(Thread.currentThread().isInterrupted());
          assertEquals(1, pending.size());
          ParallelBulkJsonImporter.unwindInFlight(pending, new NeverTerminatingExecutor());
          assertEquals(0, bytes.slabCount());
          assertEquals(before.totalLentBuffers(), Allocators.getInstance().scratchStats().totalLentBuffers());
        } finally {
          Thread.interrupted();
          bytes.release();
        }
      }
    }
  }

  // NeverTerminatingExecutor never terminates: ExecutorService.close() would wait on it forever.
  @SuppressWarnings("java:S2093")
  @Test
  void aRunningBuildKeepsItsBufferUntilItLeaves() throws Exception {
    final ScratchStats before = Allocators.getInstance().scratchStats();
    final FusedSliceAndScan.ChunkBuffer bytes = bytes();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final KeyValueLeafPage page = page();
    final ChunkBuild build = new ChunkBuild(bytes, () -> {
      started.countDown();
      boolean permitted = false;
      while (!permitted) {
        try {
          finish.await();
          permitted = true;
        } catch (final InterruptedException _) {
          // The unwind cancels this build with an interrupt; it stays running until the test lets it go.
        }
      }
      try {
        assertEquals(7, bytes.byteAt(0));
      } catch (final Throwable error) {
        failure.set(error);
      }
      return List.of(page);
    });
    final Thread worker = new Thread(build, "chunk-owner-running");
    worker.start();
    try {
      started.await();
      final ArrayDeque<PendingBuild> pending = new ArrayDeque<>();
      pending.addLast(new PendingBuild(build, null, 0L, 0L, 0L));
      ParallelBulkJsonImporter.unwindInFlight(pending, new NeverTerminatingExecutor());
      assertTrue(build.isCancelled());
      assertEquals(1, bytes.slabCount());
      assertEquals(before.totalLentBuffers() + 1, Allocators.getInstance().scratchStats().totalLentBuffers());
    } finally {
      finish.countDown();
      worker.join();
      bytes.release();
    }
    assertNull(failure.get());
    assertTrue(page.isClosed());
    assertEquals(before.totalLentBuffers(), Allocators.getInstance().scratchStats().totalLentBuffers());
    assertEquals(before.staleReleases(), Allocators.getInstance().scratchStats().staleReleases());
  }

  @Test
  void completedBuildReturnsItsBufferAndPublishesItsResult() throws Exception {
    final ScratchStats before = Allocators.getInstance().scratchStats();
    final FusedSliceAndScan.ChunkBuffer bytes = bytes();
    final KeyValueLeafPage page = page();
    final ChunkBuild build = new ChunkBuild(bytes, () -> {
      assertEquals(7, bytes.byteAt(0));
      return List.of(page);
    });
    try {
      build.run();
      assertEquals(List.of(page), build.get());
      assertFalse(page.isClosed());
      assertEquals(0, bytes.slabCount());
      assertEquals(before.totalLentBuffers(), Allocators.getInstance().scratchStats().totalLentBuffers());
    } finally {
      bytes.release();
      page.retire();
    }
  }

  @Test
  void anOfferFinishingAfterTheFinalDrainReturnsItsChunkToTheFeeder() throws Exception {
    final ScratchStats before = Allocators.getInstance().scratchStats();
    final FusedSliceAndScan.ChunkBuffer bytes = bytes();
    final FusedSliceAndScan.Chunk chunk =
        new FusedSliceAndScan.Chunk(bytes, bytes.length(), true, 0L, 0L, 0L, null, null, null, null, null, null);
    final DelayedOfferQueue queue = new DelayedOfferQueue();
    final AtomicBoolean abandoned = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread feeder = new Thread(() -> {
      try {
        if (!ParallelBulkJsonImporter.handOff(queue, chunk, abandoned)) {
          bytes.release();
        }
      } catch (final Throwable error) {
        failure.set(error);
      }
    }, "chunk-owner-feeder");
    feeder.start();
    try {
      queue.entered.await();
      Thread.currentThread().interrupt();
      ParallelBulkJsonImporter.stopFeeder(feeder, queue, abandoned);
      assertTrue(Thread.currentThread().isInterrupted());
      Thread.interrupted();
      queue.complete.countDown();
      feeder.join();
      assertNull(failure.get());
      assertTrue(queue.isEmpty());
      assertEquals(0, bytes.slabCount());
      assertEquals(before.totalLentBuffers(), Allocators.getInstance().scratchStats().totalLentBuffers());
    } finally {
      Thread.interrupted();
      queue.complete.countDown();
      feeder.join();
      queue.clear();
      bytes.release();
    }
  }

  private static KeyValueLeafPage page() {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, ResourceConfiguration.newBuilder("chunk-owner-test").build(), 1,
        null, null, false);
  }

  private static FusedSliceAndScan.ChunkBuffer bytes() {
    final FusedSliceAndScan.ChunkBuffer bytes = new FusedSliceAndScan.ChunkBuffer();
    bytes.append(new byte[] {7}, 0, 1);
    return bytes;
  }

  private static final class NeverTerminatingExecutor extends AbstractExecutorService {
    private final List<Runnable> queued = new ArrayList<>();
    private boolean shutdown;
    private boolean awaited;

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      final List<Runnable> discarded = new ArrayList<>(queued);
      queued.clear();
      return discarded;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
      awaited = true;
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      return false;
    }

    @Override
    public void execute(final Runnable task) {
      queued.add(task);
    }
  }

  private static final class DelayedOfferQueue extends ArrayBlockingQueue<Object> {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch complete = new CountDownLatch(1);

    DelayedOfferQueue() {
      super(1);
    }

    @Override
    public boolean offer(final Object item, final long timeout, final TimeUnit unit) throws InterruptedException {
      entered.countDown();
      complete.await();
      return super.offer(item, timeout, unit);
    }
  }
}
