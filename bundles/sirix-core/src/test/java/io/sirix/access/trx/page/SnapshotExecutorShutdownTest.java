package io.sirix.access.trx.page;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** NATIVE-406: append coordinators must drain before their serialization workers. */
@Timeout(20)
final class SnapshotExecutorShutdownTest {
  @Test
  void drainsRunningAndQueuedAppendBeforeClosingSerialization() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor append =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final ForkJoinPool serialization = new ForkJoinPool(1);
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);
    final AtomicInteger result = new AtomicInteger();
    final FutureTask<Void> closing = new FutureTask<>(() -> {
      SnapshotExecutorShutdown.drain(append, serialization);
      return null;
    });
    final Thread closer = new Thread(closing, "snapshot-shutdown-test");
    try {
      submit(append, () -> {
        entered.countDown();
        try {
          finish.await();
          result.addAndGet(serialization.submit(() -> 40).join());
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interrupted);
        }
      });
      entered.await();
      submit(append, () -> result.addAndGet(serialization.submit(() -> 2).join()));
      closer.start();
      while (!append.isShutdown() && !closing.isDone()) {
        if (Thread.interrupted()) {
          throw new InterruptedException("Shutdown watchdog interrupted the test");
        }
        Thread.onSpinWait();
      }
      assertTrue(append.isShutdown());
      assertFalse(closing.isDone());
      assertFalse(serialization.isShutdown());
      finish.countDown();
      closing.get();
      assertEquals(42, result.get());
      assertTrue(append.isTerminated());
      assertTrue(serialization.isTerminated());
      assertEquals(2, append.availableAdmissions());
      SnapshotExecutorShutdown.drain(append, serialization);
    } finally {
      finish.countDown();
      append.close();
      serialization.close();
      closer.join();
    }
  }

  @Test
  void validatesBothPoolsBeforeStartingShutdown() {
    try (final ForkJoinPool pool = new ForkJoinPool(1)) {
      assertThrows(NullPointerException.class, () -> SnapshotExecutorShutdown.drain(pool, null));
      assertFalse(pool.isShutdown());
      assertThrows(NullPointerException.class, () -> SnapshotExecutorShutdown.drain(null, pool));
      assertFalse(pool.isShutdown());
    }
  }

  private static void submit(final NodeStorageEngineWriter.SnapshotAppendExecutor executor, final Runnable action)
      throws InterruptedException {
    assertTrue(executor.acquireAdmissionUntilProgressStalls(TimeUnit.SECONDS.toNanos(5)));
    final NodeStorageEngineWriter.AdmittedSnapshotAppendTask task =
        new NodeStorageEngineWriter.AdmittedSnapshotAppendTask() {
          @Override
          public void run() {
            action.run();
          }
        };
    task.armAdmission();
    executor.execute(task);
  }
}
