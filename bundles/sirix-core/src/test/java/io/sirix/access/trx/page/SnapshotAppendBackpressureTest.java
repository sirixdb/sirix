package io.sirix.access.trx.page;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SnapshotAppendBackpressureTest {

  /**
   * How long {@link #appendProgressRestartsTheAdmissionDeadline()} lets its stall deadline run before
   * it reports progress. Comfortably inside the deadline, and large enough that a caller which
   * ignored the progress would return that much sooner than the assertion allows.
   */
  private static final long PROGRESS_DELAY_MILLIS = 150L;

  @Test
  void aSaturatedAppendExecutorBackpressuresWithoutCallerExecution() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch workerStarted = new CountDownLatch(1);
    final CountDownLatch releaseWorker = new CountDownLatch(1);
    try {
      submit(executor, task(() -> {
        workerStarted.countDown();
        await(releaseWorker);
      }));
      assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
      submit(executor, task(() -> {
      }));

      final CountDownLatch submissionReturned = new CountDownLatch(1);
      final CountDownLatch taskRan = new CountDownLatch(1);
      final AtomicReference<Thread> executionThread = new AtomicReference<>();
      final Thread submittingThread = new Thread(() -> {
        try {
          assertTrue(executor.acquireAdmissionUntilProgressStalls(TimeUnit.SECONDS.toNanos(5)));
          final TestTask admitted = task(() -> {
            executionThread.set(Thread.currentThread());
            assertTrue(NodeStorageEngineWriter.isSnapshotAppendWorkerThread());
            taskRan.countDown();
          });
          admitted.armAdmission();
          executor.execute(admitted);
          submissionReturned.countDown();
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }, "snapshot-append-submitter");
      submittingThread.start();

      assertFalse(submissionReturned.await(100, TimeUnit.MILLISECONDS));
      releaseWorker.countDown();
      assertTrue(submissionReturned.await(5, TimeUnit.SECONDS));
      assertTrue(taskRan.await(5, TimeUnit.SECONDS));
      assertNotSame(submittingThread, executionThread.get());
      submittingThread.join(5_000L);
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void stalledAdmissionStopsAtTheNoProgressDeadline() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      submit(executor, task(() -> await(release)));
      submit(executor, task(() -> await(release)));
      final long start = System.nanoTime();
      assertFalse(executor.acquireAdmissionUntilProgressStalls(TimeUnit.MILLISECONDS.toNanos(80)));
      final long elapsed = System.nanoTime() - start;
      assertTrue(elapsed >= TimeUnit.MILLISECONDS.toNanos(70));
      assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2));
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void appendProgressRestartsTheAdmissionDeadline() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch release = new CountDownLatch(1);
    final long stallTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500);
    try {
      submit(executor, task(() -> await(release)));
      submit(executor, task(() -> await(release)));
      final AtomicLong signalledAt = new AtomicLong();
      final Thread progress = new Thread(() -> {
        // Signal only once the waiter is parked on the admission semaphore. A bare sleep decided
        // this test on scheduling luck: a signal published before the wait began was never observed
        // (the deadline then ran its full, unextended length) and one published after the deadline
        // had already expired came too late. Both leave the caller returning at the unextended
        // deadline, and both were reachable on a loaded macOS runner.
        while (executor.admissionWaiters() == 0) {
          Thread.onSpinWait();
        }
        // Let a measurable part of the deadline burn down first, so a restart is distinguishable
        // from no restart at all. The assertion below is anchored on the signal rather than on the
        // call, so oversleeping only makes the restart MORE visible.
        try {
          Thread.sleep(PROGRESS_DELAY_MILLIS);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
        signalledAt.set(System.nanoTime());
        executor.signalProgress();
      }, "snapshot-append-progress");
      progress.start();

      assertFalse(executor.acquireAdmissionUntilProgressStalls(stallTimeoutNanos));
      final long returnedAt = System.nanoTime();
      progress.join(5_000L);

      // The deadline is measured from the LAST observed progress, not from the call: an admission
      // may only be given up after a further full stall timeout has passed without any.
      final long signalled = signalledAt.get();
      assertNotEquals(0L, signalled, "the progress signal must have been published while the caller waited");
      assertTrue(returnedAt - signalled >= stallTimeoutNanos,
          "admission was given up " + TimeUnit.NANOSECONDS.toMillis(returnedAt - signalled)
              + " ms after progress, before the restarted " + TimeUnit.NANOSECONDS.toMillis(stallTimeoutNanos)
              + " ms deadline");
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void admissionInterruptionIsPreserved() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      submit(executor, task(() -> await(release)));
      submit(executor, task(() -> await(release)));
      final AtomicBoolean interrupted = new AtomicBoolean();
      final Thread waiter = new Thread(() -> {
        try {
          executor.acquireAdmissionUntilProgressStalls(TimeUnit.SECONDS.toNanos(5));
        } catch (final InterruptedException expected) {
          interrupted.set(true);
          Thread.currentThread().interrupt();
        }
      });
      waiter.start();
      Thread.sleep(50L);
      waiter.interrupt();
      waiter.join(5_000L);
      assertTrue(interrupted.get());
      assertTrue(waiter.isInterrupted());
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void shutdownReleasesEveryRunningAndDrainedAdmission() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch running = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean drainedCancelled = new AtomicBoolean();
    submit(executor, task(() -> {
      running.countDown();
      await(release);
    }));
    assertTrue(running.await(5, TimeUnit.SECONDS));
    submit(executor, new TestTask(() -> {
    }) {
      @Override
      void cancelledByShutdown() {
        drainedCancelled.set(true);
      }
    });

    executor.shutdownNow();
    release.countDown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertTrue(drainedCancelled.get());
    assertTrue(executor.availableAdmissions() == 2);
    assertTrue(executor.progressMarker() > 0L);
  }

  @Test
  void shutdownReleasesAllDrainedAdmissionsWhenCancellationCallbackThrows() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 2);
    final CountDownLatch running = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean firstCancelled = new AtomicBoolean();
    final AtomicBoolean secondCancelled = new AtomicBoolean();

    submit(executor, task(() -> {
      running.countDown();
      await(release);
    }));
    assertTrue(running.await(5, TimeUnit.SECONDS));
    submit(executor, new TestTask(() -> {
    }) {
      @Override
      void cancelledByShutdown() {
        firstCancelled.set(true);
        throw new IllegalStateException("first cancellation callback failed");
      }
    });
    submit(executor, new TestTask(() -> {
    }) {
      @Override
      void cancelledByShutdown() {
        secondCancelled.set(true);
      }
    });

    final IllegalStateException failure = assertThrows(IllegalStateException.class, executor::shutdownNow);
    release.countDown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertEquals("first cancellation callback failed", failure.getMessage());
    assertTrue(firstCancelled.get());
    assertTrue(secondCancelled.get());
    assertEquals(3, executor.availableAdmissions());
  }

  @Test
  void priorAfterExecuteCannotReleaseARearmedPersistentTaskAdmission() throws Exception {
    final NodeStorageEngineWriter.SnapshotAppendExecutor executor =
        NodeStorageEngineWriter.createSnapshotAppendExecutor(1, 1);
    final CountDownLatch firstAdmissionReleased = new CountDownLatch(1);
    final CountDownLatch returnFromFirstRun = new CountDownLatch(1);
    final CountDownLatch secondRunCompleted = new CountDownLatch(1);
    final AtomicBoolean firstRun = new AtomicBoolean(true);

    final TestTask persistentTask = new TestTask(() -> {
      if (firstRun.compareAndSet(true, false)) {
        firstAdmissionReleased.countDown();
        await(returnFromFirstRun);
      } else {
        secondRunCompleted.countDown();
      }
    }) {
      @Override
      boolean executorReleasesAdmission() {
        return false;
      }
    };

    try {
      submit(executor, persistentTask);
      assertTrue(firstAdmissionReleased.await(5, TimeUnit.SECONDS));

      // Model executeSnapshotWrite's ordering: it releases this epoch's admission before it
      // publishes the signal that lets the foreground reuse the persistent task identity.
      executor.releaseTaskAdmission(persistentTask);
      assertTrue(executor.acquireAdmissionUntilProgressStalls(TimeUnit.SECONDS.toNanos(5)));
      persistentTask.armAdmission();

      // Let invocation N return and wait until its afterExecute callback is positively complete.
      // The callback must not consume the already-rearmed admission belonging to invocation N+1.
      returnFromFirstRun.countDown();
      final long completionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (executor.getCompletedTaskCount() < 1L && System.nanoTime() < completionDeadline) {
        Thread.onSpinWait();
      }
      assertTrue(executor.getCompletedTaskCount() >= 1L);
      assertTrue(persistentTask.ownsAdmission());
      assertEquals(1, executor.availableAdmissions());

      executor.execute(persistentTask);
      assertTrue(secondRunCompleted.await(5, TimeUnit.SECONDS));
      executor.releaseTaskAdmission(persistentTask);
      final long secondCompletionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (executor.getCompletedTaskCount() < 2L && System.nanoTime() < secondCompletionDeadline) {
        Thread.onSpinWait();
      }
      assertTrue(executor.getCompletedTaskCount() >= 2L);
      assertEquals(2, executor.availableAdmissions());
    } finally {
      returnFromFirstRun.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void submit(final NodeStorageEngineWriter.SnapshotAppendExecutor executor, final TestTask task)
      throws InterruptedException {
    assertTrue(executor.acquireAdmissionUntilProgressStalls(TimeUnit.SECONDS.toNanos(5)));
    task.armAdmission();
    executor.execute(task);
  }

  private static TestTask task(final Runnable action) {
    return new TestTask(action);
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static class TestTask extends NodeStorageEngineWriter.AdmittedSnapshotAppendTask {
    private final Runnable action;

    private TestTask(final Runnable action) {
      this.action = action;
    }

    @Override
    public void run() {
      action.run();
    }
  }
}
