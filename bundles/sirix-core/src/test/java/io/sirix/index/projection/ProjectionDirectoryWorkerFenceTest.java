package io.sirix.index.projection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
final class ProjectionDirectoryWorkerFenceTest {
  @ParameterizedTest
  @ValueSource(strings = {"success", "interrupt", "startFailure", "workerFailure"})
  void neverReturnsWhileAStartedWorkerOwnsState(final String mode) throws InterruptedException {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch cancellationObserved = new CountDownLatch(1);
    final AtomicBoolean exited = new AtomicBoolean();
    final AtomicBoolean returned = new AtomicBoolean();
    final AtomicBoolean cancelled = new AtomicBoolean();
    final AtomicBoolean interruptedResult = new AtomicBoolean();
    final AtomicBoolean interruptRestored = new AtomicBoolean();
    final AtomicReference<Throwable> observed = new AtomicReference<>();
    final RuntimeException failure = new IllegalStateException(mode);
    final Thread worker = new Thread(() -> {
      entered.countDown();
      awaitUninterruptibly(release, cancellationObserved);
      cancelled.set(Thread.currentThread().isInterrupted());
      exited.set(true);
    });
    final Thread second = mode.equals("startFailure")
        ? new Thread() {
          @Override
          public synchronized void start() {
            throw failure;
          }
        }
        : new Thread(() -> {
          if (mode.equals("workerFailure")) {
            // Production workers capture failures into slots that are read only after the fence.
            observed.set(failure);
          }
        });
    final Thread owner = new Thread(() -> {
      try {
        interruptedResult.set(ProjectionIndexHOTStorage.startAndJoinDirectoryWorkers(new Thread[] {worker, second}));
      } catch (final Throwable e) {
        observed.set(e);
      } finally {
        interruptRestored.set(Thread.currentThread().isInterrupted());
        returned.set(true);
      }
    });
    owner.start();
    try {
      entered.await();
      awaitJoining(owner);
      if (mode.equals("interrupt")) {
        owner.interrupt();
      }
      if (mode.equals("interrupt") || mode.equals("startFailure")) {
        // A thread-state sample can still describe the old join after interrupt clears its flag.
        // The worker's acknowledgement is the deterministic proof that cancellation was delivered.
        cancellationObserved.await();
        awaitJoining(owner);
      }
      assertFalse(returned.get());
      assertFalse(exited.get());
    } finally {
      release.countDown();
      owner.join();
      worker.join();
      second.join();
    }
    assertEquals(mode.equals("interrupt") || mode.equals("startFailure"), cancelled.get());
    assertEquals(mode.equals("interrupt"), interruptedResult.get());
    assertTrue(exited.get());
    assertTrue(returned.get());
    assertEquals(mode.equals("interrupt"), interruptRestored.get());
    if (mode.endsWith("Failure")) {
      assertSame(failure, observed.get());
    }
  }

  private static void awaitJoining(final Thread owner) {
    while (owner.isAlive() && owner.getState() != Thread.State.WAITING) {
      Thread.yield();
    }
    assertTrue(owner.isAlive(), "owner returned before its blocked worker exited");
  }

  private static void awaitUninterruptibly(final CountDownLatch latch, final CountDownLatch cancellationObserved) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (final InterruptedException e) {
        interrupted = true;
        cancellationObserved.countDown();
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
