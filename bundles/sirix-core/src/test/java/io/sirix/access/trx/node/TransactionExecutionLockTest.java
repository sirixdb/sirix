package io.sirix.access.trx.node;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import io.sirix.access.trx.node.TransactionExecutionLock.CallbackBatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(15)
final class TransactionExecutionLockTest {
  @Test
  void dispatchWaitsForTheOutermostUnlock() {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final AtomicInteger calls = new AtomicInteger();
    execution.lock();
    execution.lock();
    execution.enqueue(() -> {
      assertFalse(execution.isHeldByCurrentThread());
      assertEquals(0, calls.getAndIncrement());
    });
    execution.lock();
    execution.unlock();
    execution.unlock();
    assertEquals(0, calls.get());
    execution.unlock();
    assertEquals(1, calls.get());
    execution.lock();
    execution.unlock();
    assertEquals(1, calls.get());
  }

  @Test
  void scheduledDeliveryFollowsTheRunnableBoundary() throws Exception {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final ScheduledExecutorService scheduler = execution.newScheduler(Thread::new);
    final CompletableFuture<Void> notified = new CompletableFuture<>();
    final AtomicInteger stage = new AtomicInteger();
    try {
      scheduler.execute(() -> {
        execution.lock();
        try {
          execution.enqueue(() -> {
            try {
              assertFalse(execution.isHeldByCurrentThread());
              assertFalse(execution.isInScheduledTask());
              assertEquals(1, stage.get());
              notified.complete(null);
            } catch (final Throwable failure) {
              notified.completeExceptionally(failure);
              throw failure;
            }
          });
        } finally {
          execution.unlock();
        }
        assertFalse(notified.isDone());
        stage.incrementAndGet();
      });
      notified.get(10, TimeUnit.SECONDS);
    } finally {
      scheduler.shutdown();
      assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void callbackCompletionDoesNotReserveTheMutationLock() throws Exception {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final CompletableFuture<Boolean> acquired = new CompletableFuture<>();
    execution.enqueue(() -> {
      final Thread contender = new Thread(() -> {
        final boolean success = execution.tryLock();
        if (success) {
          execution.unlock();
        }
        acquired.complete(success);
      });
      contender.start();
      try {
        acquired.get(10, TimeUnit.SECONDS);
      } catch (final Exception failure) {
        throw new AssertionError(failure);
      }
    });
    execution.lock();
    execution.unlock();
    assertTrue(acquired.get(10, TimeUnit.SECONDS));
    assertTrue(execution.tryLock(1, TimeUnit.SECONDS));
    execution.unlock();
  }

  @Test
  void callbackErrorIsRethrownAfterIndependentDeliveryAndPhaseRestoration() {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final AssertionError primary = new AssertionError("first callback");
    final AssertionError later = new AssertionError("later callback");
    final AtomicInteger attempted = new AtomicInteger();
    execution.enqueue(() -> { throw primary; });
    execution.enqueue(() -> { throw later; });
    execution.enqueue(attempted::incrementAndGet);
    assertSame(primary, assertThrows(AssertionError.class, () -> { execution.lock(); execution.unlock(); }));
    assertSame(later, primary.getSuppressed()[0]);
    assertEquals(1, attempted.get());
    assertTrue(execution.tryLock());
    execution.unlock();
  }

  @Test
  void deferredEpochsBelongToTheirCompletionDispatcherOrFinalCloser() {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final AtomicInteger delivered = new AtomicInteger();
    final CallbackBatch first = execution.deferCallbacks();
    final CallbackBatch second = execution.deferCallbacks();
    first.enqueue(delivered::incrementAndGet);
    second.enqueue(() -> delivered.addAndGet(10));
    execution.lock();
    execution.unlock();
    assertEquals(0, delivered.get());
    execution.dispatch(first);
    assertEquals(1, delivered.get());
    execution.dispatch(first);
    assertEquals(1, delivered.get());
    execution.lock();
    try {
      execution.drainBeforeRetirement(() -> {});
    } finally {
      execution.unlock();
    }
    assertEquals(11, delivered.get());
    execution.dispatch(second);
    assertEquals(11, delivered.get());
  }

  @Test
  void retirementRechecksWorkersAdmittedWhileCallbackDispatchReleasedExecution() throws Exception {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final Semaphore worker = new Semaphore(1);
    final CountDownLatch firstEntered = new CountDownLatch(1);
    final CompletableFuture<Void> releaseFirst = new CompletableFuture<>();
    final CompletableFuture<Void> secondDelivered = new CompletableFuture<>();
    final CompletableFuture<Void> retired = new CompletableFuture<>();
    final AtomicInteger fences = new AtomicInteger();
    execution.enqueue(() -> {
      firstEntered.countDown();
      releaseFirst.join();
    });
    final Thread closer = new Thread(() -> {
      execution.lock();
      try {
        execution.drainBeforeRetirement(() -> {
          fences.incrementAndGet();
          worker.acquireUninterruptibly();
          worker.release();
        });
        retired.complete(null);
      } catch (final Throwable failure) {
        retired.completeExceptionally(failure);
      } finally {
        execution.unlock();
      }
    });
    try {
      closer.start();
      assertTrue(firstEntered.await(10, TimeUnit.SECONDS));
      execution.lock();
      try {
        worker.acquireUninterruptibly();
        execution.deferCallbacks().enqueue(() -> secondDelivered.complete(null));
      } finally {
        execution.unlock();
      }
      releaseFirst.complete(null);
      while (!worker.hasQueuedThreads()) {
        assertFalse(retired.isDone());
        Thread.onSpinWait();
      }
      assertFalse(secondDelivered.isDone());
      worker.release();
      retired.get(10, TimeUnit.SECONDS);
      secondDelivered.get(10, TimeUnit.SECONDS);
      assertTrue(fences.get() >= 2);
    } finally {
      releaseFirst.complete(null);
      if (worker.availablePermits() == 0) {
        worker.release();
      }
      closer.join(10000);
    }
  }

  @Test
  void callbackFailureDoesNotStrandTheExecutionPhase() {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final IllegalStateException primary = new IllegalStateException("first callback");
    final IllegalArgumentException later = new IllegalArgumentException("second callback");
    final AtomicInteger attempted = new AtomicInteger();
    execution.enqueue(() -> { throw primary; });
    execution.enqueue(() -> { throw later; });
    execution.enqueue(attempted::incrementAndGet);
    assertDoesNotThrow(() -> { execution.lock(); execution.unlock(); });
    assertSame(later, primary.getSuppressed()[0]);
    assertEquals(1, attempted.get());
    execution.lock();
    execution.unlock();
  }
}
