package io.sirix.access.trx.node;

import java.util.concurrent.CompletableFuture;
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
    execution.dispatch();
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
  void competingTryLockCannotBypassTheCallbackBorrow() throws Exception {
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
    execution.dispatch();
    assertFalse(acquired.get(10, TimeUnit.SECONDS));
    assertTrue(execution.tryLock(1, TimeUnit.SECONDS));
    execution.unlock();
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
    assertDoesNotThrow(execution::dispatch);
    assertSame(later, primary.getSuppressed()[0]);
    assertEquals(1, attempted.get());
    execution.lock();
    execution.unlock();
  }
}
