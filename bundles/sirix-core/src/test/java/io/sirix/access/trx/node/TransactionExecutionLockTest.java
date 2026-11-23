package io.sirix.access.trx.node;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
final class TransactionExecutionLockTest {
  @Test
  void enclosingExecutionRejectsCloseUntilEveryAcquisitionReturns() throws Exception {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    execution.lock();
    execution.lock();
    assertThrows(IllegalStateException.class, execution::assertCanClose);
    execution.unlock();
    assertThrows(IllegalStateException.class, execution::assertCanClose);
    final CompletableFuture<Boolean> blocked = new CompletableFuture<>();
    final Thread contender = new Thread(() -> blocked.complete(execution.tryLock()));
    contender.start();
    assertFalse(blocked.get(10, TimeUnit.SECONDS));
    contender.join(10000);
    execution.unlock();
    execution.assertCanClose();
    assertTrue(execution.tryLock());
    execution.unlock();
  }

  @Test
  void scheduledMutationUsesOrdinaryExecutionOwnership() throws Exception {
    final TransactionExecutionLock execution = new TransactionExecutionLock();
    final ScheduledExecutorService scheduler = execution.newScheduler(Thread::new);
    final CompletableFuture<Void> completed = new CompletableFuture<>();
    try {
      scheduler.execute(() -> {
        execution.lock();
        try {
          assertThrows(IllegalStateException.class, execution::assertCanClose);
        } finally {
          execution.unlock();
        }
        execution.assertCanClose();
        completed.complete(null);
      });
      completed.get(10, TimeUnit.SECONDS);
    } finally {
      scheduler.shutdown();
      assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
