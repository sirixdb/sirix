package io.sirix.access.trx.node;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantLock;

public final class TransactionExecutionLock extends ReentrantLock {
  private static final long serialVersionUID = 1L;

  public void assertCanClose() {
    if (isHeldByCurrentThread()) {
      throw new IllegalStateException("Lifecycle close must run outside transaction execution");
    }
  }

  public ScheduledExecutorService newScheduler(final ThreadFactory factory) {
    return Executors.newSingleThreadScheduledExecutor(factory);
  }
}
