package io.sirix.access.trx.node;

import io.sirix.cache.ShardedPageCache;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction execution T and its callback ownership boundary. Outermost unlock delivers queued
 * callbacks with T released; other executions wait for that callback phase. Scheduled work defers
 * delivery until after its Runnable returns. Admission P is independent and remains owned.
 */
public final class TransactionExecutionLock extends ReentrantLock {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionExecutionLock.class);
  private final ConcurrentLinkedQueue<Runnable> callbacks = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CallbackBatch> deferredCallbacks = new ConcurrentLinkedQueue<>();
  private final Condition ready = newCondition();
  private @Nullable Thread callbackThread;
  private @Nullable Thread scheduledTaskThread;

  @Override
  public void lock() {
    super.lock();
    boolean success = false;
    try {
      while (!phaseAvailable()) {
        ready.awaitUninterruptibly();
      }
      success = true;
    } finally {
      if (!success) {
        super.unlock();
      }
    }
  }

  private boolean phaseAvailable() {
    final Thread current = Thread.currentThread();
    return (callbackThread == null || callbackThread == current)
        && (scheduledTaskThread == null || scheduledTaskThread == current);
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    super.lockInterruptibly();
    boolean success = false;
    try {
      while (!phaseAvailable()) {
        ready.await();
      }
      success = true;
    } finally {
      if (!success) {
        super.unlock();
      }
    }
  }

  @Override
  public boolean tryLock() {
    if (!super.tryLock()) {
      return false;
    }
    if (!phaseAvailable()) {
      super.unlock();
      return false;
    }
    return true;
  }

  @Override
  public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException {
    long remaining = unit.toNanos(time);
    final long start = System.nanoTime();
    if (!super.tryLock(remaining, TimeUnit.NANOSECONDS)) {
      return false;
    }
    boolean success = false;
    try {
      remaining -= System.nanoTime() - start;
      while (!phaseAvailable()) {
        if (remaining <= 0) {
          return false;
        }
        remaining = ready.awaitNanos(remaining);
      }
      success = true;
      return true;
    } finally {
      if (!success) {
        super.unlock();
      }
    }
  }

  @Override
  public void unlock() {
    try {
      if (getHoldCount() == 1 && callbackThread == null && scheduledTaskThread == null) {
        drainLocked();
      }
    } finally {
      super.unlock();
    }
  }

  public void enqueue(final Runnable callback) {
    callbacks.add(callback);
  }

  public static final class CallbackBatch {
    private final ConcurrentLinkedQueue<Runnable> callbacks = new ConcurrentLinkedQueue<>();

    public void enqueue(final Runnable callback) {
      callbacks.add(callback);
    }
  }

  public CallbackBatch deferCallbacks() {
    final CallbackBatch batch = new CallbackBatch();
    deferredCallbacks.add(batch);
    return batch;
  }

  public void dispatch(final CallbackBatch batch) {
    lock();
    try {
      if (deferredCallbacks.remove(batch)) {
        callbacks.addAll(batch.callbacks);
      }
    } finally {
      unlock();
    }
  }

  public void drainBeforeRetirement() {
    if (getHoldCount() != 1 || scheduledTaskThread != null) {
      throw new IllegalStateException("Payload retirement requires an outer lifecycle execution");
    }
    CallbackBatch batch;
    while ((batch = deferredCallbacks.poll()) != null) {
      callbacks.addAll(batch.callbacks);
    }
    drainLocked();
  }

  public void assertCanClose() {
    if (isHeldByCurrentThread() || isInScheduledTask()) {
      throw new IllegalStateException("Lifecycle close must run outside transaction execution and scheduled tasks");
    }
  }

  boolean isInScheduledTask() {
    return scheduledTaskThread == Thread.currentThread();
  }

  public ScheduledExecutorService newScheduler(final ThreadFactory factory) {
    return new ScheduledThreadPoolExecutor(1, factory) {
      @Override
      protected void beforeExecute(final Thread thread, final Runnable task) {
        super.beforeExecute(thread, task);
        lock();
        try {
          scheduledTaskThread = thread;
        } finally {
          TransactionExecutionLock.super.unlock();
        }
      }

      @Override
      protected void afterExecute(final Runnable task, final @Nullable Throwable failure) {
        TransactionExecutionLock.super.lock();
        try {
          scheduledTaskThread = null;
          try {
            drainLocked();
          } finally {
            ready.signalAll();
          }
        } finally {
          TransactionExecutionLock.super.unlock();
          super.afterExecute(task, failure);
        }
      }
    };
  }

  @SuppressWarnings("java:S1181")
  private void drainLocked() {
    if (callbacks.isEmpty()) {
      return;
    }
    callbackThread = Thread.currentThread();
    super.unlock();
    Throwable failure = null;
    boolean error = false;
    try {
      Runnable callback;
      while ((callback = callbacks.poll()) != null) {
        try {
          callback.run();
        } catch (final RuntimeException | Error next) {
          failure = ShardedPageCache.retainCleanupFailure(failure, next);
          error |= next instanceof Error;
        }
      }
    } finally {
      super.lock();
      callbackThread = null;
      ready.signalAll();
    }
    if (error) {
      ShardedPageCache.rethrowCleanupFailure(failure);
    }
    if (failure != null) {
      LOGGER.error("Commit notification failed after publication", failure);
    }
  }
}
