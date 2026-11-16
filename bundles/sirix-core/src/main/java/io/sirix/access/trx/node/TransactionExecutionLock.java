package io.sirix.access.trx.node;

import io.sirix.cache.ShardedPageCache;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.List;
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
 * callbacks with T released and independent read views. Mutations never wait for callback completion;
 * teardown alone fences outstanding dispatch. Scheduled work defers delivery until its Runnable
 * returns. Admission P remains owned. No scheduler termination wait may retain T.
 */
public final class TransactionExecutionLock extends ReentrantLock {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionExecutionLock.class);
  private final ConcurrentLinkedQueue<Runnable> callbacks = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CallbackBatch> deferredCallbacks = new ConcurrentLinkedQueue<>();
  private final Condition ready = newCondition();
  private boolean dispatching;
  private volatile @Nullable Thread scheduledTaskThread;

  @Override
  public void unlock() {
    try {
      if (getHoldCount() == 1 && !dispatching && scheduledTaskThread == null) {
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
      while (dispatching || scheduledTaskThread != null && !isInScheduledTask()) {
        ready.awaitUninterruptibly();
      }
      if (deferredCallbacks.remove(batch)) {
        callbacks.addAll(batch.callbacks);
      }
    } finally {
      unlock();
    }
  }

  public void drainBeforeRetirement(final Runnable fenceWorker) {
    if (getHoldCount() != 1 || isInScheduledTask()) {
      throw new IllegalStateException("Payload retirement requires an outer lifecycle execution");
    }
    do {
      while (dispatching || scheduledTaskThread != null) {
        ready.awaitUninterruptibly();
      }
      fenceWorker.run();
      CallbackBatch batch;
      while ((batch = deferredCallbacks.poll()) != null) {
        callbacks.addAll(batch.callbacks);
      }
      drainLocked();
    } while (dispatching || scheduledTaskThread != null || !deferredCallbacks.isEmpty() || !callbacks.isEmpty());
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
    Throwable failure = null;
    boolean error = false;
    while (!dispatching && scheduledTaskThread == null && !callbacks.isEmpty()) {
      final List<Runnable> pending = new ArrayList<>();
      Runnable callback;
      while ((callback = callbacks.poll()) != null) {
        pending.add(callback);
      }
      dispatching = true;
      super.unlock();
      try {
        for (final Runnable work : pending) {
          try {
            work.run();
          } catch (final RuntimeException | Error next) {
            failure = ShardedPageCache.retainCleanupFailure(failure, next);
            error |= next instanceof Error;
          }
        }
      } finally {
        super.lock();
        dispatching = false;
        ready.signalAll();
      }
    }
    if (error) {
      ShardedPageCache.rethrowCleanupFailure(failure);
    }
    if (failure != null) {
      LOGGER.error("Commit notification failed after publication", failure);
    }
  }
}
