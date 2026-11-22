package io.sirix.access;

import io.sirix.exception.SirixThreadedException;
import java.time.Duration;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import io.sirix.exception.SirixIOException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.IntConsumer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatabaseNotifications implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseNotifications.class);
  // Both callback finally blocks restore the parent (null outside callbacks), releasing ownership.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<@Nullable CallbackScope> CURRENT = new ThreadLocal<>();

  private record CallbackScope(DatabaseNotifications owner, @Nullable CallbackScope parent) {
  }

  private record Notification(DatabaseNotifications owner, long resourceId, int revision,
      IntConsumer callback) implements Runnable {
    @Override
    public void run() {
      owner.deliver(this);
    }
  }

  private final Object progress = new Object();
  private int retirements;
  private final ThreadPoolExecutor executor;
  private final int capacity;
  private volatile Duration closeTimeout = Duration.ofSeconds(5);
  private @Nullable Path databasePath;
  private final ArrayDeque<Notification> deferred = new ArrayDeque<>();
  private boolean failedRetirement;
  private int pending;
  private int preHooks;
  private boolean accepting = true;
  private boolean stopped;

  public DatabaseNotifications() {
    this(Integer.parseInt(System.getProperty("sirix.notifications.capacity", "1024")));
  }

  public DatabaseNotifications(final int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("Notification capacity must be positive");
    }
    this.capacity = capacity;
    setCloseTimeout(Duration.ofMillis(Long.getLong("sirix.notifications.closeTimeoutMillis", 5000L)));
    executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(capacity), work -> {
      final Thread thread = new Thread(work, "sirix-database-notifications");
      thread.setDaemon(true);
      return thread;
    });
  }

  void bindDatabase(final Path path) {
    databasePath = Objects.requireNonNull(path);
  }

  public static void assertMayRetireDatabase(final Path path) {
    for (@Nullable
    CallbackScope scope = CURRENT.get(); scope != null; scope = scope.parent()) {
      final @Nullable Path database = scope.owner().databasePath;
      if (database != null) {
        try {
          if (Files.exists(path) && Files.isSameFile(database, path)) {
            scope.owner().assertNotInCallback();
          }
        } catch (final IOException failure) {
          throw new SirixIOException(failure);
        }
      }
    }
  }

  // Conversion validates representability before publishing the timeout to lifecycle callers.
  @SuppressWarnings({"ReturnValueIgnored", "java:S2201"})
  public void setCloseTimeout(final Duration timeout) {
    Objects.requireNonNull(timeout);
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("Notification close timeout must be positive");
    }
    timeout.toNanos();
    closeTimeout = timeout;
  }

  public Duration getCloseTimeout() {
    return closeTimeout;
  }

  public static void assertOutsideCallback() {
    if (CURRENT.get() != null) {
      throw new IllegalStateException("Lifecycle calls are forbidden from a commit callback of this database");
    }
  }

  public void assertNotInCallback() {
    for (@Nullable
    CallbackScope scope = CURRENT.get(); scope != null; scope = scope.parent()) {
      if (scope.owner() == this) {
        throw new IllegalStateException("Lifecycle calls are forbidden from a commit callback of this database");
      }
    }
  }

  public void submit(final long resourceId, final int revision, final IntConsumer callback) {
    Objects.requireNonNull(callback);
    if (resourceId < 0 || revision < 0) {
      throw new IllegalArgumentException("Notification resource and revision must be non-negative");
    }
    synchronized (progress) {
      if (stopped || failedRetirement) {
        LOGGER.warn("Dropping a database notification after close or callback timeout");
      } else if (pending + deferred.size() >= capacity) {
        LOGGER.warn("Dropping a database notification: retained-work capacity exhausted");
      } else {
        final Notification notification = new Notification(this, resourceId, revision, callback);
        if (!accepting) {
          deferred.addLast(notification);
        } else {
          execute(notification);
        }
      }
    }
  }

  // Both callers (submit and Retirement.close) hold progress throughout submission.
  @SuppressWarnings("java:S2273")
  private void execute(final Notification notification) {
    pending++;
    try {
      executor.execute(notification);
    } catch (final RuntimeException | Error failure) {
      pending--;
      progress.notifyAll();
      LOGGER.error("Database notification submission failed after durable publication", failure);
    }
  }

  private void deliver(final Notification notification) {
    final @Nullable CallbackScope previous = CURRENT.get();
    try {
      CURRENT.set(new CallbackScope(this, previous));
      notification.callback().accept(notification.revision());
    } catch (final RuntimeException | Error failure) {
      LOGGER.error("Database notification failed after durable publication", failure);
    } finally {
      CURRENT.set(previous);
      synchronized (progress) {
        pending--;
        progress.notifyAll();
      }
    }
  }

  public void runPreCommit(final Runnable hook) {
    Objects.requireNonNull(hook);
    assertNotInCallback();
    synchronized (progress) {
      if (!accepting) {
        if (!stopped) {
          throw new RetirementInProgressException();
        }
        throw new IllegalStateException("Database notification admission is stopped after close");
      }
      preHooks++;
    }
    final @Nullable CallbackScope previous = CURRENT.get();
    try {
      CURRENT.set(new CallbackScope(this, previous));
      hook.run();
    } finally {
      CURRENT.set(previous);
      synchronized (progress) {
        preHooks--;
        progress.notifyAll();
      }
    }
  }

  public static final class RetirementInProgressException extends IllegalStateException {
    private RetirementInProgressException() {
      super("Database notification admission is temporarily stopped for retirement");
    }
  }

  private void assertRetirementAllowed() {
    assertNotInCallback();
    if (Thread.currentThread().isInterrupted()) {
      throw new SirixThreadedException(new InterruptedException("Interrupted before notification retirement"));
    }
  }

  public Retirement retireHistory(final long resourceId, final int revision) {
    assertRetirementAllowed();
    if (resourceId < 0 || revision < 0) {
      throw new IllegalArgumentException("Notification resource and revision must be non-negative");
    }
    synchronized (progress) {
      assertDeferredHistoryRetained(resourceId, revision);
      final Retirement scope = retire();
      try {
        assertDeferredHistoryRetained(resourceId, revision);
        return scope;
      } catch (final RuntimeException | Error failure) {
        scope.close();
        throw failure;
      }
    }
  }

  private void assertDeferredHistoryRetained(final long resourceId, final int revision) {
    for (final Notification notification : deferred) {
      if (notification.resourceId() == resourceId && notification.revision() > revision) {
        throw new IllegalStateException(
            "Destructive maintenance would invalidate a deferred publication; retry after delivery");
      }
    }
  }

  public Retirement retire() {
    assertRetirementAllowed();
    synchronized (progress) {
      retirements++;
      accepting = false;
      try {
        awaitProgress(closeTimeout.toNanos(), System.nanoTime(), true);
        return new Retirement();
      } catch (final InterruptedException failure) {
        Thread.currentThread().interrupt();
        abandonAfterFailure();
        throw new SirixThreadedException(failure);
      } catch (final RuntimeException | Error failure) {
        abandonAfterFailure();
        throw failure;
      }
    }
  }

  public void awaitIdle(final Duration timeout) {
    Objects.requireNonNull(timeout);
    assertNotInCallback();
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("Notification await timeout must be positive");
    }
    synchronized (progress) {
      try {
        awaitProgress(timeout.toNanos(), System.nanoTime(), false);
      } catch (final InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new SirixThreadedException(failure);
      }
    }
  }

  private void awaitProgress(final long timeout, final long started, final boolean abandon)
      throws InterruptedException {
    while (pending != 0 || preHooks != 0 || !abandon && !deferred.isEmpty()) {
      final long remaining = timeout - (System.nanoTime() - started);
      if (remaining <= 0) {
        if (abandon) {
          final List<Runnable> dropped = new ArrayList<>();
          executor.getQueue().drainTo(dropped);
          pending -= dropped.size();
          final int count = dropped.size() + deferred.size();
          deferred.clear();
          LOGGER.warn("Database notification close deadline expired; abandoned {} queued notifications", count);
        }
        if (pending != 0 || preHooks != 0 || !abandon && !deferred.isEmpty()) {
          throw timeoutFailure();
        }
        return;
      }
      TimeUnit.NANOSECONDS.timedWait(progress, remaining);
    }
  }

  // Only retire's failure paths call this method, while holding progress.
  @SuppressWarnings("java:S2273")
  private void abandonAfterFailure() {
    retirements--;
    failedRetirement = true;
    final List<Runnable> dropped = new ArrayList<>();
    executor.getQueue().drainTo(dropped);
    pending -= dropped.size();
    final int count = dropped.size() + deferred.size();
    deferred.clear();
    LOGGER.warn(
        "Database notification retirement failed; abandoned {} queued notifications and retained active readers",
        count);
    progress.notifyAll();
  }

  public SirixThreadedException timeoutFailure() {
    return new SirixThreadedException(new ExecutionException(new TimeoutException(
        "Database callback retirement exceeded " + closeTimeout + "; live reader ownership is retained for retry")));
  }

  public final class Retirement implements AutoCloseable {
    private boolean closed;

    private Retirement() {}

    @Override
    public void close() {
      synchronized (progress) {
        if (!closed) {
          closed = true;
          if (--retirements == 0) {
            accepting = !stopped;
            if (accepting) {
              failedRetirement = false;
              Notification notification;
              while ((notification = deferred.pollFirst()) != null) {
                execute(notification);
              }
            }
          }
          progress.notifyAll();
        }
      }
    }
  }

  @Override
  public void close() {
    assertNotInCallback();
    try (Retirement ignored = retire()) {
      synchronized (progress) {
        stopped = true;
        accepting = false;
        if (!deferred.isEmpty()) {
          LOGGER.warn("Database close abandoned {} deferred notifications", deferred.size());
          deferred.clear();
        }
        executor.shutdown();
        progress.notifyAll();
      }
    }
  }
}
