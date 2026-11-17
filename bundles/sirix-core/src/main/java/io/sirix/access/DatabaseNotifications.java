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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatabaseNotifications implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseNotifications.class);
  private static final ThreadLocal<@Nullable CallbackScope> CURRENT = new ThreadLocal<>();

  private record CallbackScope(DatabaseNotifications owner, @Nullable CallbackScope parent) {
  }

  private final Object progress = new Object();
  private int retirements;
  private final ThreadPoolExecutor executor;
  private volatile Duration closeTimeout = Duration.ofSeconds(5);
  private @Nullable Path databasePath;
  private final ArrayDeque<Runnable> deferred = new ArrayDeque<>();
  private boolean failedRetirement;
  private int pending;
  private int preHooks;
  private boolean accepting = true;
  private boolean stopped;

  public DatabaseNotifications() {
    setCloseTimeout(Duration.ofMillis(Long.getLong("sirix.notifications.closeTimeoutMillis", 5000L)));
    executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), work -> {
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

  public void submit(final Runnable notification) {
    Objects.requireNonNull(notification);
    synchronized (progress) {
      if (stopped || failedRetirement) {
        LOGGER.warn("Dropping a database notification after close or callback timeout");
      } else if (!accepting) {
        deferred.addLast(notification);
      } else {
        execute(notification);
      }
    }
  }

  private void execute(final Runnable notification) {
    pending++;
    try {
      executor.execute(() -> deliver(notification));
    } catch (final RuntimeException | Error failure) {
      pending--;
      progress.notifyAll();
      LOGGER.error("Database notification submission failed after durable publication", failure);
    }
  }

  private void deliver(final Runnable notification) {
    final @Nullable CallbackScope previous = CURRENT.get();
    try {
      CURRENT.set(new CallbackScope(this, previous));
      notification.run();
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

  public Retirement retire() {
    assertNotInCallback();
    if (Thread.currentThread().isInterrupted()) {
      throw new SirixThreadedException(new InterruptedException("Interrupted before notification retirement"));
    }
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
              Runnable notification;
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
