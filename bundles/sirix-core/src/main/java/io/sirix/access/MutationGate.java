package io.sirix.access;

import io.sirix.exception.SirixIOException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Orders, within this JVM, the operations that create, replace, write or remove a database or one
 * of its resources, so an authorization check and the write it authorized cannot be separated.
 *
 * <p>
 * <b>The invariant:</b> a caller authorized to create, overwrite or delete a named target must
 * still be writing that same target when the write happens. Without this gate the sequence is
 * check, then write, with a window between: a concurrent request that is itself authorized - an
 * HTTP {@code DELETE /:database}, a replacing {@code PUT}, or another
 * {@code jn:store}/{@code jn:load} - can replace or remove the target in that window. The first
 * caller then writes into something nobody authorized it for, or destroys data the role it holds
 * does not cover. Holding the gate across both the check and the write closes the window.
 * </p>
 *
 * <p>
 * <b>Granularity is per resource, deliberately.</b> Sirix allows one writer per resource and
 * writers on different resources of a database concurrently, so gating a whole database would
 * serialise work the engine supports: two users creating two different resources in one database
 * must not wait on each other. Resource operations therefore take a <em>shared</em> hold on the
 * database plus an <em>exclusive</em> hold on the resource, while only an operation that replaces
 * or removes the whole database takes the database exclusively and so waits for every resource
 * operation in it.
 * </p>
 *
 * <p>
 * Two callers racing the same resource name serialise on that resource's hold, which is what makes
 * create-if-absent atomic: the loser re-reads existence inside the gate and is authorized again
 * against what it now finds, so it either holds MODIFY for the resource that now exists or is
 * refused. Neither a silent overwrite nor a skipped check is reachable.
 * </p>
 *
 * <p>
 * This is not {@link DatabaseLock}, a cross-process file lock expressing database
 * <em>ownership</em> that fails fast when another process holds it. This gate orders operations
 * inside one process and waits, because a request arriving mid-load should queue rather than be
 * rejected.
 * </p>
 *
 * <p>
 * Holds are reentrant: a bulk store takes one and then calls through store and collection code that
 * takes it again on the same thread. A thread holding a shared database hold must never ask for the
 * exclusive one - {@link ReentrantReadWriteLock} cannot upgrade - so an operation that might
 * replace a database takes the exclusive hold from the start.
 * </p>
 *
 * <p>
 * Not a hot path: every caller is a database or resource lifecycle operation, never a per-record or
 * per-page step. Registry entries exist only while an owner or waiter retains them. Lifecycle
 * monitors must be acquired after the gate, never while waiting to acquire it.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class MutationGate {

  private static final Map<Path, Entry<ReentrantReadWriteLock>> DATABASES = new ConcurrentHashMap<>();

  private static final Map<Path, Entry<ReentrantLock>> RESOURCES = new ConcurrentHashMap<>();

  private MutationGate() {
    throw new AssertionError("No instances");
  }

  /**
   * Hold a whole database exclusively, for an operation that replaces or removes it. Waits for every
   * resource operation in that database and blocks new ones until released.
   *
   * @param databaseFile the database directory being replaced or removed
   * @return a handle whose {@code close} releases the hold
   */
  public static Handle acquireDatabase(final Path databaseFile) {
    final Path path = key(databaseFile);
    final var entry = retain(DATABASES, path, ReentrantReadWriteLock::new);
    final var gate = entry.lock.writeLock();
    gate.lock();
    return new Handle(() -> {
      gate.unlock();
      release(DATABASES, path);
    });
  }

  /**
   * Hold one resource for an operation that creates or writes it, plus a shared hold on its database
   * so the database cannot be replaced or removed underneath. Concurrent operations on <em>other</em>
   * resources of the same database proceed in parallel.
   *
   * @param databaseFile the database directory containing the resource
   * @param resourceName the resource being created or written
   * @return a handle whose {@code close} releases both holds
   */
  public static Handle acquireResource(final Path databaseFile, final String resourceName) {
    Objects.requireNonNull(resourceName, "resourceName");
    final Path path = key(databaseFile);
    final Path resourcePath = path.resolve(ResourceConfiguration.canonicalResourceName(resourceName)).normalize();
    final var entry = retain(DATABASES, path, ReentrantReadWriteLock::new);
    final var database = entry.lock.readLock();
    database.lock();
    try {
      final var resource = retain(RESOURCES, resourcePath, ReentrantLock::new);
      resource.lock.lock();
      return new Handle(() -> {
        resource.lock.unlock();
        release(RESOURCES, resourcePath);
        database.unlock();
        release(DATABASES, path);
      });
    } catch (final RuntimeException | Error failure) {
      database.unlock();
      release(DATABASES, path);
      throw failure;
    }
  }

  private static <L> Entry<L> retain(final Map<Path, Entry<L>> registry, final Path path,
      final Supplier<L> factory) {
    return registry.compute(path, (key, current) -> {
      final Entry<L> entry = current == null ? new Entry<>(factory.get()) : current;
      entry.users++;
      return entry;
    });
  }

  private static <L> void release(final Map<Path, Entry<L>> registry, final Path path) {
    registry.computeIfPresent(path, (key, entry) -> --entry.users == 0 ? null : entry);
  }

  private static final class Entry<L> {
    private final L lock;
    private int users;

    private Entry(final L lock) {
      this.lock = lock;
    }
  }

  private static Path key(final Path databaseFile) {
    Path ancestor = Objects.requireNonNull(databaseFile, "databaseFile").toAbsolutePath();
    Path remaining = ancestor.getFileSystem().getPath("");
    try {
      while (true) {
        try {
          return ancestor.toRealPath().resolve(remaining).normalize();
        } catch (final NoSuchFileException missing) {
          if (Files.isSymbolicLink(ancestor)) {
            ancestor = ancestor.resolveSibling(Files.readSymbolicLink(ancestor));
          } else {
            final Path parent = ancestor.getParent();
            if (parent == null) {
              throw missing;
            }
            remaining = ancestor.getFileName().resolve(remaining);
            ancestor = parent;
          }
        }
      }
    } catch (final IOException failure) {
      throw new SirixIOException("Could not resolve database path " + databaseFile, failure);
    }
  }

  /** Releases exactly once, so a try-with-resources block cannot unlock a hold it no longer owns. */
  public static final class Handle implements AutoCloseable {
    private Runnable release;

    private Handle(final Runnable release) {
      this.release = release;
    }

    @Override
    public void close() {
      final Runnable pending = release;
      if (pending != null) {
        release = null;
        pending.run();
      }
    }
  }
}
