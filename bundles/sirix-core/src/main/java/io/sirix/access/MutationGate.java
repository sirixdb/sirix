package io.sirix.access;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * per-page step. The maps hold one entry per database directory and per resource touched in this
 * JVM's lifetime, bounded by the schema rather than by the data written.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class MutationGate {

  private static final Map<Path, ReentrantReadWriteLock> DATABASES = new ConcurrentHashMap<>();

  private static final Map<Path, ReentrantLock> RESOURCES = new ConcurrentHashMap<>();

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
    final var gate = databaseGate(databaseFile).writeLock();
    gate.lock();
    return new Handle(gate::unlock);
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
    final var database = databaseGate(databaseFile).readLock();
    database.lock();
    final ReentrantLock resource =
        RESOURCES.computeIfAbsent(key(databaseFile).resolve(resourceName), path -> new ReentrantLock());
    try {
      resource.lock();
    } catch (final RuntimeException | Error failure) {
      database.unlock();
      throw failure;
    }
    return new Handle(() -> {
      resource.unlock();
      database.unlock();
    });
  }

  private static ReentrantReadWriteLock databaseGate(final Path databaseFile) {
    return DATABASES.computeIfAbsent(key(databaseFile), path -> new ReentrantReadWriteLock());
  }

  private static Path key(final Path databaseFile) {
    return Objects.requireNonNull(databaseFile, "databaseFile").toAbsolutePath().normalize();
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
