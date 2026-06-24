/*
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Ingest a large JSON dataset that has been split by the caller into <em>ordered partitions</em>,
 * shredding the partitions <strong>concurrently</strong> into one resource each within a single
 * database.
 *
 * <h2>Why partitions and not one resource</h2>
 * A SirixDB resource is one tree over a single monotonic node-key / DeweyID / page-key space written
 * under one uber page, so writing a single resource is intrinsically single-threaded (one
 * {@code Semaphore(1)} per resource). Throughput on a multi-core host therefore comes from sharding
 * the input across <em>N</em> independent resources, each shredded by its own writer thread. Different
 * resources share no write state — separate {@code sirix.data}/{@code sirix.revisions} files, separate
 * page-key sequences, separate per-resource write locks — so {@code N} writers run in parallel safely.
 * The global page caches are keyed by {@code (databaseId, resourceId)}, so the shards never collide
 * there either.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li><b>Order preserving.</b> Partition {@code i} is shredded into the resource named
 *       {@code baseName + "-" + i}; the returned list is in partition order, so a reader can
 *       reconstruct the global order by scanning the shards in index order.</li>
 *   <li><b>All-or-nothing.</b> If any partition fails (reader error, shred error, OOM, interruption),
 *       every resource this call created is removed and the first failure is rethrown (later failures
 *       attached as suppressed). On success, exactly {@code partitions.size()} resources exist.</li>
 *   <li><b>Fail-fast on collision.</b> If a target resource name already exists in the database the
 *       call throws before creating or writing anything — it never clobbers existing data.</li>
 *   <li><b>Bounded.</b> At most {@code maxConcurrency} shreds run at once. Each in-flight shred holds
 *       roughly one auto-commit window of pages off-heap (tens of MB), so size concurrency so that
 *       {@code maxConcurrency × per-shred-footprint} stays within the off-heap allocator budget; the
 *       default ({@code availableProcessors}) is safe for the standard multi-GB budget.</li>
 *   <li><b>No thread leak.</b> The dedicated executor is always shut down before returning, including
 *       on every error path.</li>
 * </ul>
 *
 * <p>The {@link Database} instance is shared across the worker threads — that is the required usage
 * (resource creation is internally synchronized and sessions are per-resource); callers must
 * <em>not</em> open one {@code Database} per thread for the same path.
 *
 * <p>This class is stateless and thread-safe; all state is method-local.
 *
 * @author Johannes Lichtenberger
 */
public final class ParallelJsonShredder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelJsonShredder.class);

  private ParallelJsonShredder() {
    throw new AssertionError("no instances");
  }

  /**
   * Shred the given ordered partitions concurrently into resources named {@code baseName-0 …
   * baseName-(n-1)} — a convenience over {@link #shred} for the common "split one dataset into N
   * evenly-named shards" case.
   *
   * @param database            the target database, shared across all workers (must be open)
   * @param partitions          ordered partitions; element {@code i} produces the {@link JsonReader}
   *                            for partition {@code i} (created lazily on its worker thread and closed
   *                            by this method). The factory or the reader it returns may throw.
   * @param baseName            resource name prefix; resources are {@code baseName-0 … baseName-(n-1)}
   * @param resourceConfigFactory given a resource name, returns the {@link ResourceConfiguration} to
   *                            create it with (its {@code newBuilder(name)} must use the supplied name)
   * @param autoCommitNodeCount auto-commit window in nodes for each shred ({@code <= 0} disables
   *                            auto-commit — only the final explicit commit is issued)
   * @param maxConcurrency      maximum shreds running concurrently; {@code <= 0} means
   *                            {@code availableProcessors}. Capped at {@code partitions.size()}.
   * @return the created resource names, in partition order ({@code result.get(i)} holds partition i)
   * @throws SirixException        if any partition fails (after rolling back all created resources)
   * @throws IllegalStateException if a target resource name already exists
   * @throws NullPointerException  if any required argument is {@code null}
   */
  public static List<String> shredPartitioned(final Database<JsonResourceSession> database,
      final List<? extends Callable<JsonReader>> partitions, final String baseName,
      final Function<String, ResourceConfiguration> resourceConfigFactory, final int autoCommitNodeCount,
      final int maxConcurrency) {
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(baseName, "baseName");
    final int n = partitions.size();
    final List<String> names = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      names.add(baseName + "-" + i);
    }
    return shred(database, names, partitions, resourceConfigFactory, autoCommitNodeCount, maxConcurrency);
  }

  /**
   * Shred the given ordered partitions concurrently into the explicitly-named resources, one writer
   * per resource: {@code resourceNames.get(i)} receives {@code partitions.get(i)}.
   *
   * <p>Use this when the caller owns the resource names (e.g. a collection's {@code resource1 …
   * resourceN} scheme); {@link #shredPartitioned} is the convenience form for {@code baseName-i}.
   *
   * @param database            the target database, shared across all workers (must be open)
   * @param resourceNames       the resource name for each partition (same size as {@code partitions})
   * @param partitions          ordered partitions; element {@code i} produces the {@link JsonReader}
   *                            for resource {@code resourceNames.get(i)} (created lazily on its worker
   *                            thread and closed by this method). The factory or reader may throw.
   * @param resourceConfigFactory given a resource name, returns the {@link ResourceConfiguration} to
   *                            create it with (its {@code newBuilder(name)} must use the supplied name)
   * @param autoCommitNodeCount auto-commit window in nodes for each shred ({@code <= 0} disables
   *                            auto-commit — only the final explicit commit is issued)
   * @param maxConcurrency      maximum shreds running concurrently; {@code <= 0} means
   *                            {@code availableProcessors}. Capped at {@code partitions.size()}.
   * @return the created resource names, in order ({@code result.get(i)} holds partition i)
   * @throws SirixException           if any partition fails (after rolling back all created resources)
   * @throws IllegalStateException    if a target resource name already exists
   * @throws IllegalArgumentException if {@code resourceNames} and {@code partitions} differ in size
   * @throws NullPointerException     if any required argument or element is {@code null}
   */
  public static List<String> shred(final Database<JsonResourceSession> database,
      final List<String> resourceNames, final List<? extends Callable<JsonReader>> partitions,
      final Function<String, ResourceConfiguration> resourceConfigFactory, final int autoCommitNodeCount,
      final int maxConcurrency) {
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(resourceNames, "resourceNames");
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(resourceConfigFactory, "resourceConfigFactory");
    if (resourceNames.size() != partitions.size()) {
      throw new IllegalArgumentException("resourceNames (" + resourceNames.size() + ") and partitions ("
          + partitions.size() + ") must have the same size");
    }

    final int n = partitions.size();
    if (n == 0) {
      return List.of();
    }
    // Defensively reject null entries up front — a null name/Callable would NPE deep inside a worker
    // and complicate the all-or-nothing rollback. Copy the names so a caller's list can't mutate
    // underneath the collision check / rollback set.
    final List<String> names = new ArrayList<>(resourceNames);
    for (int i = 0; i < n; i++) {
      Objects.requireNonNull(names.get(i), () -> "resource name is null");
      Objects.requireNonNull(partitions.get(i), () -> "partition entry is null");
    }

    // Fail-fast collision check BEFORE any mutation, so a name clash never leaves partial state and
    // never overwrites an existing resource.
    for (final String name : names) {
      if (database.existsResource(name)) {
        throw new IllegalStateException(
            "resource '" + name + "' already exists in database '" + database.getName() + "'");
      }
    }

    // Phase 1 — create the resources. createResource is internally synchronized on the database, so
    // running it serially here loses no parallelism and keeps the rollback set exact: 'created' holds
    // precisely the resources this call must remove if anything later fails.
    final List<String> created = new ArrayList<>(n);
    try {
      for (int i = 0; i < n; i++) {
        final String name = names.get(i);
        final ResourceConfiguration config = resourceConfigFactory.apply(name);
        if (config == null) {
          throw new SirixException("resourceConfigFactory returned null for resource '" + name + "'");
        }
        if (!name.equals(config.getName())) {
          throw new SirixException("resourceConfigFactory produced a config named '" + config.getName()
              + "' for requested resource '" + name + "'");
        }
        if (!database.createResource(config)) {
          // A concurrent external creator won the name between the collision check and here, or the
          // bootstrap failed; either way treat it as a hard failure and roll back.
          throw new SirixException("failed to create resource '" + name + "'");
        }
        created.add(name);
      }
    } catch (final RuntimeException | Error e) {
      rollback(database, created, e);
      throw e;
    }

    // Phase 2 — shred the partitions in parallel, one writer per resource.
    final int concurrency = Math.min(n, maxConcurrency <= 0 ? Runtime.getRuntime().availableProcessors() : maxConcurrency);
    final ExecutorService pool = Executors.newFixedThreadPool(concurrency, namedDaemonFactory());
    final List<Future<?>> futures = new ArrayList<>(n);
    try {
      for (int i = 0; i < n; i++) {
        final String name = names.get(i);
        final Callable<JsonReader> partition = partitions.get(i);
        futures.add(pool.submit(() -> {
          shredOne(database, name, partition, autoCommitNodeCount);
          return null;
        }));
      }

      // Await all, collecting the first failure. We deliberately wait for EVERY future (rather than
      // bailing on the first failure) so that no worker is still mutating its resource while we begin
      // rolling back — a half-written, concurrently-removed resource is exactly the corruption we must
      // avoid.
      Throwable firstFailure = null;
      boolean interrupted = false;
      for (final Future<?> f : futures) {
        try {
          f.get();
        } catch (final ExecutionException ee) {
          firstFailure = combine(firstFailure, ee.getCause() != null ? ee.getCause() : ee);
        } catch (final CancellationException ce) {
          // A future we cancelled after an interrupt below; record it but keep draining the rest.
          firstFailure = combine(firstFailure, ce);
        } catch (final InterruptedException ie) {
          interrupted = true;
          firstFailure = combine(firstFailure, ie);
          // Stop the remaining work promptly, then keep draining so we don't roll back live writers.
          futures.forEach(other -> other.cancel(true));
        }
      }

      if (firstFailure != null) {
        rollback(database, created, firstFailure);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        // Preserve the original failure type: RuntimeExceptions and Errors propagate as-is; checked
        // worker failures (reader IOException, InterruptedException) are wrapped in a SirixException.
        if (firstFailure instanceof RuntimeException re) {
          throw re;
        }
        if (firstFailure instanceof Error er) {
          throw er;
        }
        throw new SirixException("parallel shred failed for database '" + database.getName() + "'", firstFailure);
      }

      return List.copyOf(names);
    } finally {
      // Shut the pool down on every path. shutdownNow on an abnormal exit interrupts any stragglers
      // (e.g. a cancelled-but-still-running worker) so the JVM never leaks threads.
      pool.shutdownNow();
      try {
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
          LOGGER.warn("parallel-shred pool did not terminate within 60s for database '{}'", database.getName());
        }
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Shred a single partition into its resource: open session + write trx, insert subtree, commit. */
  private static void shredOne(final Database<JsonResourceSession> database, final String resourceName,
      final Callable<JsonReader> partition, final int autoCommitNodeCount) throws Exception {
    JsonReader reader = null;
    try {
      reader = partition.call();
      if (reader == null) {
        throw new SirixException("partition produced a null JsonReader for resource '" + resourceName + "'");
      }
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
          final JsonNodeTrx wtx =
              autoCommitNodeCount > 0 ? session.beginNodeTrx(autoCommitNodeCount) : session.beginNodeTrx()) {
        // Commit.NO: the explicit commit below is the single durable commit point for this shard.
        wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (final Exception closeEx) {
          // Never let a reader-close error mask the real outcome of the shred.
          LOGGER.debug("closing JsonReader for resource '{}' failed", resourceName, closeEx);
        }
      }
    }
  }

  /** Fold a new failure into the running first-failure: the first one wins, the rest attach as suppressed. */
  private static Throwable combine(final Throwable first, final Throwable next) {
    if (first == null) {
      return next;
    }
    first.addSuppressed(next);
    return first;
  }

  /** Best-effort removal of every resource this call created; cleanup errors attach as suppressed. */
  private static void rollback(final Database<JsonResourceSession> database, final List<String> created,
      final Throwable primary) {
    for (final String name : created) {
      try {
        database.removeResource(name);
      } catch (final RuntimeException removeEx) {
        primary.addSuppressed(
            new SirixException("rollback: failed to remove partially-created resource '" + name + "'", removeEx));
      }
    }
  }

  private static ThreadFactory namedDaemonFactory() {
    final AtomicInteger seq = new AtomicInteger();
    return runnable -> {
      final Thread t = new Thread(runnable, "sirix-parallel-shred-" + seq.getAndIncrement());
      t.setDaemon(true);
      return t;
    };
  }
}
