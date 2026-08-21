/*
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
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
 *       attached as suppressed). Rollback starts only after every worker has stopped. A worker that
 *       ignores interruption beyond the bounded shutdown timeout is the sole exception: its resources
 *       are retained rather than removed underneath a live writer, and that cleanup failure is attached
 *       to the primary exception. On success, exactly {@code partitions.size()} resources exist.</li>
 *   <li><b>Fail-fast on collision.</b> If a target resource name already exists in the database the
 *       call throws before creating or writing anything — it never clobbers existing data.</li>
 *   <li><b>Bounded.</b> At most {@code maxConcurrency} shreds run at once. Each in-flight shred holds
 *       roughly one auto-commit window of pages off-heap (tens of MB), so size concurrency so that
 *       {@code maxConcurrency × per-shred-footprint} stays within the off-heap allocator budget; the
 *       default ({@code availableProcessors}) is safe for the standard multi-GB budget.</li>
 *   <li><b>Bounded shutdown.</b> Executor shutdown is always requested and actual termination is
 *       awaited for up to 60 seconds. If a cancelled worker ignores interruption beyond that bound,
 *       the method throws with the daemon worker potentially still live and retains the resources it
 *       might still own.</li>
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
    return shredPartitioned(database, partitions, baseName, resourceConfigFactory, autoCommitNodeCount,
        maxConcurrency, AfterCommitState.KEEP_OPEN);
  }

  /**
   * As {@link #shredPartitioned(Database, List, String, Function, int, int)}, but with control over
   * what each shard's transaction does after an auto-commit.
   *
   * @param afterCommitState what a shard's transaction does after each auto-commit; both async modes
   *                         require {@code autoCommitNodeCount > 0}
   * @return the created resource names, in partition order
   */
  public static List<String> shredPartitioned(final Database<JsonResourceSession> database,
      final List<? extends Callable<JsonReader>> partitions, final String baseName,
      final Function<String, ResourceConfiguration> resourceConfigFactory, final int autoCommitNodeCount,
      final int maxConcurrency, final AfterCommitState afterCommitState) {
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(baseName, "baseName");
    final int n = partitions.size();
    final List<String> names = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      names.add(baseName + "-" + i);
    }
    return shred(database, names, partitions, resourceConfigFactory, autoCommitNodeCount, maxConcurrency,
        afterCommitState);
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
    return shred(database, resourceNames, partitions, resourceConfigFactory, autoCommitNodeCount,
        maxConcurrency, AfterCommitState.KEEP_OPEN);
  }

  /**
   * As {@link #shred(Database, List, List, Function, int, int)}, but with control over what each
   * shard's transaction does after an auto-commit.
   *
   * @param afterCommitState what a shard's transaction does after each auto-commit; both async modes
   *                         require {@code autoCommitNodeCount > 0}
   * @return the created resource names, in order
   */
  public static List<String> shred(final Database<JsonResourceSession> database,
      final List<String> resourceNames, final List<? extends Callable<JsonReader>> partitions,
      final Function<String, ResourceConfiguration> resourceConfigFactory, final int autoCommitNodeCount,
      final int maxConcurrency, final AfterCommitState afterCommitState) {
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(resourceNames, "resourceNames");
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(resourceConfigFactory, "resourceConfigFactory");
    Objects.requireNonNull(afterCommitState, "afterCommitState");
    if (resourceNames.size() != partitions.size()) {
      throw new IllegalArgumentException("resourceNames (" + resourceNames.size() + ") and partitions ("
          + partitions.size() + ") must have the same size");
    }

    if (partitions.isEmpty()) {
      return List.of();
    }
    final List<String> names = validatedNames(resourceNames, partitions);
    assertNoCollisions(database, names);
    final List<String> created = createResources(database, names, resourceConfigFactory);
    shredAllInParallel(database, names, partitions, autoCommitNodeCount, maxConcurrency, created,
        afterCommitState);
    return List.copyOf(names);
  }

  /** Copy the names and reject any null name / partition entry up front (a null would NPE in a worker). */
  private static List<String> validatedNames(final List<String> resourceNames,
      final List<? extends Callable<JsonReader>> partitions) {
    final int n = partitions.size();
    // Copy so a caller's list can't mutate underneath the collision check / rollback set.
    final List<String> names = new ArrayList<>(resourceNames);
    for (int i = 0; i < n; i++) {
      Objects.requireNonNull(names.get(i), () -> "resource name is null");
      Objects.requireNonNull(partitions.get(i), () -> "partition entry is null");
    }
    return names;
  }

  /** Fail-fast BEFORE any mutation: throw if a target name already exists, so we never overwrite. */
  private static void assertNoCollisions(final Database<JsonResourceSession> database, final List<String> names) {
    for (final String name : names) {
      if (database.existsResource(name)) {
        throw new IllegalStateException(
            "resource '" + name + "' already exists in database '" + database.getName() + "'");
      }
    }
  }

  /**
   * Phase 1 — create the resources serially (createResource is internally synchronized, so this loses
   * no parallelism). Returns precisely the resources this call created; on any failure they are rolled
   * back and the failure is rethrown.
   */
  private static List<String> createResources(final Database<JsonResourceSession> database,
      final List<String> names, final Function<String, ResourceConfiguration> resourceConfigFactory) {
    final List<String> created = new ArrayList<>(names.size());
    try {
      for (final String name : names) {
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
    return created;
  }

  /**
   * Phase 2 — shred the partitions in parallel (one writer per resource) on a bounded dedicated pool,
   * awaiting every worker. On any failure the created resources are rolled back and the first failure
   * is rethrown. Pool shutdown is always requested; termination is awaited up to the documented bound.
   */
  private static void shredAllInParallel(final Database<JsonResourceSession> database, final List<String> names,
      final List<? extends Callable<JsonReader>> partitions, final int autoCommitNodeCount,
      final int maxConcurrency, final List<String> created, final AfterCommitState afterCommitState) {
    final int n = names.size();
    final int concurrency =
        Math.min(n, maxConcurrency <= 0 ? Runtime.getRuntime().availableProcessors() : maxConcurrency);
    final ExecutorService pool;
    try {
      pool = Executors.newFixedThreadPool(concurrency, namedDaemonFactory());
    } catch (final RuntimeException | Error poolFailure) {
      rollback(database, created, poolFailure);
      throw poolFailure;
    }

    final List<Future<?>> futures = new ArrayList<>(n);
    Throwable firstFailure = null;
    boolean interrupted = false;
    try {
      for (int i = 0; i < n; i++) {
        final String name = names.get(i);
        final Callable<JsonReader> partition = partitions.get(i);
        futures.add(pool.submit(() -> {
          shredOne(database, name, partition, autoCommitNodeCount, afterCommitState);
          return null;
        }));
      }

      final Outcome outcome = awaitAll(futures);
      firstFailure = outcome.firstFailure();
      interrupted = outcome.interrupted();
    } catch (final RuntimeException | Error submissionOrAwaitFailure) {
      firstFailure = combine(firstFailure, submissionOrAwaitFailure);
      futures.forEach(future -> future.cancel(true));
    }

    // A cancelled Future is complete from Future.get's point of view before its task has necessarily
    // returned. Executor termination, not Future cancellation, is therefore the ownership fence: only
    // after it holds can rollback remove a resource without racing a worker that still owns its session.
    final ShutdownOutcome shutdown = shutDownPool(pool, database);
    interrupted |= shutdown.interrupted();
    try {
      if (firstFailure != null) {
        if (shutdown.terminated()) {
          rollback(database, created, firstFailure);
        } else {
          firstFailure.addSuppressed(new SirixException(
              "rollback skipped for database '" + database.getName()
                  + "' because the parallel-shred pool still has live workers after the shutdown timeout"));
        }
        throwShredFailure(database, firstFailure);
      }

      if (!shutdown.terminated()) {
        final SirixException shutdownFailure = new SirixException(
            "parallel-shred pool did not terminate after all workers completed for database '"
                + database.getName() + "'");
        // Every Future completed normally, so no worker owns a resource even if an executor thread has
        // failed to terminate. Rollback is safe and preserves the all-or-nothing success contract.
        rollback(database, created, shutdownFailure);
        throw shutdownFailure;
      }
    } finally {
      // Keep the flag consumed through database cleanup: lock acquisition/removal is allowed to react
      // to interruption. Restore exactly at this method's throw/return boundary, even if cleanup fails.
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** The first worker failure (the rest attached as suppressed) plus whether this thread was interrupted. */
  private record Outcome(Throwable firstFailure, boolean interrupted) {
  }

  /**
   * Await every Future outcome rather than bailing on the first failure. A cancelled Future does not
   * prove its task has returned; {@link #shutDownPool} supplies that separate ownership fence before
   * rollback. The first failure wins; the rest attach as suppressed.
   */
  private static Outcome awaitAll(final List<Future<?>> futures) {
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
        // Stop the remaining work promptly, then collect every Future outcome. Actual task termination
        // is awaited separately before rollback because cancel(true) only requests interruption.
        futures.forEach(other -> other.cancel(true));
      }
    }
    return new Outcome(firstFailure, interrupted);
  }

  /** Rethrow the first failure preserving its type: RuntimeException / Error as-is, else wrapped. */
  private static void throwShredFailure(final Database<JsonResourceSession> database, final Throwable firstFailure) {
    if (firstFailure instanceof RuntimeException re) {
      throw re;
    }
    if (firstFailure instanceof Error er) {
      throw er;
    }
    throw new SirixException("parallel shred failed for database '" + database.getName() + "'", firstFailure);
  }

  /** Whether executor termination was reached, plus any interruption consumed while waiting for it. */
  private record ShutdownOutcome(boolean terminated, boolean interrupted) {
  }

  /**
   * Shut the pool down and wait up to the existing bounded timeout for actual task termination.
   *
   * <p>Interruptions are remembered but deliberately not restored inside this method: restoring the
   * flag before another {@link ExecutorService#awaitTermination} would make it throw immediately and
   * destroy the ownership fence rollback relies on. The caller restores the flag after this method
   * establishes termination (or records that a worker outlived the timeout) and completes any safe
   * database cleanup. It restores the flag immediately before the final return or throw.</p>
   */
  private static ShutdownOutcome shutDownPool(final ExecutorService pool,
      final Database<JsonResourceSession> database) {
    pool.shutdownNow();
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    boolean interrupted = false;
    while (!pool.isTerminated()) {
      final long remaining = deadline - System.nanoTime();
      if (remaining <= 0L) {
        LOGGER.warn("parallel-shred pool did not terminate within 60s for database '{}'", database.getName());
        return new ShutdownOutcome(false, interrupted);
      }
      try {
        if (!pool.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
          LOGGER.warn("parallel-shred pool did not terminate within 60s for database '{}'", database.getName());
          return new ShutdownOutcome(false, interrupted);
        }
      } catch (final InterruptedException ignored) {
        interrupted = true;
        // A second interrupt may arrive while cancellation is already in flight. Reassert the shutdown
        // request, consume the signal for now, and keep waiting within the original wall-clock bound.
        pool.shutdownNow();
      }
    }
    return new ShutdownOutcome(true, interrupted);
  }

  /** Shred a single partition into its resource: open session + write trx, insert subtree, commit. */
  private static void shredOne(final Database<JsonResourceSession> database, final String resourceName,
      final Callable<JsonReader> partition, final int autoCommitNodeCount,
      final AfterCommitState afterCommitState) throws Exception {
    JsonReader reader = null;
    try {
      reader = partition.call();
      if (reader == null) {
        throw new SirixException("partition produced a null JsonReader for resource '" + resourceName + "'");
      }
      // On what to pass for afterCommitState, measured on a 2.1 GB input across four shards on four
      // cores, auto-committing every 100k nodes, best of three warm rounds, repeated three times:
      //
      //   KEEP_OPEN               58.5  68.9  70.2 MB/s
      //   KEEP_OPEN_ASYNC_FLUSH   60.1  62.1  63.9 MB/s
      //   KEEP_OPEN_ASYNC_COMMIT  73.0  74.9  77.9 MB/s
      //
      // Async commit's worst run beats every other mode's best, and it keeps KEEP_OPEN's revision
      // semantics — a revision per threshold — while moving the durability barriers off the writer.
      // It is not the default only because a hardening failure poisons the transaction terminally,
      // which is a choice for the caller rather than for this method.
      //
      // Async flush wins at small inputs (1.22x over KEEP_OPEN on 176 MB) and loses at this scale;
      // it also creates no intermediate revisions, so it is not semantically interchangeable.
      //
      // An earlier comment here claimed async modes regress in the parallel path. That was measured
      // for async commit with auto-commit disabled on a 25 MB input, where the whole shard sits in
      // one transaction; it does not hold once auto-commit bounds the epoch.
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
          final JsonNodeTrx wtx = autoCommitNodeCount > 0
              ? session.beginNodeTrx(autoCommitNodeCount, afterCommitState)
              : session.beginNodeTrx(afterCommitState)) {
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
