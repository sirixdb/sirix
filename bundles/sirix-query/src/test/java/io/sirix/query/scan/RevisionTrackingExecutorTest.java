package io.sirix.query.scan;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.QueryContext;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for the indirection that lets a compiled query follow the revision it should be
 * reading — see {@link RevisionTrackingExecutor}.
 */
final class RevisionTrackingExecutorTest {

  private static final String DB = "revision-tracking-db";
  private static final String RES = "records.jn";

  /**
   * The delegation trap: a method added to {@link VectorizedExecutor} later would be inherited here
   * as the interface default ("unsupported") instead of being forwarded. At the entry points brackit
   * substitutes at translate time that is not a lost fast path but a failed query, and nothing else
   * in the suite would notice — every existing test would still pass. So the full set is checked
   * structurally, and a new interface method breaks the build instead of the user.
   */
  @Test
  void everyExecutorMethodIsForwarded() {
    final Set<String> forwarded = new HashSet<>();
    for (final Method m : RevisionTrackingExecutor.class.getDeclaredMethods()) {
      if (!m.isSynthetic()) {
        forwarded.add(signature(m));
      }
    }
    final List<String> missing = new ArrayList<>();
    for (final Method m : VectorizedExecutor.class.getDeclaredMethods()) {
      if (m.isSynthetic() || Modifier.isStatic(m.getModifiers())) {
        continue;
      }
      if (!forwarded.contains(signature(m))) {
        missing.add(signature(m));
      }
    }
    assertTrue(missing.isEmpty(),
        "RevisionTrackingExecutor must forward every VectorizedExecutor method; missing: " + missing);
  }

  private static String signature(final Method m) {
    return m.getName() + Arrays.toString(m.getParameterTypes());
  }

  /**
   * Before anything has ever resolved, every entry point declines rather than throwing — the
   * translator then builds the generic pipeline, which is the only correct thing to do when there is
   * nothing to serve from.
   */
  @Test
  void anUnresolvableExecutorDeclinesEverything() {
    final var lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
    final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(() -> null, lifecycle);
    assertNull(executor.lastResolved());
    assertFalse(executor.canExecute(null));
    assertFalse(executor.acceptsSource(SourceRef.document("db", "res", SourceRef.LATEST_REVISION)));
    assertFalse(executor.supportsSortedScan());
    assertFalse(executor.supportsMultiKeyGroupBy());
    assertNull(executor.executeAggregate(null, new String[] {"[]"}, "sum", "age"));
    assertNull(executor.executeGroupByCount(null, new String[] {"[]"}, "age"));
    assertNull(executor.executePredicateCount(null, new String[] {"[]"}, null));
  }

  /**
   * A resolver that transiently stops answering must leave the last resolved executor in place while
   * its lifecycle remains open. Terminal chain close is covered by the lifecycle tests and rejects
   * through the shared admission fence.
   */
  @Test
  void theLastResolvedExecutorSurvivesAResolverThatStopsAnswering() throws Exception {
    final Path dbDir = Files.createTempDirectory("sirix-revision-tracking-");
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbDir.resolve(DB)));
      try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
        db.createResource(ResourceConfiguration.newBuilder(RES).build());
        try (final var session = db.beginResourceSession(RES)) {
          try (final var wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"age\":1},{\"age\":2}]"));
            wtx.commit();
          }
          final var lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
          final SirixVectorizedExecutor resolved =
              new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 1, lifecycle);
          try {
            final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>();
            final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get, lifecycle);
            assertNull(executor.lastResolved());

            answer.set(resolved);
            assertTrue(executor.canExecute(null));
            assertSame(resolved, executor.lastResolved(), "the resolved executor must be remembered");

            answer.set(null);
            assertTrue(executor.canExecute(null),
                "a resolver that stops answering must keep serving from the last one");
            assertSame(resolved, executor.lastResolved());
          } finally {
            resolved.close();
            lifecycle.closeAndAwait();
          }
        }
      }
    } finally {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  /**
   * The gate pins what it admitted so the scan that follows serves one revision — and the pin dies
   * with that scan. A pin that outlived its evaluation would sit on the thread until the next gate
   * call: the next execution of the same compiled query would then be served by the executor of the
   * PREVIOUS one, which after a commit is exactly the stale-revision answer this class exists to
   * remove.
   */
  @Test
  void theGatePinDoesNotOutliveTheScanItAdmitted() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final SirixVectorizedExecutor gated = executorFactory.get();
      final SirixVectorizedExecutor afterCommit = executorFactory.get();
      try {
        final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>(gated);
        final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get, lifecycle);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);
        final QueryContext evaluation = new BrackitQueryContext();

        assertTrue(executor.acceptsSource(source, evaluation), "the gate must admit its own document");
        assertSame(gated, executor.lastResolved());

        // Within the admitted evaluation the pin holds, even though the resolver has moved on.
        answer.set(afterCommit);
        assertTrue(executor.canExecute(evaluation));
        assertSame(gated, executor.lastResolved(), "the gated scan must serve from what the gate admitted");

        // The scan consumes the pin; everything after it re-resolves.
        executor.executeAggregate(evaluation, new String[] {"[]"}, "count", "age");
        assertTrue(executor.canExecute(evaluation));
        assertSame(afterCommit, executor.lastResolved(), "the pin must not outlive the scan that consumed it");
      } finally {
        gated.close();
        afterCommit.close();
      }
    });
  }

  @Test
  void aCapabilityLeaseConsumesTheGatePinItAdmitted() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final SirixVectorizedExecutor gated = executorFactory.get();
      final SirixVectorizedExecutor afterLease = executorFactory.get();
      try {
        final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>(gated);
        final RevisionTrackingExecutor executor =
            new RevisionTrackingExecutor(answer::get, ignored -> answer::get, lifecycle);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);
        final QueryContext evaluation = new BrackitQueryContext();

        assertTrue(executor.acceptsSource(source, evaluation));
        answer.set(afterLease);
        try (final SirixExecutorProvider.Lease lease = executor.acquire(evaluation, source)) {
          assertNotNull(lease);
          assertSame(gated, lease.executor());
        }

        assertTrue(executor.canExecute(evaluation));
        assertSame(afterLease, executor.lastResolved());
      } finally {
        gated.close();
        afterLease.close();
      }
    });
  }

  /**
   * Belt and braces for the same property: a pin is bound to the evaluation that took it, so even one
   * left behind cannot answer for a LATER execution — which carries its own {@link QueryContext}.
   */
  @Test
  void aPinCannotServeADifferentEvaluation() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final SirixVectorizedExecutor gated = executorFactory.get();
      final SirixVectorizedExecutor afterCommit = executorFactory.get();
      try {
        final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>(gated);
        final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get, lifecycle);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);

        assertTrue(executor.acceptsSource(source, new BrackitQueryContext()));
        assertSame(gated, executor.lastResolved());

        answer.set(afterCommit);
        assertTrue(executor.canExecute(new BrackitQueryContext()));
        assertSame(afterCommit, executor.lastResolved(),
            "a pin from an earlier evaluation must not answer for a new one");
      } finally {
        gated.close();
        afterCommit.close();
      }
    });
  }

  @Test
  void terminalRejectionClearsPinsForContextAndCapabilityCalls() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final SirixVectorizedExecutor resolved = executorFactory.get();
      try {
        final RevisionTrackingExecutor contextCall = new RevisionTrackingExecutor(() -> resolved, lifecycle);
        final RevisionTrackingExecutor capabilityCall = new RevisionTrackingExecutor(() -> resolved, lifecycle);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);
        final QueryContext pinnedContext = new BrackitQueryContext();

        assertTrue(contextCall.acceptsSource(source, pinnedContext));
        assertTrue(capabilityCall.acceptsSource(source, pinnedContext));
        assertTrue(hasPinnedExecutor(contextCall));
        assertTrue(hasPinnedExecutor(capabilityCall));

        lifecycle.closeAndAwait();

        // A different context used to leave the old context's pin behind; the no-context
        // capability path used to leave every pin behind. Terminal close is irreversible, so
        // neither reference can belong to useful outer work after admission is rejected.
        assertThrows(IllegalStateException.class, () -> contextCall.canExecute(new BrackitQueryContext()));
        assertThrows(IllegalStateException.class, capabilityCall::supportsSortedScan);
        assertFalse(hasPinnedExecutor(contextCall));
        assertFalse(hasPinnedExecutor(capabilityCall));
      } finally {
        resolved.close();
      }
    });
  }

  private static boolean hasPinnedExecutor(final RevisionTrackingExecutor executor) throws Exception {
    final Field threadLocalField = RevisionTrackingExecutor.class.getDeclaredField("pinnedForEvaluation");
    threadLocalField.setAccessible(true);
    final var threadLocal = (ThreadLocal<?>) threadLocalField.get(executor);
    final Object pin = threadLocal.get();
    final Field executorField = pin.getClass().getDeclaredField("executor");
    executorField.setAccessible(true);
    return executorField.get(pin) != null;
  }

  /**
   * Resolver work is part of the forwarded call: it can open a resource session, inspect its latest
   * revision, and construct an executor. Terminal close must therefore count the call before the
   * resolver starts, not only after an executor has been returned.
   */
  @Test
  void terminalCloseWaitsForResolverAdmittedBeforeSessionAccess() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final var resolverStarted = new CountDownLatch(1);
      final var releaseResolver = new CountDownLatch(1);
      final var resolverObservedOpenSession = new AtomicBoolean();
      final var resolverCalls = new AtomicInteger();
      final var executor = new RevisionTrackingExecutor(() -> {
        resolverCalls.incrementAndGet();
        resolverStarted.countDown();
        try {
          if (!releaseResolver.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to release the resolver");
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Resolver interrupted", e);
        }
        resolverObservedOpenSession.set(!session.isClosed());
        final SirixVectorizedExecutor resolved = executorFactory.get();
        resolved.retire();
        return resolved;
      }, lifecycle);

      final var threads = Executors.newFixedThreadPool(2);
      try {
        final var query = threads.submit(
            () -> executor.executeAggregate(new BrackitQueryContext(), new String[] {"[]"}, "sum", "age"));
        assertTrue(resolverStarted.await(30, TimeUnit.SECONDS), "the query must reach its resolver");

        final var close = threads.submit(() -> {
          lifecycle.closeAndAwait();
          session.close();
        });

        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!lifecycle.isClosed() && System.nanoTime() < deadline) {
          Thread.sleep(1);
        }
        assertTrue(lifecycle.isClosed(), "terminal close must publish its fence");
        assertFalse(close.isDone(), "close must wait for the already-admitted resolver");
        assertFalse(session.isClosed(), "the resource session must remain open while resolution is admitted");

        releaseResolver.countDown();
        assertNotNull(query.get(30, TimeUnit.SECONDS));
        close.get(30, TimeUnit.SECONDS);
        assertTrue(resolverObservedOpenSession.get(), "the resolver must run before its session is closed");
        assertTrue(session.isClosed());
        assertThrows(IllegalStateException.class, () -> executor.canExecute(new BrackitQueryContext()));
        assertEquals(1, resolverCalls.get(), "terminal rejection must happen before resolver access");
      } finally {
        releaseResolver.countDown();
        threads.shutdownNow();
        assertTrue(threads.awaitTermination(30, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  void aPerSourceChildPropagatesTheSameTerminalFence() throws Exception {
    withResource((session, executorFactory, lifecycle) -> {
      final var resolverCalls = new AtomicInteger();
      final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);
      final RevisionTrackingExecutor root = new RevisionTrackingExecutor(executorFactory, ignored -> () -> {
        resolverCalls.incrementAndGet();
        return executorFactory.get();
      }, lifecycle);

      final VectorizedExecutor child = root.executorForSource(source);
      assertEquals(1, resolverCalls.get());
      lifecycle.closeAndAwait();

      assertThrows(IllegalStateException.class, () -> child.canExecute(new BrackitQueryContext()));
      assertEquals(1, resolverCalls.get(), "the child must reject before invoking its resolver");
    });
  }

  /** A null resolver is a programming error, not a decline. */
  @Test
  void aNullResolverIsRejected() {
    final var lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
    assertThrows(IllegalArgumentException.class, () -> new RevisionTrackingExecutor(null, lifecycle));
    assertThrows(IllegalArgumentException.class, () -> new RevisionTrackingExecutor(() -> null, null));
  }

  /** What a test does with an open resource session and a factory for executors bound to it. */
  private interface ResourceTest {
    void run(JsonResourceSession session, Supplier<SirixVectorizedExecutor> executorFactory,
        SirixVectorizedExecutor.ExecutionLifecycle lifecycle) throws Exception;
  }

  /** Creates a throwaway single-revision resource, runs {@code test} against it, and removes it. */
  private static void withResource(final ResourceTest test) throws Exception {
    final Path dbDir = Files.createTempDirectory("sirix-revision-tracking-");
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbDir.resolve(DB)));
      try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
        db.createResource(ResourceConfiguration.newBuilder(RES).build());
        try (final var session = db.beginResourceSession(RES)) {
          try (final var wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"age\":1},{\"age\":2}]"));
            wtx.commit();
          }
          final var lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
          try {
            test.run(session,
                () -> new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 1, lifecycle),
                lifecycle);
          } finally {
            lifecycle.closeAndAwait();
          }
        }
      }
    } finally {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }
}
