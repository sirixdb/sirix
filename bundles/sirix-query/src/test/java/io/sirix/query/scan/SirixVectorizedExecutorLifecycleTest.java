/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBObject;
import io.sirix.query.json.ThreadSafeJsonReadOnlyTrx;
import io.sirix.service.json.shredder.JsonShredder;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixVectorizedExecutorLifecycleTest {

  private static JsonResourceSession unusedSessionStub() {
    return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
        new Class<?>[] {JsonResourceSession.class}, (proxy, method, args) -> null);
  }

  @Test
  void genericThreadSafeProxyConstructionDoesNotReadMetadataEagerly() {
    final AtomicInteger metadataReads = new AtomicInteger();
    final JsonNodeReadOnlyTrx owner =
        (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
            new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, args) -> {
              if (method.getName().equals("getResourceSession") || method.getName().equals("getRevisionNumber")
                  || method.getName().equals("getRevisionTimestamp") || method.getName().equals("getMaxNodeKey")
                  || method.getName().equals("getId")) {
                metadataReads.incrementAndGet();
              }
              return defaultValue(method.getReturnType());
            });

    new ThreadSafeJsonReadOnlyTrx(owner);

    assertEquals(0, metadataReads.get(), "the generic proxy constructor must remain metadata-lazy");
  }

  @Test
  void reentrantTerminalCloseFailsBeforePartialTeardown() {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    executor.enterExecution();
    try {
      assertThrows(IllegalStateException.class, executor::close);
      assertFalse(executor.isClosed(), "reentrant close must fail before shutting down worker pools");
    } finally {
      executor.leaveExecution();
    }
    executor.close();
    assertTrue(executor.isClosed());
  }

  @Test
  void closeFencesSessionBoundProjectionWarmup() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final ExecutorService warmupPool = projectionWarmupPool(executor);
    final CountDownLatch warmupStarted = new CountDownLatch(1);
    final CountDownLatch releaseWarmup = new CountDownLatch(1);
    final CountDownLatch closeReturned = new CountDownLatch(1);
    final AtomicReference<Throwable> closeFailure = new AtomicReference<>();

    warmupPool.execute(() -> {
      warmupStarted.countDown();
      awaitUninterruptibly(releaseWarmup);
    });
    assertTrue(warmupStarted.await(5, TimeUnit.SECONDS));

    final Thread closer = new Thread(() -> {
      try {
        executor.close();
      } catch (final Throwable failure) {
        closeFailure.set(failure);
      } finally {
        closeReturned.countDown();
      }
    }, "sirix-vectorized-close-test");
    closer.start();
    try {
      awaitTerminalPublication(executionLifecycle(executor));
      assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS),
          "close must not return while a warmup job can still use the resource session");
    } finally {
      releaseWarmup.countDown();
    }

    assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
    closer.join(5_000L);
    assertFalse(closer.isAlive());
    assertTrue(closeFailure.get() == null, () -> "close failed: " + closeFailure.get());
  }

  /**
   * Closing an executor must never INTERRUPT a warm-up job that is already running.
   *
   * <p>
   * The warm-up lane reads through the resource's striped {@code FileChannel}s, which the storage
   * lends to every reader of that resource at once. {@code FileChannel} is an
   * {@code InterruptibleChannel}: interrupting a thread blocked in one of its operations closes the
   * channel — not just for that thread, but for every other borrower of the same stripe. While close
   * used {@code shutdownNow()}, a per-query executor closing over a running segment sweep therefore
   * took down an unrelated concurrent parallel scan with a bare {@code ClosedChannelException}:
   * ClickBench query 20 failed that way in 2 of 8 full sweeps.
   * </p>
   *
   * <p>
   * The job below waits for close to publish the shutdown and then sleeps, so an interrupt issued by
   * close lands inside a call that reports it. A queued-but-unstarted job is a different case and
   * stays cancellable — {@link #closeCancelsQueuedWarmupsWithoutInterruptingTheRunningOne} pins that
   * half.
   * </p>
   */
  @Test
  void closeMustNotInterruptARunningWarmupHoldingSharedStorageChannels() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final ExecutorService warmupPool = projectionWarmupPool(executor);
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicBoolean interrupted = new AtomicBoolean();

    warmupPool.execute(() -> {
      started.countDown();
      try {
        while (!warmupPool.isShutdown()) {
          Thread.onSpinWait();
        }
        // Shutdown is published; an interrupt from close is issued immediately after it, so this
        // sleep either completes untouched or reports the interrupt.
        Thread.sleep(500L);
      } catch (final InterruptedException interrupt) {
        interrupted.set(true);
        Thread.currentThread().interrupt();
      } finally {
        finished.countDown();
      }
    });
    assertTrue(started.await(5, TimeUnit.SECONDS), "the warm-up job must reach the lane");

    executor.close();

    assertTrue(finished.await(5, TimeUnit.SECONDS), "close must await the running warm-up job");
    assertFalse(interrupted.get(),
        "close interrupted a running warm-up job — that closes the SHARED storage channel and fails "
            + "every concurrent reader of the resource");
    assertTrue(executor.isClosed());
  }

  /** The other half of the contract: a job that never started is still cancelled, not run. */
  @Test
  void closeCancelsQueuedWarmupsWithoutInterruptingTheRunningOne() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final ExecutorService warmupPool = projectionWarmupPool(executor);
    final CountDownLatch runningStarted = new CountDownLatch(1);
    final CountDownLatch releaseRunning = new CountDownLatch(1);
    final AtomicBoolean queuedRan = new AtomicBoolean();
    final AtomicBoolean queuedCancelled = new AtomicBoolean();

    warmupPool.execute(() -> {
      runningStarted.countDown();
      awaitUninterruptibly(releaseRunning);
    });
    assertTrue(runningStarted.await(5, TimeUnit.SECONDS));
    // Single-threaded lane, so this one cannot start while the job above holds it.
    warmupPool.execute(new ProjectionIndexRegistry.CancellableBackgroundTask() {
      @Override
      public void run() {
        queuedRan.set(true);
      }

      @Override
      public void cancelBeforeExecution() {
        queuedCancelled.set(true);
      }
    });

    final CountDownLatch closeReturned = new CountDownLatch(1);
    final Thread closer = new Thread(() -> {
      try {
        executor.close();
      } finally {
        closeReturned.countDown();
      }
    }, "sirix-vectorized-close-queued-test");
    closer.start();
    try {
      assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS),
          "close must not return while the running warm-up job holds the lane");
    } finally {
      releaseRunning.countDown();
    }
    assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
    closer.join(5_000L);

    assertFalse(queuedRan.get(), "a queued warm-up must not run after close");
    assertTrue(queuedCancelled.get(),
        "a queued warm-up must receive its cancellation hook, or the one-shot latch it guards is " + "never released");
  }

  @Test
  void closeFencesRecordMaterializationLanes() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final CountDownLatch laneStarted = new CountDownLatch(1);
    final CountDownLatch releaseLane = new CountDownLatch(1);
    final CountDownLatch materializationReturned = new CountDownLatch(1);
    final CountDownLatch closeReturned = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread materializer = new Thread(() -> {
      try {
        executor.parallelRecordMaterialization(1, ignored -> {
          laneStarted.countDown();
          awaitUninterruptibly(releaseLane);
        });
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      } finally {
        materializationReturned.countDown();
      }
    }, "sirix-record-materialization-test");
    materializer.start();
    assertTrue(laneStarted.await(5, TimeUnit.SECONDS));

    final Thread closer = new Thread(() -> {
      try {
        executor.close();
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      } finally {
        closeReturned.countDown();
      }
    }, "sirix-record-materialization-close-test");
    closer.start();
    try {
      awaitTerminalPublication(executionLifecycle(executor));
      assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS),
          "close must not return while a record-materialization lane can still use the session");
    } finally {
      releaseLane.countDown();
    }

    assertTrue(materializationReturned.await(5, TimeUnit.SECONDS));
    assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
    materializer.join(5_000L);
    closer.join(5_000L);
    assertFalse(materializer.isAlive());
    assertFalse(closer.isAlive());
    assertTrue(failure.get() == null, () -> "lifecycle operation failed: " + failure.get());
  }

  @Test
  void terminalCloseFencesADegradedInlineCallAndRejectsLateWork() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    executor.retire();
    final CountDownLatch inlineStarted = new CountDownLatch(1);
    final CountDownLatch releaseInline = new CountDownLatch(1);
    final CountDownLatch closeReturned = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread caller = new Thread(() -> {
      try {
        executor.parallel(1, ignored -> {
          inlineStarted.countDown();
          awaitUninterruptibly(releaseInline);
        });
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      }
    }, "sirix-retired-inline-test");
    caller.start();
    assertTrue(inlineStarted.await(5, TimeUnit.SECONDS));

    final Thread closer = new Thread(() -> {
      try {
        executor.close();
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      } finally {
        closeReturned.countDown();
      }
    }, "sirix-terminal-close-test");
    closer.start();
    try {
      awaitTerminalPublication(executionLifecycle(executor));
      assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS),
          "terminal close must wait for inline work admitted before its fence");
    } finally {
      releaseInline.countDown();
    }

    assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
    caller.join(5_000L);
    closer.join(5_000L);
    assertFalse(caller.isAlive());
    assertFalse(closer.isAlive());
    assertTrue(failure.get() == null, () -> "lifecycle operation failed: " + failure.get());

    final AtomicBoolean lateTaskRan = new AtomicBoolean();
    assertThrows(IllegalStateException.class, () -> executor.parallel(1, ignored -> lateTaskRan.set(true)));
    assertFalse(lateTaskRan.get(), "terminal close must reject rather than run late work inline");
  }

  @Test
  void sharedTerminalFenceDrainsTopLevelWorkOnAnAlreadyRetiredExecutor() throws Exception {
    final SirixVectorizedExecutor.ExecutionLifecycle lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
    final SirixVectorizedExecutor retired = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1, lifecycle);
    retired.retire();
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch closeReturned = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread query = new Thread(() -> {
      try {
        retired.enterExecution();
        try {
          entered.countDown();
          awaitUninterruptibly(release);
        } finally {
          retired.leaveExecution();
        }
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      }
    }, "sirix-retired-top-level-test");
    query.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS));

    final Thread closer = new Thread(() -> {
      try {
        lifecycle.closeAndAwait();
      } catch (final Throwable throwable) {
        failure.compareAndSet(null, throwable);
      } finally {
        closeReturned.countDown();
      }
    }, "sirix-shared-lifecycle-close-test");
    closer.start();
    try {
      awaitTerminalPublication(lifecycle);
      assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS),
          "the chain fence must retain retired executors' admitted top-level calls");
    } finally {
      release.countDown();
    }

    assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
    query.join(5_000L);
    closer.join(5_000L);
    assertTrue(failure.get() == null, () -> "lifecycle operation failed: " + failure.get());
    assertThrows(IllegalStateException.class, retired::enterExecution);
    retired.close();
  }

  @Test
  void revisionAdvancesKeepScanAndLazyResultTransactionsBounded() {
    final TrackingSession tracking = new TrackingSession();
    final SirixVectorizedExecutor.ExecutionLifecycle lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
    final JsonDBCollection collection =
        (JsonDBCollection) Proxy.newProxyInstance(JsonDBCollection.class.getClassLoader(),
            new Class<?>[] {JsonDBCollection.class}, (proxy, method, args) -> defaultValue(method.getReturnType()));
    final List<SirixVectorizedExecutor> executors = new ArrayList<>();
    final List<JsonDBObject> oldResults = new ArrayList<>();

    try {
      for (int revision = 1; revision <= 32; revision++) {
        final int boundRevision = revision;
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(tracking.session, boundRevision, 4, lifecycle);
        executors.add(executor);

        // This is the ordinary aggregate/scan cursor route. Each fixed worker keeps one cursor for
        // executor-local reuse; retirement must release those four slots in one bounded step.
        executor.parallel(4, lane -> assertEquals(1_000L + boundRevision, executor.workerTrx().getNodeKey()));
        final int scanAndPriorConsumerCursors = revision == 1
            ? 4
            : 5;
        assertEquals(scanAndPriorConsumerCursors, tracking.active.get(),
            "one cursor per live worker plus the revision-rebindable consumer cursor is expected");

        final JsonNodeReadOnlyTrx constructionCursor = executor.recordTrx();
        final JsonDBObject lazyResult;
        try {
          assertTrue(constructionCursor.moveTo(1_000L + boundRevision));
          lazyResult = new JsonDBObject(constructionCursor, collection);
        } finally {
          executor.releaseRecordTrx(constructionCursor);
        }
        assertEquals(scanAndPriorConsumerCursors, tracking.active.get(),
            "materialization owner must close without disturbing reusable worker cursors");
        assertEquals(1_000L + boundRevision, lazyResult.getNodeKey());
        assertEquals(boundRevision, lazyResult.getTrx().getRevisionNumber());
        assertEquals(Instant.EPOCH.plusSeconds(boundRevision), lazyResult.getTrx().getRevisionTimestamp());
        assertEquals(10_000L + boundRevision, lazyResult.getTrx().getMaxNodeKey());
        assertEquals(5, tracking.active.get(),
            "four reusable worker cursors plus one revision-rebindable consumer cursor are expected");
        oldResults.add(lazyResult);
        executor.retire();
        assertEquals(1, tracking.active.get(),
            "retiring a revision must release all worker cursors and retain only the consumer slot");
      }

      assertEquals(1, tracking.active.get(), "revision advances must leave one consumer cursor, not 32 lane pools");
      assertTrue(tracking.maximum.get() <= 6,
          () -> "active transactions exceeded four workers, one consumer, and one transient owner: "
              + tracking.maximum.get());
      assertEquals(1_001L, oldResults.getFirst().getNodeKey(),
          "an old lazy object must transparently reopen its immutable revision");
      assertEquals(1, tracking.active.get(), "reopening an old revision must replace, not add, a cursor");
      assertEquals(0, tracking.sharedLookups.get(),
          "executor cursors must not move the leak into the session's (thread,revision) shared map");
    } finally {
      lifecycle.closeAndAwait();
      for (final SirixVectorizedExecutor executor : executors) {
        executor.close();
      }
    }
    assertEquals(0, tracking.active.get(), "terminal lifecycle close must unregister its last consumer cursor");
  }

  @Test
  void realSessionTransactionMapStaysBoundedAcrossRevisionsAndOldLazyResults() throws Exception {
    final Path databasePath = Files.createTempDirectory("sirix-executor-lifecycle-").resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (final var database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder("records").build());
        try (final JsonResourceSession session = database.beginResourceSession("records")) {
          try (final var wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":0}"));
            wtx.commit();
            for (int revision = 2; revision <= 12; revision++) {
              assertTrue(wtx.moveToDocumentRoot());
              assertTrue(wtx.moveToFirstChild());
              assertTrue(wtx.moveToFirstChild());
              wtx.setNumberValue(revision);
              wtx.commit();
            }
          }

          final AbstractResourceSession<?, ?> trackedSession = assertInstanceOf(AbstractResourceSession.class, session);
          final SirixVectorizedExecutor.ExecutionLifecycle lifecycle = new SirixVectorizedExecutor.ExecutionLifecycle();
          final JsonDBCollection collection =
              (JsonDBCollection) Proxy.newProxyInstance(JsonDBCollection.class.getClassLoader(),
                  new Class<?>[] {JsonDBCollection.class},
                  (proxy, method, args) -> defaultValue(method.getReturnType()));
          final List<SirixVectorizedExecutor> executors = new ArrayList<>();
          final List<JsonDBObject> results = new ArrayList<>();
          try {
            for (int revision = 1; revision <= 12; revision++) {
              final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2, lifecycle);
              executors.add(executor);
              executor.parallel(2, lane -> assertTrue(executor.workerTrx().moveToDocumentRoot()));
              final int scanAndPriorConsumerCursors = revision == 1
                  ? 2
                  : 3;
              assertEquals(scanAndPriorConsumerCursors, trackedSession.activeTrxCount(),
                  "fixed workers retain one reusable cursor until this revision executor retires");

              final JsonNodeReadOnlyTrx constructionCursor = executor.recordTrx();
              final JsonDBObject result;
              try {
                result = new JsonDBObject(constructionCursor, collection);
              } finally {
                executor.releaseRecordTrx(constructionCursor);
              }
              assertEquals(scanAndPriorConsumerCursors, trackedSession.activeTrxCount(),
                  "the construction transaction must close while reusable worker cursors remain");
              assertTrue(result.getNodeKey() >= 0);
              assertEquals(3, trackedSession.activeTrxCount(),
                  "two workers and one revision-rebindable consumer cursor must remain bounded");
              results.add(result);
              executor.retire();
              assertEquals(1, trackedSession.activeTrxCount(),
                  "retirement must release both worker cursors without closing the lazy consumer");
            }

            final long oldestNodeKey = results.getFirst().getNodeKey();
            assertTrue(oldestNodeKey >= 0, "the oldest lazy object must remain readable");
            assertEquals(1, trackedSession.activeTrxCount(),
                "reopening revision 1 must replace the revision 12 cursor in nodeTrxMap");
          } finally {
            lifecycle.closeAndAwait();
            for (final SirixVectorizedExecutor executor : executors) {
              executor.close();
            }
          }
          assertEquals(0, trackedSession.activeTrxCount(),
              "terminal lifecycle close must empty the real session transaction map");
        }
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }
  }

  @Test
  void workerFailureDrainsEveryAlreadySubmittedLane() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 2);
    final CountDownLatch secondStarted = new CountDownLatch(1);
    final CountDownLatch secondDone = new CountDownLatch(1);
    final AtomicBoolean parallelReturned = new AtomicBoolean();
    final AtomicBoolean secondObservedEarlyReturn = new AtomicBoolean();
    try {
      assertThrows(IllegalStateException.class, () -> executor.parallel(2, lane -> {
        if (lane == 0) {
          assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
          throw new IllegalStateException("fallback signal");
        }
        secondStarted.countDown();
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
        while (!parallelReturned.get() && System.nanoTime() < deadline) {
          LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
        }
        secondObservedEarlyReturn.set(parallelReturned.get());
        secondDone.countDown();
      }));
      parallelReturned.set(true);

      assertTrue(secondDone.await(5, TimeUnit.SECONDS));
      assertFalse(secondObservedEarlyReturn.get(),
          "a failed first Future must not let a still-running sibling escape parallel()");
    } finally {
      parallelReturned.set(true);
      executor.close();
    }
  }

  @Test
  void interruptedJoinStillWaitsForThatWorkerAndRestoresInterrupt() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final CountDownLatch workerStarted = new CountDownLatch(1);
    final CountDownLatch releaseWorker = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean interruptRestored = new AtomicBoolean();
    final Thread caller = new Thread(() -> {
      try {
        executor.parallel(1, lane -> {
          workerStarted.countDown();
          assertTrue(releaseWorker.await(5, TimeUnit.SECONDS));
        });
      } catch (final Throwable throwable) {
        failure.set(throwable);
        interruptRestored.set(Thread.currentThread().isInterrupted());
      }
    }, "sirix-parallel-interrupt-test");

    try {
      caller.start();
      assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
      caller.interrupt();
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
      assertTrue(caller.isAlive(), "an interrupted Future.get must re-wait the same live worker");

      releaseWorker.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(caller.isAlive());
      assertInstanceOf(RuntimeException.class, failure.get());
      assertTrue(interruptRestored.get(), "parallel() must restore the caller's interrupt status after draining");
    } finally {
      releaseWorker.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(5));
      executor.close();
    }
  }

  private static ExecutorService projectionWarmupPool(final SirixVectorizedExecutor executor) throws Exception {
    final Field field = SirixVectorizedExecutor.class.getDeclaredField("projectionWarmupPool");
    field.setAccessible(true);
    return (ExecutorService) field.get(executor);
  }

  private static SirixVectorizedExecutor.ExecutionLifecycle executionLifecycle(final SirixVectorizedExecutor executor)
      throws Exception {
    final Field field = SirixVectorizedExecutor.class.getDeclaredField("executionLifecycle");
    field.setAccessible(true);
    return (SirixVectorizedExecutor.ExecutionLifecycle) field.get(executor);
  }

  private static void awaitTerminalPublication(final SirixVectorizedExecutor.ExecutionLifecycle lifecycle) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!lifecycle.isClosed() && System.nanoTime() < deadline) {
      LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
    }
    assertTrue(lifecycle.isClosed(), "terminal close must publish its fence before the wait assertion");
  }

  private static void awaitUninterruptibly(final CountDownLatch latch) {
    boolean interrupted = false;
    for (;;) {
      try {
        latch.await();
        break;
      } catch (final InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static Object defaultValue(final Class<?> type) {
    if (!type.isPrimitive()) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == float.class) {
      return 0F;
    }
    if (type == double.class) {
      return 0D;
    }
    if (type == char.class) {
      return '\0';
    }
    return null;
  }

  /** Fake resource session exposing exact registered-transaction cardinality. */
  private static final class TrackingSession {
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maximum = new AtomicInteger();
    private final AtomicInteger nextId = new AtomicInteger();
    private final AtomicInteger sharedLookups = new AtomicInteger();
    private final JsonResourceSession session =
        (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
            new Class<?>[] {JsonResourceSession.class}, this::invokeSession);

    private Object invokeSession(final Object proxy, final Method method, final Object[] args) {
      return switch (method.getName()) {
        case "beginNodeReadOnlyTrx" -> newCursor((int) args[0]);
        case "getOrCreateSharedReadOnlyTrx" -> {
          sharedLookups.incrementAndGet();
          throw new AssertionError("SirixVectorizedExecutor must not use sharedTrxMap");
        }
        case "isClosed" -> false;
        case "toString" -> "tracking-json-resource-session";
        default -> defaultValue(method.getReturnType());
      };
    }

    private JsonNodeReadOnlyTrx newCursor(final int revision) {
      final int now = active.incrementAndGet();
      maximum.accumulateAndGet(now, Math::max);
      final int id = nextId.incrementAndGet();
      final AtomicBoolean closed = new AtomicBoolean();
      return (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
          new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, args) -> switch (method.getName()) {
            case "close" -> {
              if (closed.compareAndSet(false, true)) {
                active.decrementAndGet();
              }
              yield null;
            }
            case "isClosed" -> closed.get();
            case "getResourceSession" -> session;
            case "getRevisionNumber" -> revision;
            case "getRevisionTimestamp" -> Instant.EPOCH.plusSeconds(revision);
            case "getMaxNodeKey" -> 10_000L + revision;
            case "getId" -> id;
            case "getNodeKey" -> 1_000L + revision;
            case "getFirstChildKey" -> -1L;
            case "isDocumentRoot" -> false;
            case "moveTo" -> true;
            case "toString" -> "tracking-json-read-trx-" + revision;
            default -> defaultValue(method.getReturnType());
          });
    }
  }
}
