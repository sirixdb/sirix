/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
  void closeLeavesEscapingRecordCursorsOwnedByTheSession() throws Exception {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(unusedSessionStub(), 1, 1);
    final AtomicBoolean cursorClosed = new AtomicBoolean();
    final JsonNodeReadOnlyTrx cursor = (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(
        JsonNodeReadOnlyTrx.class.getClassLoader(), new Class<?>[] {JsonNodeReadOnlyTrx.class},
        (proxy, method, args) -> {
          if (method.getName().equals("close")) {
            cursorClosed.set(true);
          }
          if (method.getName().equals("isClosed")) {
            return cursorClosed.get();
          }
          return null;
        });
    setField(executor, "recordTrx", cursor);
    setField(executor, "recordTrxLanes", new JsonNodeReadOnlyTrx[] {cursor});

    executor.close();

    assertFalse(cursorClosed.get(),
        "executor retirement must not invalidate cursors retained by already-returned lazy items");
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

  private static void setField(final SirixVectorizedExecutor executor, final String name, final Object value)
      throws Exception {
    final Field field = SirixVectorizedExecutor.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(executor, value);
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
}
