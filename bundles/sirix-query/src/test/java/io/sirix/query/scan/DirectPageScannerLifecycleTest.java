package io.sirix.query.scan;

import java.util.concurrent.CountDownLatch;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import org.junit.jupiter.api.Test;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@Timeout(10)
final class DirectPageScannerLifecycleTest {
  @Test
  void publicScanUsesDaemonWorkersAndClosesTheirReaders() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final JsonNodeReadOnlyTrx node = mock(JsonNodeReadOnlyTrx.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    when(session.beginNodeReadOnlyTrx(1)).thenReturn(node);
    when(node.getMaxNodeKey()).thenReturn(0L);
    final AtomicBoolean daemon = new AtomicBoolean();
    when(session.createStorageEngineReader(1)).thenAnswer(invocation -> {
      daemon.set(Thread.currentThread().isDaemon());
      return reader;
    });
    assertEquals(0, DirectPageScanner.countTotal(session, 1, 2));
    assertTrue(daemon.get());
    verify(node).close();
    verify(reader).close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"interrupt", "workerFailure", "startFailure"})
  void failureWaitsForWorkersThatHaveNotYetHonoredCancellation(final String mode) throws InterruptedException {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch awaitingTermination = new CountDownLatch(1);
    final AtomicBoolean exited = new AtomicBoolean();
    final AtomicBoolean cancelled = new AtomicBoolean();
    final AtomicBoolean returned = new AtomicBoolean();
    final AtomicBoolean interruptRestored = new AtomicBoolean();
    final AtomicReference<Throwable> observed = new AtomicReference<>();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
      private int submitted;

      @Override
      public void execute(final Runnable command) {
        if (++submitted == 2 && mode.equals("startFailure")) {
          awaitUninterruptibly(entered);
          throw new RejectedExecutionException("injected partial submission failure");
        }
        super.execute(command);
      }

      private boolean firstWait = true;

      @Override
      public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        if (firstWait) {
          firstWait = false;
          return false; // Inject an expired wait without introducing a wall-clock assertion.
        }
        awaitingTermination.countDown();
        return super.awaitTermination(timeout, unit);
      }
    };
    final Thread owner = new Thread(() -> {
      try {
        DirectPageScanner.parallel(2, index -> {
          if (mode.equals("workerFailure") && index == 0) {
            entered.await();
            throw new IllegalStateException("injected worker failure");
          }
          entered.countDown();
          awaitUninterruptibly(release);
          cancelled.set(Thread.currentThread().isInterrupted());
          exited.set(true);
        }, executor);
      } catch (final Throwable failure) {
        observed.set(failure);
      } finally {
        interruptRestored.set(Thread.currentThread().isInterrupted());
        returned.set(true);
        awaitingTermination.countDown();
      }
    });
    owner.start();
    try {
      entered.await();
      if (mode.equals("interrupt")) {
        owner.interrupt();
      }
      awaitingTermination.await();
      assertFalse(returned.get());
      assertFalse(exited.get());
    } finally {
      release.countDown();
      owner.join();
      executor.shutdownNow();
    }
    assertTrue(executor.isTerminated());
    assertTrue(exited.get());
    assertTrue(cancelled.get());
    assertNotNull(observed.get());
    if (mode.equals("interrupt")) {
      assertTrue(interruptRestored.get());
    }
  }

  private static void awaitUninterruptibly(final CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (final InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
