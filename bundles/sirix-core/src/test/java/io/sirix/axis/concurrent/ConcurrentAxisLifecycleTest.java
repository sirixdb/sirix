package io.sirix.axis.concurrent;

import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.Filter;
import io.sirix.axis.ForAxis;
import io.sirix.axis.NestedAxis;
import io.sirix.axis.NonStructuralWrapperAxis;
import io.sirix.axis.SelfAxis;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.PredicateFilterAxis;
import java.util.concurrent.ExecutorService;
import io.sirix.exception.SirixThreadedException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyLong;

@Timeout(10)
final class ConcurrentAxisLifecycleTest {
  @ParameterizedTest
  @ValueSource(strings = {"close", "reset"})
  void cancellationClosesNestedConcurrentProducers(final String mode) throws InterruptedException {
    final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class);
    when(cursor.moveTo(anyLong())).thenReturn(true);
    final Axis source = mock(Axis.class);
    final AtomicInteger produced = new AtomicInteger();
    final CountDownLatch prefetched = new CountDownLatch(1);
    when(source.hasNext()).thenReturn(true);
    when(source.nextLong()).thenAnswer(invocation -> {
      final int key = produced.incrementAndGet();
      if (key == 300) {
        prefetched.countDown();
      }
      return (long) key;
    });
    final ConcurrentAxis<JsonNodeReadOnlyTrx> inner = new ConcurrentAxis<>(cursor, source);
    final JsonNodeReadOnlyTrx outerCursor = mock(JsonNodeReadOnlyTrx.class);
    when(outerCursor.moveTo(anyLong())).thenReturn(true);
    @SuppressWarnings("unchecked")
    final Filter<JsonNodeReadOnlyTrx> filter = mock(Filter.class);
    when(filter.getTrx()).thenReturn(cursor);
    when(filter.filter()).thenReturn(true);
    final Axis nested = new NestedAxis(new FilterAxis<>(inner, filter), new SelfAxis(cursor));
    try (ConcurrentAxis<JsonNodeReadOnlyTrx> outer = new ConcurrentAxis<>(outerCursor, nested)) {
      assertTrue(outer.hasNext());
      outer.nextLong();
      prefetched.await();
      final ExecutorService outerExecutor = outer.executorService;
      final ExecutorService innerExecutor = inner.executorService;
      if (mode.equals("reset")) {
        outer.reset(0);
      } else {
        outer.close();
      }
      assertTrue(outerExecutor.isTerminated());
      assertTrue(innerExecutor.isTerminated());
    }
    assertTrue(inner.executorService.isTerminated());
  }

  @ParameterizedTest
  @ValueSource(strings = {"nested", "for", "filter", "predicate", "nonStructural", "union", "intersect", "except"})
  void compositeCloseAttemptsEveryOwnedAxis(final String kind) {
    final XmlNodeReadOnlyTrx cursor = mock(XmlNodeReadOnlyTrx.class);
    final Axis first = mock(Axis.class);
    final Axis second = mock(Axis.class);
    when(first.getCursor()).thenReturn(cursor);
    when(first.asXmlNodeReadTrx()).thenReturn(cursor);
    when(second.getCursor()).thenReturn(mock(XmlNodeReadOnlyTrx.class));
    final IllegalStateException failure = new IllegalStateException("close");
    doThrow(failure).when(first).close();
    @SuppressWarnings("unchecked")
    final Filter<XmlNodeReadOnlyTrx> filter = mock(Filter.class);
    when(filter.getTrx()).thenReturn(cursor);
    final Axis composite = switch (kind) {
      case "for" -> new ForAxis(first, second);
      case "filter" -> new FilterAxis<>(first, filter);
      case "predicate" -> new PredicateFilterAxis(cursor, first);
      case "nonStructural" -> new NonStructuralWrapperAxis(first);
      case "union" -> new ConcurrentUnionAxis<>(mock(XmlNodeReadOnlyTrx.class), first, second);
      case "intersect" -> new ConcurrentIntersectAxis<>(mock(XmlNodeReadOnlyTrx.class), first, second);
      case "except" -> new ConcurrentExceptAxis<>(mock(XmlNodeReadOnlyTrx.class), first, second);
      default -> new NestedAxis(first, second);
    };
    assertSame(failure, assertThrows(IllegalStateException.class, composite::close));
    verify(first).close();
    if (kind.equals("nested") || kind.equals("for") || kind.equals("union") || kind.equals("intersect")
        || kind.equals("except")) {
      verify(second).close();
    }
  }

  @Test
  void helperClosesDescendantsAfterFailureAndRetainsThePrimaryError() {
    final Axis producer = mock(Axis.class);
    final IllegalStateException failure = new IllegalStateException("iteration");
    final IllegalStateException cleanup = new IllegalStateException("descendant close");
    when(producer.hasNext()).thenThrow(failure);
    doThrow(cleanup).when(producer).close();
    final ConcurrentAxisHelper helper = new ConcurrentAxisHelper(producer, new LinkedBlockingQueue<>());
    helper.run();
    verify(producer).close();
    assertSame(failure, helper.failure());
    assertSame(cleanup, failure.getSuppressed()[0]);
  }

  @ParameterizedTest
  @ValueSource(strings = {"close", "reset", "interrupt", "singleInterrupt", "alreadyShutdown"})
  void cancellationFencesProducerBeforeReturnOrReset(final String mode) throws InterruptedException {
    final boolean interruptMode = mode.equals("interrupt") || mode.equals("singleInterrupt");
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch awaitingTermination = new CountDownLatch(1);
    final AtomicBoolean exited = new AtomicBoolean();
    final AtomicBoolean returned = new AtomicBoolean();
    final AtomicBoolean resetWhileActive = new AtomicBoolean();
    final AtomicBoolean interruptRestored = new AtomicBoolean();
    final AtomicReference<Throwable> observed = new AtomicReference<>();
    final AtomicInteger calls = new AtomicInteger();
    final Axis producer = mock(Axis.class);
    when(producer.hasNext()).thenAnswer(invocation -> {
      if (!interruptMode && calls.getAndIncrement() == 0) {
        return true;
      }
      entered.countDown();
      awaitUninterruptibly(release);
      exited.set(true);
      return false;
    });
    when(producer.nextLong()).thenReturn(1L);
    doAnswer(invocation -> {
      resetWhileActive.set(!exited.get());
      return null;
    }).when(producer).reset(0);
    final ConcurrentAxis<JsonNodeReadOnlyTrx> axis = new ConcurrentAxis<>(mock(JsonNodeReadOnlyTrx.class), producer);
    axis.executorService.shutdown();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
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
    axis.executorService = executor;
    if (!interruptMode) {
      assertEquals(1, axis.nextKey());
    }
    final Thread owner = new Thread(() -> {
      try {
        switch (mode) {
          case "reset" -> axis.reset(0);
          case "interrupt", "singleInterrupt" -> axis.nextKey();
          case "alreadyShutdown" -> {
            executor.shutdownNow();
            axis.close();
          }
          default -> axis.close();
        }
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
      if (interruptMode) {
        owner.interrupt();
      }
      awaitingTermination.await();
      assertFalse(returned.get());
      assertFalse(resetWhileActive.get());
      assertFalse(exited.get());
      // Also test a single interruption, so a later one cannot mask a lost original signal.
      if (!mode.equals("singleInterrupt")) {
        owner.interrupt();
      }
    } finally {
      release.countDown();
      owner.join();
      axis.close();
    }
    assertTrue(executor.isTerminated());
    assertTrue(exited.get());
    assertFalse(resetWhileActive.get());
    assertTrue(interruptRestored.get());
    if (interruptMode) {
      assertInstanceOf(SirixThreadedException.class, observed.get());
    }
  }

  @Test
  void resetDiscardsBufferedResultsAndExhaustionTerminatesProducer() throws InterruptedException {
    final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class);
    when(cursor.moveTo(anyLong())).thenReturn(true);
    final Axis producer = mock(Axis.class);
    final AtomicInteger generation = new AtomicInteger();
    final AtomicInteger produced = new AtomicInteger();
    when(producer.hasNext()).thenAnswer(invocation -> produced.getAndIncrement() < (generation.get() == 0
        ? 2
        : 1));
    when(producer.nextLong()).thenAnswer(invocation -> generation.get() == 0
        ? 1L
        : 7L);
    doAnswer(invocation -> {
      generation.incrementAndGet();
      produced.set(0);
      return null;
    }).when(producer).reset(7);
    try (final ConcurrentAxis<JsonNodeReadOnlyTrx> axis = new ConcurrentAxis<>(cursor, producer)) {
      assertTrue(axis.hasNext()); // Leave AbstractAxis READY as well as prefetched keys in the queue.
      axis.executorService.shutdown();
      assertTrue(axis.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
      axis.reset(7);
      verify(producer).reset(7);
      assertEquals(7, axis.nextLong());
      assertFalse(axis.hasNext());
      assertTrue(axis.isFinished());
      assertTrue(axis.executorService.isTerminated());
      assertEquals(-1, axis.nextKey());
      assertEquals(-1, axis.done());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"result", "end"})
  @SuppressWarnings("unchecked")
  void producerRestoresInterruptionDuringQueuePublication(final String stage) throws InterruptedException {
    final Axis producer = mock(Axis.class);
    when(producer.hasNext()).thenReturn(stage.equals("result"));
    when(producer.nextLong()).thenReturn(1L);
    final BlockingQueue<Long> queue = mock(BlockingQueue.class);
    doThrow(new InterruptedException("cancel publication")).when(queue).put(anyLong());
    final AtomicBoolean restored = new AtomicBoolean();
    final Thread worker = new Thread(() -> {
      new ConcurrentAxisHelper(producer, queue).run();
      restored.set(Thread.currentThread().isInterrupted());
    });
    worker.start();
    worker.join();
    assertTrue(restored.get());
  }

  @Test
  void workerFailureIsReportedAfterTermination() {
    final Axis producer = mock(Axis.class);
    final AssertionError failure = new AssertionError("worker failed");
    when(producer.hasNext()).thenThrow(failure);
    try (final ConcurrentAxis<JsonNodeReadOnlyTrx> axis =
        new ConcurrentAxis<>(mock(JsonNodeReadOnlyTrx.class), producer)) {
      assertSame(failure, assertThrows(AssertionError.class, axis::nextKey));
      assertTrue(axis.executorService.isTerminated());
    }
  }

  @Test
  void submissionFailureClosesExecutor() {
    final Axis producer = mock(Axis.class);
    try (final ConcurrentAxis<JsonNodeReadOnlyTrx> axis =
        new ConcurrentAxis<>(mock(JsonNodeReadOnlyTrx.class), producer)) {
      axis.executorService.shutdown();
      assertThrows(RejectedExecutionException.class, axis::nextKey);
      assertTrue(axis.executorService.isTerminated());
      assertTrue(axis.isFinished());
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
