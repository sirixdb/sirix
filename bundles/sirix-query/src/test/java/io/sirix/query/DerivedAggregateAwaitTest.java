package io.sirix.query;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class DerivedAggregateAwaitTest {
  private final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
  private final DerivedIndexMaintainer.Resource resource = mock(DerivedIndexMaintainer.Resource.class);
  private final StorageEngineReader source = mock(StorageEngineReader.class);

  private void laggingSource() {
    doReturn(session).when(source).getResourceSession();
    when(source.getRevisionNumber()).thenReturn(2);
    when(source.getUberPage()).thenReturn(new UberPage(3));
    when(session.getDerivedResource()).thenReturn(resource);
    when(resource.maintainedRevision()).thenReturn(1);
  }

  @Test
  void aSuccessfulWaitRetriesAndRetainsTheExactReader() throws Exception {
    laggingSource();
    final StorageEngineReader index = mock(StorageEngineReader.class);
    when(session.openDerivedIndexReader(source)).thenReturn(null, index);
    when(resource.awaitRevision(2, Duration.ofMillis(20))).thenReturn(true);
    try (var readers = new DerivedAggregateReaders()) {
      assertSame(index, readers.borrow(source));
      assertSame(index, readers.borrow(source));
      verify(resource).awaitRevision(2, Duration.ofMillis(20));
      verify(session, times(2)).openDerivedIndexReader(source);
    }
    verify(index).close();
  }

  @Test
  void timeoutIsPaidOnceButMappingsRemainRetryable() throws Exception {
    laggingSource();
    final StorageEngineReader index = mock(StorageEngineReader.class);
    when(session.openDerivedIndexReader(source)).thenReturn(null, null, index);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(source));
      assertNull(readers.borrow(source));
      assertSame(index, readers.borrow(source));
      verify(resource).awaitRevision(2, Duration.ofMillis(20));
    }
  }

  @Test
  void reachingAFrontierDoesNotInventAnInteriorMapping() throws Exception {
    laggingSource();
    when(resource.awaitRevision(2, Duration.ofMillis(20))).thenReturn(true);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(source));
      assertNull(readers.borrow(source));
      verify(resource).awaitRevision(2, Duration.ofMillis(20));
      verify(session, times(3)).openDerivedIndexReader(source);
    }
  }

  @Test
  void aReissuedPublicationGetsItsOwnWaitAttempt() throws Exception {
    laggingSource();
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(source));
      when(source.getUberPage()).thenReturn(new UberPage(3));
      assertNull(readers.borrow(source));
      verify(resource, times(2)).awaitRevision(2, Duration.ofMillis(20));
    }
  }

  @Test
  void exactHitsAndCoveredBatchInteriorsNeverWait() throws Exception {
    laggingSource();
    final StorageEngineReader index = mock(StorageEngineReader.class);
    when(session.openDerivedIndexReader(source)).thenReturn(index);
    try (var readers = new DerivedAggregateReaders()) {
      assertSame(index, readers.borrow(source));
    }
    when(resource.maintainedRevision()).thenReturn(3);
    assertFalse(DerivedAggregateAwait.awaitMapping(source));
    verify(resource, never()).awaitRevision(anyInt(), any(Duration.class));
  }

  @Test
  void disabledFailedAndClosedMaintenanceLeaveTheExactFallback() throws Exception {
    laggingSource();
    assertFalse(DerivedAggregateAwait.awaitMapping(source, Duration.ZERO));
    when(resource.failure()).thenReturn(new IllegalStateException("poisoned"));
    assertFalse(DerivedAggregateAwait.awaitMapping(source));
    verify(resource, never()).awaitRevision(anyInt(), any(Duration.class));
    when(resource.failure()).thenReturn(null);
    when(resource.awaitRevision(2, Duration.ofMillis(20))).thenThrow(new IllegalStateException("closed"));
    assertFalse(DerivedAggregateAwait.awaitMapping(source));
  }

  @Test
  void interruptionPreservesTheFlagAndLeavesTheExactFallback() throws Exception {
    laggingSource();
    when(resource.awaitRevision(2, Duration.ofMillis(20))).thenThrow(new InterruptedException());
    try {
      assertFalse(DerivedAggregateAwait.awaitMapping(source));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
