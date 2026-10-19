package io.sirix.query;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class DerivedAggregateNonblockingTest {
  @Test
  void laggingMappingsStayRetryableWithoutWaitingForTheMaintainer() throws Exception {
    final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
    final DerivedIndexMaintainer.Resource resource = mock(DerivedIndexMaintainer.Resource.class);
    final StorageEngineReader source = mock(StorageEngineReader.class);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    doReturn(session).when(source).getResourceSession();
    when(source.getRevisionNumber()).thenReturn(2);
    when(source.getUberPage()).thenReturn(new UberPage(3));
    when(session.getDerivedResource()).thenReturn(resource);
    when(session.openDerivedIndexReader(source)).thenReturn(null, null, index);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(source));
      assertNull(readers.borrow(source));
      assertSame(index, readers.borrow(source));
      assertSame(index, readers.borrow(source));
      verify(session, times(3)).openDerivedIndexReader(source);
      verify(resource, never()).awaitRevision(anyInt(), any(Duration.class));
    }
    verify(index).close();
  }
}
