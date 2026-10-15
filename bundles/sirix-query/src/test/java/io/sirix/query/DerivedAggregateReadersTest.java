package io.sirix.query;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class DerivedAggregateReadersTest {
  @Test
  void distinctSourceReadersReuseOnlyTheSamePublicationAndRetryLag() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final UberPage publication = new UberPage(3);
    final StorageEngineReader first = source(session, publication, 2);
    final StorageEngineReader second = source(session, publication, 2);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    when(session.openDerivedIndexReader(first)).thenReturn(null, index);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(first));
      assertSame(index, readers.borrow(first));
      first.close();
      when(first.isClosed()).thenReturn(true);
      assertSame(index, readers.borrow(second));
      verify(session, never()).openDerivedIndexReader(second);

      // A truncate and replacement can publish the same revision and timestamp. Object identity
      // must still force a fresh persisted nonce check; revision equality would serve stale data.
      final StorageEngineReader replaced = source(session, new UberPage(3), 2);
      final StorageEngineReader replacement = mock(StorageEngineReader.class);
      when(session.openDerivedIndexReader(replaced)).thenReturn(replacement);
      assertSame(replacement, readers.borrow(replaced));
      verify(index).close();
    }
  }

  @Test
  void boundsRetentionAndClosesEverySnapshot() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final UberPage publication = new UberPage(66);
    final StorageEngineReader[] indexes = new StorageEngineReader[65];
    final var readers = new DerivedAggregateReaders();
    for (int revision = 1; revision <= indexes.length; revision++) {
      final StorageEngineReader source = source(session, publication, revision);
      indexes[revision - 1] = mock(StorageEngineReader.class);
      when(session.openDerivedIndexReader(source)).thenReturn(indexes[revision - 1]);
      assertSame(indexes[revision - 1], readers.borrow(source));
    }
    verify(indexes[0]).close();
    verify(indexes[1], never()).close();
    readers.close();
    readers.close();
    for (final StorageEngineReader index : indexes) {
      verify(index).close();
    }
    assertThrows(IllegalStateException.class, () -> readers.borrow(source(session, publication, 1)));
  }

  @Test
  void writersDeclineAndForeignThreadsUseSourceOwnedReaders() throws InterruptedException {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final StorageEngineReader source = source(session, new UberPage(2), 1);
    final StorageEngineReader local = mock(StorageEngineReader.class);
    final StorageEngineReader foreign = mock(StorageEngineReader.class);
    when(session.openDerivedIndexReader(source)).thenReturn(local);
    when(source.getDerivedIndexReader()).thenReturn(foreign);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(mock(StorageEngineWriter.class)));
      final StorageEngineReader mutableView = mock(StorageEngineReader.class);
      when(mutableView.hasTrxIntentLog()).thenReturn(true);
      assertNull(readers.borrow(mutableView));
      assertSame(local, readers.borrow(source));
      final AtomicReference<StorageEngineReader> observed = new AtomicReference<>();
      final Thread worker = Thread.ofPlatform().start(() -> observed.set(readers.borrow(source)));
      worker.join();
      assertSame(foreign, observed.get());
      verify(foreign, never()).close();
    }
    verify(local).close();
    verify(foreign, never()).close();
  }

  private static StorageEngineReader source(final JsonResourceSession session, final UberPage publication,
      final int revision) {
    final StorageEngineReader source = mock(StorageEngineReader.class);
    doReturn(session).when(source).getResourceSession();
    when(source.getUberPage()).thenReturn(publication);
    when(source.getRevisionNumber()).thenReturn(revision);
    return source;
  }
}
