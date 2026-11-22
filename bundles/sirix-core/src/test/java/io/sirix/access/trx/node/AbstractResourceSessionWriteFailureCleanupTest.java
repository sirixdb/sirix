/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.sirix.access.ResourceStore;
import io.sirix.access.DatabaseNotifications;
import io.sirix.access.trx.node.InternalResourceSession.Abort;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.cache.BufferManager;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.Node;
import io.sirix.index.interval.ValidTimeReadCache;
import java.util.Map;
import io.sirix.utils.ObjectPool;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import io.sirix.page.UberPage;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Isolated
@Order(Integer.MAX_VALUE)
final class AbstractResourceSessionWriteFailureCleanupTest {


  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  @Timeout(30)
  void retryCompletesRealNodeOwnershipAfterWriterRefusal(final int refusals) throws ReflectiveOperationException {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final User firstUser = new User("first", UUID.randomUUID());
    final User secondUser = new User("second", UUID.randomUUID());
    try (Database<JsonResourceSession> firstDatabase = Databases.openJsonDatabase(databasePath, firstUser);
        Database<JsonResourceSession> secondDatabase = Databases.openJsonDatabase(databasePath, secondUser)) {
      firstDatabase.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession first = firstDatabase.beginResourceSession("resource");
          JsonResourceSession second = secondDatabase.beginResourceSession("resource")) {
        final JsonNodeTrx transaction = first.beginNodeTrx();
        final StorageEngineWriter original = transaction.getStorageEngineWriter();
        final StorageEngineWriter writer = mock(StorageEngineWriter.class, delegatesTo(original));
        final Field writerField = AbstractNodeTrxImpl.class.getDeclaredField("storageEngineWriter");
        writerField.setAccessible(true);
        writerField.set(transaction, writer);
        final Field readerField = AbstractNodeTrxImpl.class.getDeclaredField("nodeReadOnlyTrx");
        readerField.setAccessible(true);
        final InternalNodeReadOnlyTrx<?> reader = (InternalNodeReadOnlyTrx<?>) readerField.get(transaction);
        reader.setPageReadTransaction(writer);
        final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) first;
        session.storageEngineWriterMap.put(transaction.getId(), writer);
        final RuntimeException refusal = new IllegalStateException("worker still owns writer");
        final AtomicInteger attempts = new AtomicInteger();
        doThrow(refusal).doAnswer(invocation -> original.rollback()).when(writer).rollback();
        doAnswer(invocation -> {
          if (attempts.getAndIncrement() < refusals) {
            throw refusal;
          }
          original.close();
          return null;
        }).when(writer).close();
        final Field schedulerField = AbstractNodeTrxImpl.class.getDeclaredField("commitScheduler");
        schedulerField.setAccessible(true);
        final ScheduledExecutorService scheduler = (ScheduledExecutorService) schedulerField.get(transaction);

        assertSame(refusal, assertThrows(RuntimeException.class, first::close));
        assertFalse(first.isClosed());
        assertFalse(transaction.isClosed());
        assertTrue(second.hasRunningNodeWriteTrx());
        assertFalse(scheduler.isShutdown());
        assertFalse(writer.isClosed());
        if (refusals == 2) {
          assertSame(refusal, assertThrows(RuntimeException.class, first::close));
        } else {
          first.close();
        }
        assertTrue(first.isClosed());
        assertTrue(transaction.isClosed());
        assertTrue(scheduler.isTerminated());
        assertFalse(second.hasRunningNodeWriteTrx());
        try (JsonNodeTrx next = second.beginNodeTrx()) {
          assertFalse(next.isClosed());
        }
        first.close();
        assertFalse(second.hasRunningNodeWriteTrx());
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void closesSharedTransactionsAndClearsTheirRegistryAfterCacheFailure() throws ReflectiveOperationException {
    final FailingResourceSession session = new FailingResourceSession(null, mock(JsonNodeTrx.class));
    final ValidTimeReadCache cache = mock(ValidTimeReadCache.class);
    final RuntimeException first = new IllegalStateException("valid-time cache close");
    final RuntimeException second = new IllegalStateException("shared transaction close");
    final Field cacheField = AbstractResourceSession.class.getDeclaredField("validTimeReadCache");
    cacheField.setAccessible(true);
    cacheField.set(session, cache);
    final Field sharedField = AbstractResourceSession.class.getDeclaredField("sharedTrxMap");
    sharedField.setAccessible(true);
    final Map<Object, JsonNodeReadOnlyTrx> shared = (Map<Object, JsonNodeReadOnlyTrx>) sharedField.get(session);
    final JsonNodeReadOnlyTrx transaction = mock(JsonNodeReadOnlyTrx.class);
    shared.put(new Object(), transaction);
    doThrow(first).when(cache).clear();
    doAnswer(invocation -> {
      when(transaction.isClosed()).thenReturn(true);
      throw second;
    }).when(transaction).close();
    assertSame(first, assertThrows(RuntimeException.class, session::close));
    assertSame(second, first.getSuppressed()[0]);
    verify(transaction).close();
    verify(session.storage).close();
    assertTrue(shared.isEmpty());
    assertTrue(session.isClosed());
  }

  @Test
  void closesOwnersRegisteredDuringRollbackEvenWhenNodeCloseFails() {
    final FailingResourceSession session = new FailingResourceSession(null, mock(JsonNodeTrx.class));
    final JsonNodeTrx transaction = mock(JsonNodeTrx.class);
    final StorageEngineWriter original = mock(StorageEngineWriter.class);
    final StorageEngineWriter replacement = mock(StorageEngineWriter.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final RuntimeException failure = new IllegalStateException("node close after rollback");
    session.nodeTrxMap.put(1, transaction);
    session.storageEngineWriterMap.put(1, original);
    doAnswer(invocation -> {
      session.storageEngineWriterMap.put(1, replacement);
      session.storageEngineReaderMap.put(2, reader);
      return transaction;
    }).when(transaction).rollback();
    doThrow(failure).when(transaction).close();
    when(transaction.isClosed()).thenReturn(true);

    assertSame(failure, assertThrows(RuntimeException.class, session::close));
    verify(original).close();
    verify(replacement).close();
    verify(reader).close();
    verify(session.storage).close();
    assertTrue(session.isClosed());
  }

  @Test
  void writerRefusingCloseRetainsDependentStorageUntilRetry() {
    final RuntimeException refusal = new IllegalStateException("worker still owns storage");
    final FailingResourceSession session = new FailingResourceSession(null, mock(JsonNodeTrx.class));
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final StorageEngineReader independentReader = mock(StorageEngineReader.class);
    when(writer.getTrxId()).thenReturn(1);
    session.storageEngineWriterMap.put(1, writer);
    session.storageEngineReaderMap.put(2, independentReader);
    doThrow(refusal).when(writer).close();

    assertSame(refusal, assertThrows(RuntimeException.class, session::close));
    verify(independentReader).close();
    verify(session.storage, never()).close();
    verify(session.resourceStore, never()).closeResourceSession(session.resourceConfig.getResource());
    assertFalse(session.isClosed());
    assertSame(writer, session.storageEngineReaderMap.get(1));
    assertTrue(session.storageEngineWriterMap.isEmpty());

    doNothing().when(writer).close();
    session.close();
    verify(writer, times(2)).close();
    verify(session.storage).close();
    verify(session.resourceStore).closeResourceSession(session.resourceConfig.getResource());
    assertTrue(session.isClosed());
  }

  @Test
  @SuppressWarnings("unchecked")
  void closeAttemptsEveryOwnerAndPreservesRollbackFailure() throws ReflectiveOperationException {
    final RuntimeException first = new IllegalStateException("rollback");
    final RuntimeException nodeFailure = new IllegalStateException("node close");
    final RuntimeException readerFailure = new IllegalStateException("reader close");
    final RuntimeException poolFailure = new IllegalStateException("pool close");
    final RuntimeException storageFailure = new IllegalStateException("storage close");
    final RuntimeException storeFailure = new IllegalStateException("store close");
    final JsonNodeTrx transaction = mock(JsonNodeTrx.class);
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final FailingResourceSession session = new FailingResourceSession(null, transaction);
    session.nodeTrxMap.put(1, transaction);
    session.storageEngineWriterMap.put(1, writer);
    session.storageEngineReaderMap.put(2, reader);
    doThrow(first).when(transaction).rollback();
    doAnswer(invocation -> {
      // A failing transaction may unregister its writer before throwing. The session still owns it.
      session.storageEngineWriterMap.remove(1);
      throw nodeFailure;
    }).when(transaction).close();
    when(transaction.isClosed()).thenReturn(true);
    doThrow(readerFailure).when(reader).close();
    final ObjectPool<StorageEngineReader> pool = mock(ObjectPool.class);
    final Field poolField = AbstractResourceSession.class.getDeclaredField("pool");
    poolField.setAccessible(true);
    ((AtomicReference<ObjectPool<StorageEngineReader>>) poolField.get(session)).set(pool);
    doThrow(poolFailure).when(pool).close();
    doThrow(storageFailure).when(session.storage).close();
    doThrow(storeFailure).when(session.resourceStore).closeResourceSession(session.resourceConfig.getResource());

    final RuntimeException observed = assertThrows(RuntimeException.class, session::close);
    assertSame(first, observed);
    assertEquals(5, observed.getSuppressed().length);
    assertSame(nodeFailure, observed.getSuppressed()[0]);
    assertSame(readerFailure, observed.getSuppressed()[1]);
    assertSame(poolFailure, observed.getSuppressed()[2]);
    assertSame(storageFailure, observed.getSuppressed()[3]);
    assertSame(storeFailure, observed.getSuppressed()[4]);
    verify(transaction).close();
    verify(writer).close();
    verify(reader).close();
    verify(pool).close();
    verify(session.storage).close();
    verify(session.resourceStore).closeResourceSession(session.resourceConfig.getResource());
    assertTrue(session.isClosed());
    assertTrue(session.nodeTrxMap.isEmpty());
    assertTrue(session.storageEngineReaderMap.isEmpty());
    assertTrue(session.storageEngineWriterMap.isEmpty());
    session.close();
    verify(transaction, times(1)).close();
    verify(session.storage, times(1)).close();
  }

  @Test
  void listenerBindingFailureClosesUnregisteredWriterAndLeavesPermitReusable() {
    final RuntimeException bindingFailure = new IllegalStateException("projection listener binding failed");
    final RuntimeException closeFailure = new IllegalStateException("writer close failed");
    final StorageEngineWriter failedWriter = writerWithDocumentNode();
    doThrow(closeFailure).when(failedWriter).close();
    final StorageEngineWriter succeedingWriter = writerWithDocumentNode();
    final JsonNodeTrx succeedingTrx = mock(JsonNodeTrx.class);

    final FailingResourceSession session =
        new FailingResourceSession(bindingFailure, succeedingTrx, failedWriter, succeedingWriter);
    try {
      final RuntimeException thrown = assertThrows(RuntimeException.class, session::beginNodeTrx);

      assertSame(bindingFailure, thrown, "cleanup must not replace the listener-binding failure");
      assertEquals(1, thrown.getSuppressed().length);
      assertSame(closeFailure, thrown.getSuppressed()[0]);
      verify(failedWriter).close();
      assertEquals(1, session.writeLock.availablePermits(), "failed construction must release exactly one permit");

      final JsonNodeTrx opened = session.beginNodeTrx();
      assertSame(succeedingTrx, opened, "a subsequent writer must be available immediately");
      assertEquals(0, session.writeLock.availablePermits());
      verify(succeedingWriter, never()).close();

      final int transactionId = opened.getId();
      session.closeWriteTransaction(transactionId);
      succeedingWriter.close();
      verify(succeedingWriter).close();
      assertEquals(1, session.writeLock.availablePermits());
    } finally {
      session.close();
    }
  }

  private static StorageEngineWriter writerWithDocumentNode() {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final Node document = mock(Node.class);
    when(writer.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.DOCUMENT, -1)).thenReturn(document);
    return writer;
  }

  /** Deterministic seam: the first node-transaction construction models listener binding failure. */
  private static final class FailingResourceSession extends AbstractResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> {

    private final ArrayDeque<StorageEngineWriter> writers = new ArrayDeque<>();
    private final RuntimeException firstConstructionFailure;
    private final JsonNodeTrx succeedingTrx;
    private int constructionAttempts;

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FailingResourceSession(final RuntimeException firstConstructionFailure, final JsonNodeTrx succeedingTrx,
        final StorageEngineWriter... writers) {
      super(testResourceStore(), ResourceConfiguration.newBuilder("resource").build(), mock(BufferManager.class),
          storageWithEmptyRevisionIndex(), new UberPage(1), new Semaphore(1), null,
          mock(StorageEngineWriterFactory.class));
      this.firstConstructionFailure = firstConstructionFailure;
      this.succeedingTrx = succeedingTrx;
      for (final StorageEngineWriter writer : writers) {
        this.writers.addLast(writer);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ResourceStore testResourceStore() {
      final ResourceStore store = mock(ResourceStore.class);
      when(store.getNotifications()).thenReturn(new DatabaseNotifications());
      return store;
    }

    private static IOStorage storageWithEmptyRevisionIndex() {
      final IOStorage storage = mock(IOStorage.class);
      when(storage.getRevisionIndexHolder()).thenReturn(new RevisionIndexHolder());
      return storage;
    }

    @Override
    public StorageEngineWriter createPageTransaction(final int id, final int representRevision,
        final int storedRevision, final Abort abort, final boolean isBoundToNodeTrx) {
      return writers.removeFirst();
    }

    @Override
    public JsonNodeReadOnlyTrx createNodeReadOnlyTrx(final int nodeTrxId, final StorageEngineReader storageEngineReader,
        final Node documentNode) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public JsonNodeTrx createNodeReadWriteTrx(final int nodeTrxId, final StorageEngineWriter storageEngineWriter,
        final int maxNodeCount, final Duration autoCommitDelay, final Node documentNode,
        final AfterCommitState afterCommitState) {
      if (constructionAttempts++ == 0) {
        throw firstConstructionFailure;
      }
      when(succeedingTrx.getId()).thenReturn(nodeTrxId);
      return succeedingTrx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx>> C getRtxIndexController(final int revision) {
      return (C) mock(IndexController.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx>> C getWtxIndexController(final int revision) {
      return (C) mock(IndexController.class);
    }

    @Override
    protected void invalidateIndexControllers(final int firstRevision) {}

  }
}
